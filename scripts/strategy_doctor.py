"""Strategy Doctor — autonomous analyst that watches the bot's actual trade
history and surfaces concrete, one-click-implementable config changes.

The Doctor:
- Runs every `interval_minutes` minutes in the background (also forceable via
  POST /api/doctor/run-now).
- Produces *suggestions* not changes. The user reviews each suggestion and
  hits Apply (auto-merges `actions` into bot_config) or Dismiss (hidden for
  the cooldown window).
- Only suggests changes that map to EXISTING config keys with code support —
  no "wishful thinking" suggestions.
- Tells the user when sample size is too low rather than make low-confidence
  calls. The "needs_more_data" suggestion category is informational only.

Persistence model:
- `strategy_suggestions` collection, schema:
  {
    id: str (uuid),
    category: str (sizing | sl | tp | partial | hold | gate | scanner |
                   classifier | timing | needs_more_data),
    title: str (short headline),
    rationale: str (multi-line explainer + stats),
    actions: dict[str, Any] (bot_config keys → new values; empty for info)
    confidence: str (high | med | low),
    metrics: dict (raw numbers backing the suggestion — for the UI tooltip),
    status: str (pending | applied | dismissed),
    created_at: ISO timestamp,
    applied_at: ISO timestamp | None,
    dismissed_at: ISO timestamp | None,
    expires_at: ISO timestamp (auto-dismissed after this if untouched),
  }
"""
from __future__ import annotations

import asyncio
import hashlib
import logging
import math
import os
import statistics
import uuid
from collections import Counter, defaultdict
from datetime import datetime, timedelta, timezone
from typing import Any

logger = logging.getLogger("strategy_doctor")

# ---------- Tunables ----------
# How long an analysis window covers
ANALYSIS_LOOKBACK_HOURS = 24
# Minimum closed trades before any sizing/exit suggestion fires
MIN_TRADES_FOR_SUGGESTION = 30
# How long a dismissed suggestion stays hidden before being re-evaluated
DISMISS_COOLDOWN_HOURS = 24
# Suggestions auto-expire after this if neither applied nor dismissed
SUGGESTION_TTL_HOURS = 72
# Default loop interval
DEFAULT_INTERVAL_MINUTES = 30


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _hash_signature(category: str, action_keys: list[str], action_values: dict | None = None) -> str:
    """Stable signature for a (category, action-set) combo. When action_values
    is supplied, includes the proposed VALUES so the doctor doesn't re-suggest
    a fix that's already in force (e.g. "set TP=8" when TP is already 8 — the
    rule keeps detecting a frequent-TP pattern from old trades and re-firing).
    Backward-compatible: older code paths that pass only keys get a value-less
    signature."""
    raw = f"{category}|{','.join(sorted(action_keys))}"
    if action_values:
        vals = ",".join(f"{k}={action_values[k]}" for k in sorted(action_values))
        raw = f"{raw}|{vals}"
    return hashlib.sha1(raw.encode()).hexdigest()[:12]


class StrategyDoctor:
    def __init__(self, db, hub=None):
        self.db = db
        self.hub = hub  # broadcast new suggestions over WS if available
        self._task: asyncio.Task | None = None
        self.interval_minutes = DEFAULT_INTERVAL_MINUTES

    # ---------- public lifecycle ----------
    async def start(self, interval_minutes: int = DEFAULT_INTERVAL_MINUTES):
        self.interval_minutes = max(5, int(interval_minutes))
        if self._task and not self._task.done():
            return
        self._task = asyncio.create_task(self._loop(), name="strategy_doctor")

    async def stop(self):
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass

    async def _loop(self):
        # Initial delay so we don't analyse on bare-fresh DB at startup
        await asyncio.sleep(60)
        while True:
            try:
                await self.run_once()
            except asyncio.CancelledError:
                raise
            except Exception as e:
                logger.exception(f"doctor cycle failed: {e}")
            await asyncio.sleep(self.interval_minutes * 60)

    # ---------- analysis ----------
    async def run_once(self) -> list[dict]:
        """Run all rules, persist new suggestions, expire stale ones.
        Returns the suggestions that were freshly inserted this cycle."""
        # Expire untouched suggestions older than TTL
        await self._expire_stale()

        cfg_doc = await self.db.bot_config.find_one({}, {"_id": 0}) or {}
        trades = await self._fetch_recent_trades()

        existing_pending = await self._existing_pending_signatures()
        existing_dismissed = await self._existing_recently_dismissed_signatures()
        # NEW: also dedup against recently-applied suggestions whose actions
        # are still in force in bot_config. Without this, a rule that detects
        # "frequent TP hits" keeps re-suggesting `take_profit_pct=8` every
        # cycle for the next 24h even after you applied it, because the OLD
        # trades that triggered the rule are still in the lookback window.
        existing_applied = await self._existing_applied_active_signatures(cfg_doc)
        skip = existing_pending | existing_dismissed | existing_applied

        suggestions: list[dict] = []

        # No-data case: surface a single "need more data" card
        if len(trades) < MIN_TRADES_FOR_SUGGESTION:
            suggestions.append({
                "category": "needs_more_data",
                "title": f"Need more trade data ({len(trades)}/{MIN_TRADES_FOR_SUGGESTION})",
                "rationale": (
                    f"I've seen only {len(trades)} closed trades in the last "
                    f"{ANALYSIS_LOOKBACK_HOURS}h. Need at least {MIN_TRADES_FOR_SUGGESTION} "
                    "to generate statistically meaningful suggestions. The bot is still "
                    "operating normally — this just means I'm still learning your setup."
                ),
                "actions": {},
                "confidence": "low",
                "metrics": {"n_trades": len(trades)},
            })
        else:
            # Run each rule. Each returns 0+ suggestion dicts.
            for rule in (
                self._rule_sizing_advantage,
                self._rule_stop_loss_tightness,
                self._rule_take_profit_frequency,
                self._rule_partial_tp_threshold,
                self._rule_hold_time,
                self._rule_distribution_vacuum_gate,
                self._rule_classifier_bucket_focus,
                self._rule_time_of_day,
                self._rule_protocol_focus,
                self._rule_pattern_tp_calibration,
                self._rule_greylist_sniper_tuning,
            ):
                try:
                    rule_out = rule(trades, cfg_doc)
                    if rule_out:
                        suggestions.extend(rule_out)
                except Exception as e:
                    logger.exception(f"rule {rule.__name__} failed: {e}")

        # Deduplicate against pending + dismissed signatures
        fresh = []
        for s in suggestions:
            # Value-aware signature so "set X=N" doesn't dedup against
            # "set X=M" — different proposals deserve separate evaluation.
            sig = _hash_signature(
                s["category"],
                list((s.get("actions") or {}).keys()),
                s.get("actions") or {},
            )
            s["signature"] = sig
            if sig in skip:
                continue
            s["id"] = str(uuid.uuid4())
            s["status"] = "pending"
            s["created_at"] = _now_iso()
            s["applied_at"] = None
            s["dismissed_at"] = None
            s["expires_at"] = (datetime.now(timezone.utc) + timedelta(hours=SUGGESTION_TTL_HOURS)).isoformat()
            fresh.append(s)

        if fresh:
            await self.db.strategy_suggestions.insert_many([dict(s) for s in fresh])
            logger.info(f"doctor inserted {len(fresh)} new suggestion(s)")
            if self.hub:
                try:
                    # Strip _id before broadcasting (insert_many mutates dicts)
                    for s in fresh:
                        s.pop("_id", None)
                    await self.hub.broadcast("doctor_new_suggestions", {"count": len(fresh)})
                except Exception:
                    pass
        return fresh

    # ---------- data fetch ----------
    async def _fetch_recent_trades(self) -> list[dict]:
        since = (datetime.now(timezone.utc) - timedelta(hours=ANALYSIS_LOOKBACK_HOURS)).isoformat()
        # Include both modes — paper data is honest for strategy analysis;
        # live data is what we ultimately care about. Strategy logic is
        # mode-independent (only execution differs).
        cur = self.db.trades.find({
            "status": "closed",
            "exit_time": {"$gte": since},
            "ghost_entry": {"$ne": True},
        }, {"_id": 0})
        return await cur.to_list(5000)

    async def _existing_pending_signatures(self) -> set[str]:
        cur = self.db.strategy_suggestions.find(
            {"status": "pending"}, {"signature": 1, "_id": 0},
        )
        return {d["signature"] async for d in cur if d.get("signature")}

    async def _existing_recently_dismissed_signatures(self) -> set[str]:
        cutoff = (datetime.now(timezone.utc) - timedelta(hours=DISMISS_COOLDOWN_HOURS)).isoformat()
        cur = self.db.strategy_suggestions.find({
            "status": "dismissed",
            "dismissed_at": {"$gte": cutoff},
        }, {"signature": 1, "_id": 0})
        return {d["signature"] async for d in cur if d.get("signature")}

    async def _existing_applied_active_signatures(self, cfg_doc: dict) -> set[str]:
        """Dedup against APPLIED suggestions whose action values still match
        the current bot_config. Without this, after a rule like "set
        take_profit_pct=8" is applied, the SAME rule keeps re-suggesting it
        next cycle until the lookback rolls past — annoying user noise.

        We consider an applied suggestion "still active" when EVERY key in
        its `actions` dict still equals the current value in bot_config.
        If the user later changed any of those keys manually, the suggestion
        is no longer in effect and we should let the rule re-fire."""
        cutoff = (datetime.now(timezone.utc) - timedelta(hours=DISMISS_COOLDOWN_HOURS)).isoformat()
        cur = self.db.strategy_suggestions.find({
            "status": "applied",
            "applied_at": {"$gte": cutoff},
        }, {"signature": 1, "actions": 1, "_id": 0})
        active: set[str] = set()
        async for d in cur:
            sig = d.get("signature")
            actions = d.get("actions") or {}
            if not sig or not actions:
                continue
            # Are all action values STILL in force in bot_config?
            still_in_force = all(
                cfg_doc.get(k) == v for k, v in actions.items()
            )
            if still_in_force:
                active.add(sig)
        return active

    async def _expire_stale(self):
        now = _now_iso()
        await self.db.strategy_suggestions.update_many(
            {"status": "pending", "expires_at": {"$lt": now}},
            {"$set": {"status": "expired"}},
        )

    # ============== RULES ==============
    # Each rule receives (trades, cfg_doc) and returns 0+ suggestions.
    # Each suggestion MUST include: category, title, rationale, actions,
    # confidence, metrics. `actions` is a dict of bot_config keys → new values.

    def _rule_sizing_advantage(self, trades, cfg):
        """Detect a clear size-band advantage in WR/PnL."""
        small = [t for t in trades if (t.get("entry_usd") or 0) < 0.7]
        big = [t for t in trades if (t.get("entry_usd") or 0) > 1.5]
        if len(small) < 15 or len(big) < 15:
            return []
        wr_s = sum(1 for t in small if t.get("pnl_pct", 0) > 0) / len(small) * 100
        wr_b = sum(1 for t in big if t.get("pnl_pct", 0) > 0) / len(big) * 100
        avg_s = statistics.mean(t.get("pnl_pct", 0) for t in small)
        avg_b = statistics.mean(t.get("pnl_pct", 0) for t in big)
        # Small beats big by 15+ pp WR AND has better avg PnL
        if (wr_s - wr_b) >= 15 and avg_s > avg_b:
            cur_max = cfg.get("max_trade_usd", 1.0)
            if cur_max > 1.0:
                return [{
                    "category": "sizing",
                    "title": "Smaller trades outperforming — lower max_trade_usd to $0.90",
                    "rationale": (
                        f"Trades <$0.70 hit {wr_s:.0f}% WR (avg {avg_s:+.1f}%) "
                        f"vs trades >$1.50 at {wr_b:.0f}% WR (avg {avg_b:+.1f}%). "
                        "Lower max_trade_usd so the bot stays in the high-WR band."
                    ),
                    "actions": {"max_trade_usd": 0.9},
                    "confidence": "high" if min(len(small), len(big)) >= 30 else "med",
                    "metrics": {
                        "small_n": len(small), "small_wr": round(wr_s, 1), "small_avg": round(avg_s, 2),
                        "big_n": len(big), "big_wr": round(wr_b, 1), "big_avg": round(avg_b, 2),
                    },
                }]
        return []

    def _rule_stop_loss_tightness(self, trades, cfg):
        """If SL fires at materially worse than the configured threshold,
        the severity override needs re-tuning OR the SL itself."""
        sl_trades = [t for t in trades if "stop-loss" in (t.get("exit_reason") or "")]
        if len(sl_trades) < 10:
            return []
        sl_pnls = [t.get("pnl_pct", 0) for t in sl_trades]
        median_sl = statistics.median(sl_pnls)
        configured_sl = float(cfg.get("stop_loss_pct", 10))
        # SL avg should be -SL ± slip. If 2x worse, something's wrong.
        if median_sl < -(configured_sl * 2.0):
            return [{
                "category": "sl",
                "title": f"SL exits hitting {median_sl:.0f}% median (vs configured -{configured_sl:.0f}%)",
                "rationale": (
                    f"{len(sl_trades)} stop-loss exits over {ANALYSIS_LOOKBACK_HOURS}h had "
                    f"median PnL {median_sl:+.1f}% — 2x worse than the configured SL of "
                    f"-{configured_sl:.0f}%. Likely cause: pools are crashing faster than the "
                    "persistence gate confirms. Recommended: enable v2.1 severity override "
                    "(already in code — confirm intelligent_exit_v2=True) and tighten the "
                    "panic auto-slip cap so fills land closer to the trigger."
                ),
                "actions": {"intelligent_exit_v2": True, "auto_exit_slip_cap_bps": 900},
                "confidence": "high",
                "metrics": {
                    "n_sl": len(sl_trades),
                    "median_sl_pnl": round(median_sl, 1),
                    "configured_sl_pct": configured_sl,
                },
            }]
        return []

    def _rule_take_profit_frequency(self, trades, cfg):
        """If TP rarely fires (< 10% of closed trades), it's too high."""
        n = len(trades)
        tp_hits = sum(1 for t in trades if "take-profit" in (t.get("exit_reason") or "").lower())
        if n < 50:
            return []
        tp_rate = tp_hits / n * 100
        cur_tp = float(cfg.get("take_profit_pct", 12))
        if tp_rate < 8 and cur_tp > 8:
            new_tp = max(6, cur_tp - 4)
            return [{
                "category": "tp",
                "title": f"Take-profit rarely fires ({tp_rate:.0f}%) — lower to {new_tp:.0f}%",
                "rationale": (
                    f"Only {tp_hits}/{n} closed trades hit take-profit (current {cur_tp:.0f}%). "
                    "Most exits are happening via SL/timeout. Lower TP so more trades lock "
                    "wins before they fade."
                ),
                "actions": {"take_profit_pct": new_tp},
                "confidence": "med",
                "metrics": {"n_trades": n, "tp_hits": tp_hits, "tp_rate_pct": round(tp_rate, 1),
                            "current_tp": cur_tp, "proposed_tp": new_tp},
            }]
        return []

    def _rule_partial_tp_threshold(self, trades, cfg):
        """If partial-TP-fired trades have substantially higher WR than
        no-partial trades, lower the partial trigger so it fires more often."""
        with_p = [t for t in trades if t.get("partial_done") or t.get("partial_sig")]
        no_p = [t for t in trades if not (t.get("partial_done") or t.get("partial_sig"))]
        if len(with_p) < 10 or len(no_p) < 30:
            return []
        wr_p = sum(1 for t in with_p if t.get("pnl_pct", 0) > 0) / len(with_p) * 100
        wr_n = sum(1 for t in no_p if t.get("pnl_pct", 0) > 0) / len(no_p) * 100
        cur_partial = float(cfg.get("partial_tp_pct", 100))
        # Partial sell-fraction = 100 means no moon bag. We only suggest a
        # change when partial-firing actually correlates with wins AND user
        # currently has the moon-bag enabled.
        if (wr_p - wr_n) >= 15 and 0 < cur_partial < 100:
            return [{
                "category": "partial",
                "title": f"Partial-TP correlates with wins (+{wr_p - wr_n:.0f}pp WR)",
                "rationale": (
                    f"Trades where partial-TP fired: {len(with_p)} (WR {wr_p:.0f}%). "
                    f"Trades without: {len(no_p)} (WR {wr_n:.0f}%). The partial mechanism "
                    "is locking wins early. Consider lowering the TP gain threshold so it "
                    "fires more often, OR keeping partial_tp_pct=100 (full exit at TP) to "
                    "avoid moon-bag bleed."
                ),
                "actions": {"take_profit_pct": 8},
                "confidence": "high" if len(with_p) >= 20 else "med",
                "metrics": {"with_partial_n": len(with_p), "with_partial_wr": round(wr_p, 1),
                            "no_partial_n": len(no_p), "no_partial_wr": round(wr_n, 1)},
            }]
        return []

    def _rule_hold_time(self, trades, cfg):
        """If long holds (>30s) are markedly worse than short ones, tighten."""
        short, long_ = [], []
        for t in trades:
            try:
                e = datetime.fromisoformat(str(t["entry_time"]).replace("Z", "+00:00"))
                x = datetime.fromisoformat(str(t["exit_time"]).replace("Z", "+00:00"))
                h = (x - e).total_seconds()
            except Exception:
                continue
            if h <= 15:
                short.append(t.get("pnl_pct", 0))
            elif h >= 30:
                long_.append(t.get("pnl_pct", 0))
        if len(short) < 15 or len(long_) < 8:
            return []
        wr_s = sum(1 for p in short if p > 0) / len(short) * 100
        wr_l = sum(1 for p in long_ if p > 0) / len(long_) * 100
        cur_hold = int(cfg.get("hold_max_seconds", 30))
        if (wr_s - wr_l) >= 20 and cur_hold > 15:
            return [{
                "category": "hold",
                "title": "Long holds underperforming — drop hold_max_seconds to 15s",
                "rationale": (
                    f"≤15s holds: WR {wr_s:.0f}% ({len(short)} trades). "
                    f"≥30s holds: WR {wr_l:.0f}% ({len(long_)} trades). "
                    "Long holds = the entry signal already failed; cutting losses faster lets "
                    "the bot redeploy that capital sooner on fresh momentum."
                ),
                "actions": {"hold_max_seconds": 15},
                "confidence": "high" if len(long_) >= 15 else "med",
                "metrics": {"short_n": len(short), "short_wr": round(wr_s, 1),
                            "long_n": len(long_), "long_wr": round(wr_l, 1)},
            }]
        return []

    def _rule_distribution_vacuum_gate(self, trades, cfg):
        """If WR is poor AND distribution-vacuum is OFF, suggest turning it
        on (or vice versa). Only fires when the gate flag and overall WR
        clearly disagree."""
        if len(trades) < 50:
            return []
        wr = sum(1 for t in trades if t.get("pnl_pct", 0) > 0) / len(trades) * 100
        vacuum_on = bool(cfg.get("gate_distribution_vacuum", False))
        # WR < 25 AND gate currently OFF → suggest ON to filter spray-and-prays
        if wr < 25 and not vacuum_on:
            return [{
                "category": "gate",
                "title": "Re-enable distribution-vacuum gate to filter weak launches",
                "rationale": (
                    f"Overall WR {wr:.0f}% over {len(trades)} trades. The "
                    "distribution-vacuum filter (rejects tokens where all holders appear "
                    "in the recent window — typical of bots front-running their own buy) "
                    "is currently OFF. Turning it ON typically lifts WR by 5-10pp at the "
                    "cost of ~30% fewer entries."
                ),
                "actions": {"gate_distribution_vacuum": True},
                "confidence": "med",
                "metrics": {"n_trades": len(trades), "wr_pct": round(wr, 1)},
            }]
        return []

    def _rule_classifier_bucket_focus(self, trades, cfg):
        """If one classifier_action bucket has clearly the best PnL, suggest
        a min_risk_score or similar gate to focus there."""
        by_action = defaultdict(list)
        for t in trades:
            by_action[t.get("classifier_action") or "?"].append(t.get("pnl_pct", 0))
        # Need at least 2 buckets with 10+ trades each
        big_buckets = {k: v for k, v in by_action.items() if len(v) >= 10}
        if len(big_buckets) < 2:
            return []
        # Find best and worst
        scored = []
        for k, v in big_buckets.items():
            wr = sum(1 for p in v if p > 0) / len(v) * 100
            avg = statistics.mean(v)
            scored.append((k, len(v), wr, avg))
        scored.sort(key=lambda x: -x[2])  # by WR desc
        best, worst = scored[0], scored[-1]
        # Best beats worst by 20+ pp AND best is a recognized action
        if (best[2] - worst[2]) >= 20 and best[1] >= 10:
            # Collect actions whose WR is within 10pp of best — multiple
            # buckets often share the "winners" cohort.
            keep = sorted(
                [(k, n, wr, avg) for (k, n, wr, avg) in scored if (best[2] - wr) <= 10 and n >= 8],
                key=lambda x: -x[2],
            )
            whitelist = [k for k, *_ in keep] if keep else [best[0]]
            return [{
                "category": "classifier",
                "title": f"Restrict to '{', '.join(whitelist)}' — outperforming by {best[2] - worst[2]:.0f}pp",
                "rationale": (
                    f"'{best[0]}' actions: WR {best[2]:.0f}% (avg {best[3]:+.1f}%, n={best[1]}).\n"
                    f"'{worst[0]}' actions: WR {worst[2]:.0f}% (avg {worst[3]:+.1f}%, n={worst[1]}).\n\n"
                    f"Applying will set `classifier_action_whitelist` to {whitelist} — "
                    "bot will skip entries for any classifier action NOT in this list. "
                    "Dismiss if you'd rather explore than exploit."
                ),
                "actions": {"classifier_action_whitelist": whitelist},
                "confidence": "high",
                "metrics": {
                    "best_action": best[0], "best_wr": round(best[2], 1), "best_n": best[1],
                    "worst_action": worst[0], "worst_wr": round(worst[2], 1), "worst_n": worst[1],
                    "whitelist": whitelist,
                },
            }]
        return []

    def _rule_time_of_day(self, trades, cfg):
        """If WR varies significantly by entry hour, surface it (informational
        — we don't yet have a time-of-day gate in config)."""
        by_hour = defaultdict(list)
        for t in trades:
            try:
                h = datetime.fromisoformat(str(t["entry_time"]).replace("Z", "+00:00")).hour
            except Exception:
                continue
            by_hour[h].append(t.get("pnl_pct", 0))
        if len(by_hour) < 6:
            return []
        scored = []
        for h, v in by_hour.items():
            if len(v) < 6:
                continue
            wr = sum(1 for p in v if p > 0) / len(v) * 100
            scored.append((h, len(v), wr))
        if len(scored) < 4:
            return []
        scored.sort(key=lambda x: -x[2])
        best, worst = scored[0], scored[-1]
        if (best[2] - worst[2]) >= 30:
            return [{
                "category": "timing",
                "title": f"Trade quality varies by hour: {best[0]:02d}h UTC vs {worst[0]:02d}h UTC",
                "rationale": (
                    f"Best hour: {best[0]:02d}h UTC — WR {best[2]:.0f}% (n={best[1]}).\n"
                    f"Worst hour: {worst[0]:02d}h UTC — WR {worst[2]:.0f}% (n={worst[1]}).\n\n"
                    "**Informational** — there's no time-of-day gate in config yet. "
                    "Could be a meaningful pattern (US active hours, EU rotations, etc) "
                    "or coincidence. Watch and let me build a coded gate if the pattern persists."
                ),
                "actions": {},
                "confidence": "med",
                "metrics": {
                    "best_hour": best[0], "best_wr": round(best[2], 1), "best_n": best[1],
                    "worst_hour": worst[0], "worst_wr": round(worst[2], 1), "worst_n": worst[1],
                },
            }]
        return []

    def _rule_protocol_focus(self, trades, cfg):
        """PumpSwap vs Pump.fun WR comparison."""
        ps = [t for t in trades if t.get("protocol") == "pumpswap"]
        pf = [t for t in trades if t.get("protocol") in (None, "pumpfun")]
        if len(ps) < 15 or len(pf) < 30:
            return []
        wr_ps = sum(1 for t in ps if t.get("pnl_pct", 0) > 0) / len(ps) * 100
        wr_pf = sum(1 for t in pf if t.get("pnl_pct", 0) > 0) / len(pf) * 100
        if abs(wr_ps - wr_pf) < 15:
            return []
        winner, loser = ("PumpSwap", "Pump.fun") if wr_ps > wr_pf else ("Pump.fun", "PumpSwap")
        return [{
            "category": "gate",
            "title": f"{winner} outperforming {loser} by {abs(wr_ps - wr_pf):.0f}pp",
            "rationale": (
                f"PumpSwap (graduated): WR {wr_ps:.0f}% (n={len(ps)}).\n"
                f"Pump.fun (bonding curve): WR {wr_pf:.0f}% (n={len(pf)}).\n\n"
                "**Informational** — no per-protocol toggle yet. If the gap persists, "
                "could be worth tightening the band's individual entry filters."
            ),
            "actions": {},
            "confidence": "med",
            "metrics": {"ps_n": len(ps), "ps_wr": round(wr_ps, 1),
                        "pf_n": len(pf), "pf_wr": round(wr_pf, 1)},
        }]

    def _rule_pattern_tp_calibration(self, trades, cfg):
        """Per-pattern TP buffer calibration (Phase 2.8).

        For each tradeable pattern (slow_rug / predictable_dump), compare
        the observed `peak_pct_pre_rug` (how far the WINNERS actually ran
        before reversing) against the configured `pattern_tp_buffer_pct`.
        If the winners are routinely getting a lot higher than current TP,
        suggest LOOSENING the buffer (capture more upside). If they're
        getting cut SHORT and dumping AT TP, suggest TIGHTENING. Surfaces
        as a concrete one-click `pattern_tp_buffer_pct` change.

        Bot.py reads pattern_tp_buffer_pct on next score-update (every
        trade close), so the new value propagates within minutes of apply.
        """
        # Group winning trades by pattern_at_entry. Use ONLY trades that had
        # a pattern classified — unclassified trades have no exit suggestion
        # so they can't inform calibration.
        from collections import defaultdict
        bucket: dict[str, list[dict]] = defaultdict(list)
        for t in trades:
            pat = t.get("greylist_pattern_at_entry")
            if pat not in ("slow_rug_tradeable", "predictable_dump_tradeable"):
                continue
            if t.get("pnl_pct") is None:
                continue
            bucket[pat].append(t)

        suggestions: list[dict] = []
        cur_buffer = float(cfg.get("pattern_tp_buffer_pct", 2.0))

        for pat, ts in bucket.items():
            if len(ts) < 6:  # need ≥6 closed pattern trades per bucket
                continue
            # WINNERS only — losers (SL hits) have peak data that doesn't tell
            # us "winners ran past TP". For SL-rate signal we count separately.
            winners = [t for t in ts if (t.get("pnl_pct") or 0) > 0]
            sl_exits = [
                t for t in ts
                if "stop-loss" in (t.get("exit_reason") or "").lower()
            ]
            sl_rate = len(sl_exits) / len(ts) * 100
            short_pat = "slow rug" if pat == "slow_rug_tradeable" else "predictable dump"

            # PRIORITY 1: SL-rate too high → loosen (check FIRST so a pattern
            # with mostly-losing trades doesn't ALSO trigger a "tighten" via
            # the gap math, since loser PnLs make the gap look artificially big).
            if sl_rate >= 35 and cur_buffer <= 3.5:
                # Need at least SOME winner sample to estimate peak — if the
                # pattern is 100% SL, can't trust the loosen direction either.
                winner_peak = (
                    statistics.mean([
                        float(w["peak_pct_pre_rug"]) for w in winners
                        if w.get("peak_pct_pre_rug") is not None
                    ]) if winners else 0.0
                )
                new_buffer = min(5.0, round(cur_buffer + 1.0, 1))
                suggestions.append({
                    "category": "tp",
                    "title": (
                        f"{short_pat}: SL hit on {sl_rate:.0f}% of trades — "
                        f"loosen buffer to {new_buffer:.1f}%"
                    ),
                    "rationale": (
                        f"Over the last {len(ts)} closed `{pat}` trades, **{sl_rate:.0f}%** "
                        f"hit stop-loss instead of TP. The TP override is sitting too close "
                        f"to the rug edge — trades reverse and trip SL before reaching it "
                        f"(winners' avg peak: +{winner_peak:.1f}%).\n\n"
                        f"Raising `pattern_tp_buffer_pct` from {cur_buffer:.1f}% to "
                        f"{new_buffer:.1f}% moves the TP override further BELOW each "
                        f"creator's median rug, locking wins earlier. Applied on next "
                        f"score-update."
                    ),
                    "actions": {"pattern_tp_buffer_pct": new_buffer},
                    "confidence": "high" if len(ts) >= 12 else "med",
                    "metrics": {
                        "pattern": pat, "n": len(ts),
                        "winner_peak_pct": round(winner_peak, 1),
                        "sl_rate_pct": round(sl_rate, 1),
                        "current_buffer": cur_buffer,
                        "proposed_buffer": new_buffer,
                    },
                })
                continue  # don't ALSO consider tighten for this pattern

            # PRIORITY 2: winners running past TP → tighten buffer.
            # Need ≥4 winners with peak data and a healthy win-rate, otherwise
            # we'd amplify a small sample of lucky runners.
            if len(winners) < 4 or (len(winners) / len(ts)) < 0.4:
                continue
            peaks = [
                float(w["peak_pct_pre_rug"]) for w in winners
                if w.get("peak_pct_pre_rug") is not None
            ]
            if len(peaks) < 4:
                continue
            mean_peak = statistics.mean(peaks)
            mean_pnl = statistics.mean([float(w["pnl_pct"]) for w in winners])
            gap = mean_peak - mean_pnl

            if gap >= 4.0 and cur_buffer >= 1.5:
                new_buffer = max(0.5, round(cur_buffer - 1.0, 1))
                suggestions.append({
                    "category": "tp",
                    "title": (
                        f"{short_pat}: winners ran {gap:.1f}pp past TP — "
                        f"tighten buffer to {new_buffer:.1f}%"
                    ),
                    "rationale": (
                        f"Over the last {len(winners)} **winning** `{pat}` trades "
                        f"(of {len(ts)} total), the average peak reached "
                        f"**+{mean_peak:.1f}%** before reversing, but the realized "
                        f"mean PnL was only **+{mean_pnl:.1f}%** — leaving "
                        f"**{gap:.1f}pp** on the table.\n\n"
                        f"Lowering `pattern_tp_buffer_pct` from {cur_buffer:.1f}% to "
                        f"{new_buffer:.1f}% moves the TP override closer to each "
                        f"creator's observed median rug, capturing more upside before "
                        f"reversal. Applied on next score-update."
                    ),
                    "actions": {"pattern_tp_buffer_pct": new_buffer},
                    "confidence": "high" if len(winners) >= 8 else "med",
                    "metrics": {
                        "pattern": pat, "n": len(ts), "n_winners": len(winners),
                        "mean_peak_pct": round(mean_peak, 1),
                        "mean_pnl_pct": round(mean_pnl, 1),
                        "gap_pp": round(gap, 1),
                        "sl_rate_pct": round(sl_rate, 1),
                        "current_buffer": cur_buffer,
                        "proposed_buffer": new_buffer,
                    },
                })

        return suggestions


    def _rule_greylist_sniper_tuning(self, trades, cfg):
        """Auto-tune `greylist_snipe_min_score` based on sniper win-rate.

        The Greylist Sniper opens positions on every new launch from a
        creator that scored >= `greylist_snipe_min_score`. As the bot
        accumulates sniper-driven trades we can observe whether the
        threshold is too loose (taking too many losers) or too tight
        (missing winners). Closes the feedback loop:

          greylist score → sniper threshold → trade outcomes → re-tune

        Decision matrix (uses CLOSED sniper trades from the last 24h):

          - If win-rate < 35% AND n >= 10  → bump min_score by +5
            (more selective; current threshold is letting too many losers through)
          - If win-rate > 55% AND n >= 10  → drop min_score by -5
            (more aggressive; we're being too picky and missing winners)
          - Clamp: 25 <= min_score <= 90 (below 25 is meaningless, above 90
            is unreachable for almost all creators).

        Confidence:
          - "high" if n >= 20 trades in the window
          - "med"  if 10 <= n < 20

        Skips entirely when the sniper is disabled OR < 10 sniper trades.
        Returns at most ONE suggestion per analysis cycle.
        """
        if not cfg.get("greylist_snipe_enabled", True):
            return []
        # Filter to sniper-only trades. We use `classifier_action` (the
        # field bot.py stamps at entry) — pl_sources.classify_source maps
        # it to the `greylist_snipe` bucket label.
        sniper = [t for t in trades if t.get("classifier_action") == "greylist_snipe"]
        n = len(sniper)
        if n < 10:
            return []
        wins = sum(1 for t in sniper if (t.get("pnl_pct") or 0) > 0)
        wr = wins / n * 100.0
        avg_pnl = statistics.mean([float(t.get("pnl_pct") or 0) for t in sniper])
        cur = float(cfg.get("greylist_snipe_min_score", 45.0))
        # Decide direction
        if wr < 35.0:
            new = min(90.0, round(cur + 5.0, 1))
            direction = "tighten"
            reason_phrase = "letting too many losers through"
            verb_phrase = "more selective"
        elif wr > 55.0:
            new = max(25.0, round(cur - 5.0, 1))
            direction = "loosen"
            reason_phrase = "too picky — missing winners"
            verb_phrase = "more aggressive"
        else:
            return []
        if abs(new - cur) < 0.1:
            # Already at the clamp boundary — nothing to do.
            return []
        confidence = "high" if n >= 20 else "med"
        return [{
            "category": "greylist_sniper",
            "title": (
                f"Greylist Sniper WR {wr:.0f}% — "
                f"{direction} min_score from {cur:.0f} to {new:.0f}"
            ),
            "rationale": (
                f"Last 24h sniper-driven trades:\n"
                f"  - **{n}** closed positions\n"
                f"  - **{wr:.0f}%** win-rate (avg PnL {avg_pnl:+.1f}%)\n\n"
                f"Win-rate is {'below 35%' if wr < 35 else 'above 55%'} — "
                f"current `greylist_snipe_min_score={cur:.0f}` is "
                f"{reason_phrase}.\n\n"
                f"Applying will {direction} the threshold to **{new:.0f}** "
                f"(making the sniper {verb_phrase}). The greylist score → "
                f"sniper threshold → trade outcomes loop closes here: "
                f"the next cycle re-evaluates against the new threshold's "
                f"trade cohort. Dismiss if you want to override the auto-tune."
            ),
            "actions": {"greylist_snipe_min_score": new},
            "confidence": confidence,
            "metrics": {
                "n_sniper_trades": n,
                "sniper_wr_pct": round(wr, 1),
                "sniper_avg_pnl_pct": round(avg_pnl, 1),
                "current_min_score": cur,
                "proposed_min_score": new,
                "direction": direction,
            },
        }]


# Singleton holder — set on app startup
_doctor: StrategyDoctor | None = None


def get_doctor() -> StrategyDoctor | None:
    return _doctor


def set_doctor(d: StrategyDoctor) -> None:
    global _doctor
    _doctor = d
