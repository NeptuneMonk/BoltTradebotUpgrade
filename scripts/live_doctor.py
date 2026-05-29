"""
live_doctor — Doctor Live archetype scorer.

Extends the existing Strategy Doctor with REAL-TIME analysis of mints
currently passing scanner gates. Mines two archetypes from recent trade
history:

  - WINNER archetype: feature distribution of trades that closed with
    pnl_pct > 0 (any profit — per user spec).
  - EXIT-LIQUIDITY archetype: feature distribution of trades that closed
    with pnl_pct <= -10 (we were the bag-holder).

For every mint currently in BotState.tracking that has passed gates, we
score it against both archetypes and produce:
  - `winner_likeness_pct`: 0..100 (how close to winner profile)
  - `exit_liquidity_likeness_pct`: 0..100 (how close to loser profile)
  - `top_red_flags`: list[str] of features matching loser profile too well

We also produce STRATEGY-LEVEL insights — e.g. "losers had p75 social_score
<5, current passing mints have median social_score 2 — most candidates are
inheriting the exit-liquidity profile right now". These get surfaced as
NEW Doctor suggestions in the `live` category, with no proposed actions
(insight-only) so the user can decide what to do.

Architecture:
- Runs every `interval_minutes` in its own background loop (default 15 min;
  faster than the standard Doctor because passing-mint composition changes
  hour-to-hour).
- Re-mines archetypes on every cycle so insights track the current market
  regime.
- Persists the latest snapshot to `live_doctor_state` (singleton doc) so
  the GET endpoint is O(1).

Features used for both archetypes:
  - unique_buyers_at_entry (joined from launches.unique_buyers)
  - sol_inflow_at_entry    (joined from launches.sol_inflow)
  - curve_fill_pct
  - social_score
  - creator_bond_rate (graduated / max(1, created))
  - creator_tokens_created (signal of tradeable history regardless of rugs)
  - entry_usd (size band — user said small entries WR > big)
"""
from __future__ import annotations

import asyncio
import logging
import math
import statistics
from datetime import datetime, timedelta, timezone
import time
from typing import Any, Optional

logger = logging.getLogger("live_doctor")

LOOKBACK_HOURS = 24
MIN_SAMPLES_PER_ARCHETYPE = 8   # below this, archetype is "not yet learned"
DEFAULT_INTERVAL_MINUTES = 15

# Feature definitions — (name, extractor function). Extractors take a joined
# {trade_doc, launch_doc, creator_doc} dict and return a float (or None).
def _feat_buyers(j): return float((j.get("launch") or {}).get("unique_buyers") or 0)
def _feat_inflow(j): return float((j.get("launch") or {}).get("sol_inflow") or 0)
def _feat_curve(j):  return float((j.get("launch") or {}).get("curve_fill_pct") or 0)
def _feat_social(j): return float((j.get("launch") or {}).get("social_score") or 0)
def _feat_creator_bond_rate(j):
    c = j.get("creator") or {}
    created = max(1, int(c.get("tokens_created") or 0))
    return float(int(c.get("tokens_graduated") or 0)) / created
def _feat_creator_volume(j):
    return float(int((j.get("creator") or {}).get("tokens_created") or 0))
def _feat_entry_usd(j):
    return float((j.get("trade") or {}).get("entry_usd") or 0)


FEATURES = [
    ("unique_buyers", _feat_buyers),
    ("sol_inflow", _feat_inflow),
    ("curve_fill_pct", _feat_curve),
    ("social_score", _feat_social),
    ("creator_bond_rate", _feat_creator_bond_rate),
    ("creator_tokens_created", _feat_creator_volume),
    ("entry_usd", _feat_entry_usd),
]


def _stats(values: list[float]) -> dict:
    """Compact distribution: count, median, p25, p75."""
    if not values:
        return {"n": 0}
    s = sorted(values)
    n = len(s)
    return {
        "n": n,
        "median": s[n // 2],
        "p25": s[max(0, n // 4)],
        "p75": s[min(n - 1, (n * 3) // 4)],
        "mean": statistics.mean(s),
    }


def _z_distance(value: float, profile: dict) -> float:
    """Crude similarity: |value - median| / (p75 - p25 + tiny). Lower = closer
    to archetype. Capped at 4.0 so a single bad feature can't dominate."""
    spread = max(1e-6, (profile.get("p75") or 0) - (profile.get("p25") or 0))
    d = abs(value - (profile.get("median") or 0)) / spread
    return min(4.0, d)


def _likeness_from_distances(distances: list[float]) -> float:
    """Convert per-feature z-distances to a 0..100 likeness score.
    avg z=0 → 100%, avg z=2 → 50%, avg z=4 → 0%."""
    if not distances:
        return 0.0
    avg = sum(distances) / len(distances)
    return max(0.0, min(100.0, 100.0 * (1 - avg / 4.0)))


class LiveDoctor:
    def __init__(self, db, bot_state=None, hub=None):
        self.db = db
        self.bot_state = bot_state  # for tracking dict + scanner candidates
        self.hub = hub
        self._task: Optional[asyncio.Task] = None
        self.interval_minutes = DEFAULT_INTERVAL_MINUTES

    async def start(self, interval_minutes: int = DEFAULT_INTERVAL_MINUTES):
        self.interval_minutes = max(5, int(interval_minutes))
        if self._task and not self._task.done():
            return
        self._task = asyncio.create_task(self._loop(), name="live_doctor")

    async def stop(self):
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass

    async def _loop(self):
        await asyncio.sleep(120)  # let initial backfill settle
        while True:
            try:
                await self.run_once()
            except asyncio.CancelledError:
                raise
            except Exception as e:
                logger.exception(f"live_doctor cycle failed: {e}")
            await asyncio.sleep(self.interval_minutes * 60)

    async def _join_trades(self) -> list[dict]:
        """Pull recent closed trades, join with launches + creators."""
        since = (datetime.now(timezone.utc) - timedelta(hours=LOOKBACK_HOURS)).isoformat()
        trades = await self.db.trades.find(
            {"status": "closed", "exit_time": {"$gte": since},
             "ghost_entry": {"$ne": True}},
            {"_id": 0},
        ).to_list(2000)
        if not trades:
            return []
        mints = {t["mint"] for t in trades if t.get("mint")}
        creators_set = {t.get("creator") for t in trades if t.get("creator")}
        # Bulk fetch launches + creators
        launches_by_mint = {}
        if mints:
            cur = self.db.launches.find({"mint": {"$in": list(mints)}}, {"_id": 0})
            async for d in cur:
                launches_by_mint[d.get("mint")] = d
        creators_by_id = {}
        if creators_set:
            cur = self.db.creators.find({"_id": {"$in": list(creators_set)}}, {"_id": 1,
                "tokens_created": 1, "tokens_graduated": 1, "tokens_failed": 1})
            async for d in cur:
                creators_by_id[d.get("_id")] = d
        joined = []
        for t in trades:
            joined.append({
                "trade": t,
                "launch": launches_by_mint.get(t.get("mint")) or {},
                "creator": creators_by_id.get(t.get("creator")) or {},
            })
        return joined

    def _split_by_outcome(self, joined: list[dict]) -> tuple[list[dict], list[dict]]:
        """Winner = any profit (per user spec). Loser = <= -10% (bag-holder)."""
        winners, losers = [], []
        for j in joined:
            p = float((j.get("trade") or {}).get("pnl_pct") or 0)
            if p > 0:
                winners.append(j)
            elif p <= -10:
                losers.append(j)
        return winners, losers

    def _mine_archetype(self, joined: list[dict]) -> dict:
        """For each feature, compute distribution stats."""
        out = {"n_samples": len(joined), "features": {}}
        for name, fn in FEATURES:
            vals = []
            for j in joined:
                try:
                    v = fn(j)
                    if v is not None and not math.isnan(v):
                        vals.append(float(v))
                except Exception:
                    pass
            out["features"][name] = _stats(vals)
        return out

    def _score_against_archetype(self, j: dict, archetype: dict) -> tuple[float, list[tuple[str, float]]]:
        """Return (likeness_pct, [(feature, distance), ...]) for one candidate."""
        feats = archetype.get("features") or {}
        distances = []
        per_feat = []
        for name, fn in FEATURES:
            profile = feats.get(name) or {}
            if profile.get("n", 0) < MIN_SAMPLES_PER_ARCHETYPE:
                continue
            try:
                v = fn(j)
                if v is None:
                    continue
                d = _z_distance(float(v), profile)
                distances.append(d)
                per_feat.append((name, d))
            except Exception:
                pass
        return _likeness_from_distances(distances), per_feat

    async def run_once(self) -> dict:
        joined = await self._join_trades()
        winners, losers = self._split_by_outcome(joined)
        winner_arch = self._mine_archetype(winners)
        loser_arch = self._mine_archetype(losers)

        # Score currently-tracked passing mints
        candidates = await self._collect_passing_candidates()
        scored = []
        for c in candidates:
            wlike, w_per = self._score_against_archetype(c["joined"], winner_arch)
            xlike, x_per = self._score_against_archetype(c["joined"], loser_arch)
            # Red flags = features where loser-distance is small (close to
            # loser archetype) AND winner-distance is large
            flags = []
            for (name, w_d), (_, x_d) in zip(w_per, x_per):
                if x_d < 1.0 and w_d > 2.0:
                    flags.append(name)
            scored.append({
                "mint": c["mint"],
                "symbol": c.get("symbol"),
                "winner_likeness_pct": round(wlike, 1),
                "exit_liquidity_likeness_pct": round(xlike, 1),
                "top_red_flags": flags[:3],
            })
        scored.sort(key=lambda r: r["winner_likeness_pct"], reverse=True)

        # Strategy-level insight headlines
        insights = self._derive_insights(winner_arch, loser_arch, scored)

        # Circuit-breaker decision: should we pause new entries?
        breaker = await self._evaluate_circuit_breaker(joined, winners, losers, scored)
        if breaker:
            insights.insert(0, breaker)

        snapshot = {
            "_id": "live_doctor_state",
            "updated_at": datetime.now(timezone.utc).isoformat(),
            "lookback_hours": LOOKBACK_HOURS,
            "winner_archetype": winner_arch,
            "exit_liquidity_archetype": loser_arch,
            "scored_candidates": scored,
            "insights": insights,
            "feature_names": [n for n, _ in FEATURES],
        }
        await self.db.live_doctor_state.replace_one(
            {"_id": "live_doctor_state"}, snapshot, upsert=True,
        )
        logger.info(
            f"live_doctor: winners={len(winners)} losers={len(losers)} "
            f"candidates_scored={len(scored)} insights={len(insights)}"
        )
        try:
            if self.hub:
                await self.hub.broadcast("live_doctor_update", {
                    "candidates": scored[:10],
                    "insights": insights,
                })
        except Exception:
            pass
        return snapshot

    async def _collect_passing_candidates(self) -> list[dict]:
        """Pull currently-tracked mints from bot_state.tracking and join them
        with launches + creators so the same FEATURES extractors work."""
        if not self.bot_state:
            return []
        tracking = getattr(self.bot_state, "tracking", {}) or {}
        out = []
        mints = list(tracking.keys())[:200]  # bound for safety
        if not mints:
            return []
        launches_by_mint = {}
        cur = self.db.launches.find({"mint": {"$in": mints}}, {"_id": 0})
        async for d in cur:
            launches_by_mint[d.get("mint")] = d
        creators_set = {
            (launches_by_mint.get(m) or {}).get("creator") for m in mints
        } - {None, ""}
        creators_by_id = {}
        if creators_set:
            cur = self.db.creators.find({"_id": {"$in": list(creators_set)}},
                                        {"_id": 1, "tokens_created": 1,
                                         "tokens_graduated": 1, "tokens_failed": 1})
            async for d in cur:
                creators_by_id[d.get("_id")] = d
        for mint in mints:
            lj = launches_by_mint.get(mint) or {}
            cj = creators_by_id.get(lj.get("creator")) or {}
            # Build a synthetic 'trade' from tracking for entry_usd-style features
            bucket = tracking.get(mint) or {}
            synth_trade = {
                "mint": mint,
                "entry_usd": 0,  # not entered yet
                "creator": lj.get("creator"),
            }
            out.append({
                "mint": mint,
                "symbol": lj.get("symbol") or bucket.get("symbol"),
                "joined": {"trade": synth_trade, "launch": lj, "creator": cj},
            })
        return out

    def _derive_insights(self, winner_arch: dict, loser_arch: dict, scored: list[dict]) -> list[dict]:
        """Strategy-level insights derived from the archetypes. Each is a
        short headline + supporting numbers; UI renders as info cards."""
        insights = []
        wf = (winner_arch or {}).get("features") or {}
        lf = (loser_arch or {}).get("features") or {}
        n_w = winner_arch.get("n_samples", 0)
        n_l = loser_arch.get("n_samples", 0)

        if n_w < MIN_SAMPLES_PER_ARCHETYPE and n_l < MIN_SAMPLES_PER_ARCHETYPE:
            insights.append({
                "kind": "warmup",
                "title": "Doctor Live is warming up",
                "body": (
                    f"Need ≥{MIN_SAMPLES_PER_ARCHETYPE} samples per outcome class to "
                    f"mine archetypes. Currently winners={n_w}, losers={n_l} "
                    f"(last {LOOKBACK_HOURS}h)."
                ),
            })
            return insights

        # Insight 1: where do winners and losers diverge most?
        divergent = []
        for name, _ in FEATURES:
            wmed = (wf.get(name) or {}).get("median")
            lmed = (lf.get(name) or {}).get("median")
            if wmed is None or lmed is None:
                continue
            spread = max(0.001, abs(wmed) + abs(lmed))
            delta = abs(wmed - lmed) / spread
            divergent.append((name, wmed, lmed, delta))
        divergent.sort(key=lambda x: x[3], reverse=True)
        if divergent:
            top = divergent[:3]
            lines = [
                f"  • {name}: winners median={w:.2f} vs losers median={loser:.2f}"
                for (name, w, loser, _d) in top
            ]
            insights.append({
                "kind": "divergence",
                "title": "Top signals separating winners from exit-liquidity",
                "body": (
                    "Features where winners and losers diverge most "
                    f"(last {LOOKBACK_HOURS}h, n_winners={n_w}, n_losers={n_l}):\n"
                    + "\n".join(lines)
                ),
            })

        # Insight 2: how do current passing candidates compare?
        if scored:
            high_winner = [c for c in scored if c["winner_likeness_pct"] >= 60]
            exit_liq = [c for c in scored if c["exit_liquidity_likeness_pct"] >= 70]
            top_labels = []
            for c in scored[:3]:
                lbl = c.get("symbol") or c["mint"][:6]
                top_labels.append(f"{lbl} ({c['winner_likeness_pct']:.0f}%)")
            insights.append({
                "kind": "current_field",
                "title": "Current passing field vs archetypes",
                "body": (
                    f"{len(scored)} tracked mints currently. "
                    f"{len(high_winner)} look ≥60% like recent winners; "
                    f"{len(exit_liq)} look ≥70% like recent exit-liquidity. "
                    f"Top winner-likeness: {', '.join(top_labels)}"
                ),
            })

        # Insight 3: creator profile of recent losers
        loser_creator_bond = (lf.get("creator_bond_rate") or {}).get("median")
        winner_creator_bond = (wf.get("creator_bond_rate") or {}).get("median")
        if loser_creator_bond is not None and winner_creator_bond is not None:
            if loser_creator_bond + 0.1 < winner_creator_bond:
                insights.append({
                    "kind": "creator_signal",
                    "title": f"Creator bond-rate matters: winners {winner_creator_bond:.0%} vs losers {loser_creator_bond:.0%}",
                    "body": (
                        "Consider biasing entries toward creators with at least "
                        f"{winner_creator_bond:.0%} historical bond rate. "
                        "Rugs in the count are OK as long as the creator produces "
                        "tradeable launches at that ratio."
                    ),
                })
        return insights

    async def _evaluate_circuit_breaker(self, joined, winners, losers, scored):
        """Trailing-stop on bot regime score (0-100):
          - Score = 60% rolling-4h win-rate + 40% avg winner-likeness of passing field
          - Maintain rolling peak over `doctor_trail_lookback_minutes`
          - Trip when current < peak × (1 - drawdown_pct/100), AND score is below the min-score floor
          - Clear when current >= paused_peak × (recovery_pct/100)

        Doctor can adjust drawdown/recovery thresholds via the normal Doctor
        suggestion path (they're plain config fields).
        """
        cfg = await self.db.bot_config.find_one({}, {"_id": 0}) or {}
        if not cfg.get("doctor_circuit_breaker_enabled", True):
            return None

        # --- Compute regime score ---
        since_4h = (datetime.now(timezone.utc) - timedelta(hours=4)).isoformat()
        recent_trades = [
            (j.get("trade") or {}) for j in joined
            if ((j.get("trade") or {}).get("exit_time") or "") >= since_4h
        ]
        n = len(recent_trades)
        if n >= 5:
            wins = sum(1 for t in recent_trades if float(t.get("pnl_pct") or 0) > 0)
            wr = wins / n
        else:
            wr = None
        wlike_avg = (
            sum(c["winner_likeness_pct"] for c in scored) / len(scored)
            if scored else None
        )
        # If we don't have BOTH signals yet, abstain — can't compute score
        if wr is None and wlike_avg is None:
            return None
        wr_component = (wr if wr is not None else 0.5) * 100 * 0.6
        wlike_component = (wlike_avg if wlike_avg is not None else 50.0) * 0.4
        score = round(wr_component + wlike_component, 1)

        # --- Load + update trail state ---
        trail = await self.db.doctor_trail_state.find_one(
            {"_id": "trail"}, {"_id": 0}
        ) or {}
        lookback_min = float(cfg.get("doctor_trail_lookback_minutes", 240))
        peak = float(trail.get("peak") or 0)
        peak_ts = float(trail.get("peak_ts") or 0)
        now = time.time()
        # Roll the peak forward — if peak is older than lookback, reset it
        if peak_ts and (now - peak_ts) > lookback_min * 60:
            peak = score
            peak_ts = now
        elif score > peak:
            peak = score
            peak_ts = now

        drawdown_pct = float(cfg.get("doctor_trail_drawdown_pct", 40.0))
        recovery_pct = float(cfg.get("doctor_trail_recovery_pct", 70.0))
        min_score_floor = float(cfg.get("doctor_trail_min_score", 30.0))
        currently_paused = bool(trail.get("paused"))
        paused_peak = float(trail.get("paused_peak") or 0)

        trip_threshold = peak * (1 - drawdown_pct / 100.0)
        action_taken = None
        new_paused = currently_paused

        if not currently_paused:
            # Need TWO conditions to pause: drawdown from peak AND absolute floor
            if score < trip_threshold and score < min_score_floor and peak >= min_score_floor:
                new_paused = True
                paused_peak = peak
                # Long pause window — actual resume controlled by trail state
                # (we just need the bot's _enter guard to see "paused" until we clear it)
                await self.db.bot_config.update_one(
                    {},
                    {"$set": {
                        "doctor_pause_until_ts": now + 24 * 3600,
                        "doctor_pause_reason": (
                            f"Trail stop: regime score {score:.0f} fell {drawdown_pct:.0f}%+ "
                            f"from peak {peak:.0f}. Will resume when score recovers to "
                            f"{recovery_pct:.0f}% of {peak:.0f} ({peak * recovery_pct / 100:.0f})."
                        ),
                    }},
                )
                if self.bot_state and hasattr(self.bot_state, "load"):
                    try:
                        await self.bot_state.load()
                    except Exception:
                        pass
                action_taken = "PAUSED — trail tripped"
                logger.warning(f"DOCTOR TRAIL STOP TRIPPED: score={score} peak={peak}")
        else:
            # Currently paused — check for recovery
            recovery_threshold = paused_peak * (recovery_pct / 100.0)
            if score >= recovery_threshold:
                new_paused = False
                paused_peak = 0
                # Clear the pause
                await self.db.bot_config.update_one(
                    {},
                    {"$set": {
                        "doctor_pause_until_ts": 0,
                        "doctor_pause_reason": "",
                    }},
                )
                if self.bot_state and hasattr(self.bot_state, "load"):
                    try:
                        await self.bot_state.load()
                    except Exception:
                        pass
                action_taken = f"RESUMED — score recovered to {score}"
                logger.warning(f"DOCTOR TRAIL STOP RELEASED: score={score} >= recovery {recovery_threshold}")

        # Persist trail state
        await self.db.doctor_trail_state.replace_one(
            {"_id": "trail"},
            {
                "_id": "trail",
                "score": score,
                "score_wr_component": round(wr_component, 1),
                "score_wlike_component": round(wlike_component, 1),
                "n_recent_trades": n,
                "win_rate_4h": round(wr * 100, 1) if wr is not None else None,
                "avg_winner_likeness": round(wlike_avg, 1) if wlike_avg is not None else None,
                "peak": round(peak, 1),
                "peak_ts": peak_ts,
                "trip_threshold": round(trip_threshold, 1),
                "drawdown_pct": drawdown_pct,
                "recovery_pct": recovery_pct,
                "min_score_floor": min_score_floor,
                "paused": new_paused,
                "paused_peak": round(paused_peak, 1) if paused_peak else 0,
                "last_evaluated_at": datetime.now(timezone.utc).isoformat(),
            },
            upsert=True,
        )

        # Always surface a status insight so the UI can render the trail
        if new_paused or action_taken:
            return {
                "kind": "circuit_breaker",
                "title": "Trail stop: " + (
                    "PAUSED — waiting for recovery" if new_paused
                    else "active — monitoring"
                ),
                "body": (
                    f"Regime score: {score:.0f} (wr {wr*100:.0f}% × 60% + "
                    f"winner-like {wlike_avg:.0f}% × 40%)\n"
                    f"Peak: {peak:.0f} · drawdown trail: {drawdown_pct:.0f}% "
                    f"(trip at {trip_threshold:.0f})\n"
                    + (f"PAUSED on peak {paused_peak:.0f}; auto-resume at "
                       f"{paused_peak * recovery_pct / 100:.0f}\n"
                       if new_paused else "")
                    + (f"\nAction: {action_taken}" if action_taken else "")
                ),
            }
        return None

    async def get_snapshot(self) -> dict:
        doc = await self.db.live_doctor_state.find_one(
            {"_id": "live_doctor_state"}, {"_id": 0},
        )
        return doc or {
            "updated_at": None,
            "winner_archetype": {"n_samples": 0, "features": {}},
            "exit_liquidity_archetype": {"n_samples": 0, "features": {}},
            "scored_candidates": [],
            "insights": [{
                "kind": "warmup",
                "title": "Doctor Live hasn't run yet",
                "body": "First cycle completes ~2 min after backend start.",
            }],
            "feature_names": [n for n, _ in FEATURES],
        }
