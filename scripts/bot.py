"""
Bot orchestrator:
- Holds BotConfig & ClassifierRules state (persisted in Mongo)
- Receives new launches + trade events from the listener
- Tracks per-mint mempool metrics (unique buyers, SOL inflow) for first ~60s
- Computes a "social trending" score for the token name (no X API)
- Decides entry after a small assessment delay; monitors held positions for exit
"""
import asyncio
import logging
import time
from collections import deque
from datetime import datetime, timezone, timedelta

from solders.pubkey import Pubkey

from models import BotConfig, ClassifierRules, Launch, Trade, now_utc
from classifier import classify
import pumpfun
import pumpswap
from solana_client import get_sol_usd_price, LAMPORTS_PER_SOL
from wallet import get_keypair, get_pubkey
from social import score_term
from ws_hub import hub
from creator_history import record_new_launch, mark_outcome, get_creator, derive_rug_count
from scanner import MomentumScanner, velocity_pct_strict
from discovery import PumpfunDiscovery
from speed_modes import (
    speed_mode_resolve, estimate_tx_fee_sol, auto_tuner,
    CU_PUMPFUN, CU_PUMPSWAP,
)
from pnl_reconciler import PnLReconciler

logger = logging.getLogger("bot")

ASSESS_DELAY_S = 3.0          # wait this long after launch before deciding entry
TRACK_DURATION_S = 60.0       # short-window heavy tracking (for fresh-launch classifier)
SCANNER_TRACK_HOURS = 4       # how long we keep light tracking for the scanner
PERSIST_INTERVAL_S = 2.0      # how often to flush tracker metrics to DB
MAX_TRACKED_MINTS = 500       # cap memory


class BotState:
    def __init__(self, db):
        self.db = db
        self.config = BotConfig()
        self.rules = ClassifierRules()
        self.active_trades: dict[str, dict] = {}
        self.recent_launches: list[dict] = []
        self.kill_switch_tripped = False
        self.listener_connected = False
        self.tracking: dict[str, dict] = {}
        # Re-entry watchlist: mint -> {exit_price_sol, exit_time, attempts, ...}
        self.reentry_watch: dict[str, dict] = {}
        self._reentry_task: asyncio.Task | None = None
        # Mints we have already entered (or attempted) so the scanner doesn't double-trade
        self.entered_mints: set[str] = set()
        self._scanner_task: asyncio.Task | None = None
        # Smart-stop flag: when True, refuse new entries but let active positions
        # ride to their natural exits. A background task auto-flips enabled=False
        # once active_trades is empty.
        self.stopping_gracefully: bool = False
        self._graceful_stop_task: asyncio.Task | None = None
        # Reservation pattern: serialize the position-count gate in _enter so
        # concurrent scanner attempts can't all race past max_concurrent_positions.
        # `_pending_entry_mints` holds mints that have passed the gate but
        # haven't yet been added to active_trades (tx in flight). The gate
        # checks len(active_trades) + len(_pending_entry_mints) >= max.
        self._entry_gate_lock = asyncio.Lock()
        self._pending_entry_mints: set[str] = set()
        # Stop-loss cooldown: mint -> unix timestamp when cooldown expires.
        # Populated by `_exit_impl` when reason starts with "stop-loss hit".
        # Checked inside the entry gate lock so concurrent attempts agree.
        self.sl_cooldown_until: dict[str, float] = {}
        # Universal post-exit cooldown: mint -> unix timestamp. Prevents the
        # scanner from re-entering the SAME mint within the cooldown window
        # after ANY exit (TP, SL, timeout, classifier, hard-stop). Fixes
        # the bleed pattern where the bot opened 4 positions for the same
        # mint in 3 minutes, each one's monitor racing the others' sells.
        self.recent_exit_until: dict[str, float] = {}
        # Greylist Sniper rate cap — rolling per-hour fire counter so a wave
        # of greylist launches can't blow through the wallet. Cleared by
        # `_gc_greylist_snipe_counter` every minute.
        self._greylist_snipe_fires: list[float] = []
        self.scanner = MomentumScanner(self)
        self.discovery = PumpfunDiscovery(self)
        self.pnl_reconciler = PnLReconciler(self)

    async def load(self):
        cfg = await self.db.bot_config.find_one({"_id": "current"}, {"_id": 0})
        if cfg:
            self.config = BotConfig(**cfg)
        rules = await self.db.classifier_rules.find_one({"_id": "current"}, {"_id": 0})
        if rules:
            self.rules = ClassifierRules(**rules)
        # SAFETY: Always start with trading disabled, regardless of what was
        # persisted before the last shutdown. A crashed/restarted process
        # should never automatically resume real-money trading — the user
        # must press Start in the UI after confirming everything is healthy.
        # We do NOT clear `live_trading` here so the user's live/paper mode
        # preference is preserved across restarts; only the `enabled` flag is
        # forced off.
        was_running_before_restart = self.config.enabled
        if was_running_before_restart:
            self.config.enabled = False
            await self.db.bot_config.update_one(
                {"_id": "current"},
                {"$set": {"enabled": False}},
                upsert=True,
            )
            logger.warning(
                "BOT WAS RUNNING BEFORE THIS PROCESS START — auto-disabled "
                "for safety. Press Start in the UI to resume trading."
            )
        async for t in self.db.trades.find({"status": "active"}, {"_id": 0}):
            # Persist legacy active trades that lack the new protocol field —
            # we can't safely respawn a monitor for them since price polling
            # needs the protocol routing. They get force-closed below.
            self.active_trades[t["mint"]] = {
                "trade": t,
                "protocol": t.get("protocol", "pumpfun"),
                "pumpswap_pool": t.get("pumpswap_pool") or "",
                # Restore per-trade greylist overrides for resumed positions.
                # Without this, a backend restart mid-trade would silently
                # revert that position to BotConfig defaults — breaking the
                # "the position keeps the params it opened with" contract.
                "greylist_overrides": t.get("greylist_overrides_at_entry") or {},
                "greylist_strategy": t.get("greylist_strategy_at_entry"),
            }
        # Sweep duplicate active rows in DB. Concurrent _enter races (now fixed
        # via the entry_gate_lock) could have created multiple `status=active`
        # rows for the same mint in the past. The dict above naturally
        # de-duplicates in memory (only the last-loaded row wins), but the
        # orphaned DB rows would otherwise count toward portfolio limits and
        # never get monitored. Mark them as zombies so they're out of the way.
        await self._sweep_duplicate_active_rows()
        # Sweep legacy active trades that lack the `protocol` field. These
        # were opened before protocol was persisted, so a respawned monitor
        # would default-route to pumpfun and potentially mis-trade. Safer to
        # mark them as exit_failed_terminal — the user retains the tokens and
        # can recover them manually via their wallet UI.
        await self._sweep_legacy_active_without_protocol()
        # Respawn monitor tasks for surviving active trades. After a backend
        # restart, in-memory monitors are gone; without this, positions sit
        # with status='active' forever, with nothing watching their TP/SL.
        for mint in list(self.active_trades.keys()):
            asyncio.create_task(self._monitor_position(mint))
            logger.info(f"respawned monitor for active position {mint}")
        # Start re-entry watcher
        if self._reentry_task is None or self._reentry_task.done():
            self._reentry_task = asyncio.create_task(self._reentry_watcher())
        # Start momentum scanner
        if self._scanner_task is None or self._scanner_task.done():
            self._scanner_task = asyncio.create_task(self.scanner.loop())
        # Start Pump.fun discovery (aged tokens)
        self.discovery.start()
        # Start priority-fee auto-tuner (only consulted when speed_mode='auto')
        auto_tuner.start()
        # Start the account-event bus: one persistent Helius WSS that
        # multiplexes accountSubscribe calls for every open position.
        # Drives push-based wakes in _monitor_position so SL/TP can react
        # within one network RTT of a trade landing, vs the previous
        # 400-800ms polling floor.
        from account_event_bus import account_event_bus
        account_event_bus.start()
        # Start on-chain PnL reconciler (overwrites quoted pnl with actual
        # wallet deltas read from getTransaction every 30s)
        self.pnl_reconciler.start()
        # Periodically reconcile in-memory active_trades against DB so any
        # mints leaked by an unhandled exit exception get re-attached to a
        # monitor instead of sitting orphaned in DB.
        asyncio.create_task(self._active_trades_reconciler_loop())
        # Surface the auto-disable to any WS clients listening — front-end
        # will show "Bot auto-disabled on restart" toast if connected.
        if was_running_before_restart:
            await hub.broadcast("bot_auto_disabled_on_restart", {
                "active_positions": len(self.active_trades),
            })

    def _resolve_fees(self) -> tuple[int, int, int]:
        """Return (priority_fee_microlamports, slippage_bps, exit_slippage_bps)
        applying the current speed_mode preset. Falls back to raw config when
        speed_mode='manual' or unrecognised."""
        return speed_mode_resolve(
            self.config.speed_mode,
            self.config.priority_fee_microlamports,
            self.config.slippage_bps,
            (self.config.exit_slippage_bps
             if self.config.exit_slippage_bps > 0
             else self.config.slippage_bps),
            auto_priority_cache=auto_tuner.current_value,
        )

    def _is_panic_exit(self, reason: str) -> bool:
        """Return True for exits where landing the sell matters more than the
        price (stop-loss, hard-stop, classifier abort, bonding-curve complete,
        OR trailing-stop on a position that already peaked >20% — those are
        volatile exits where price can drop another 10-20% between IX build
        and tx land, and the standard 10% slippage gets exceeded).
        These get wider `panic_exit_slippage_bps` to avoid 6003 reverts on dumps.
        """
        r = (reason or "").lower()
        if any(k in r for k in (
            "stop-loss", "hard-stop", "classifier", "bonding curve completed"
        )):
            return True
        # Trailing-stop on a hot position — extract peak pct from the reason
        # string ("trailing-stop hit (peak +40.3%, now +32.4%)") and tier up
        # when peak ≥ 20%. Tokens that ran that hard are still volatile on
        # the way down and need 25% slippage to land the sell.
        if "trailing-stop" in r:
            try:
                # parse "(peak +XX.X%"
                idx = r.find("peak +")
                if idx >= 0:
                    peak_str = r[idx + 6:idx + 12].split("%")[0]
                    peak_val = float(peak_str)
                    if peak_val >= 20.0:
                        return True
            except (ValueError, IndexError):
                pass
        return False

    def _exit_slip_for(self, reason: str, base_exit_slip_bps: int) -> int:
        """Resolve the slippage tier for a given exit. Panic exits widen to
        `config.panic_exit_slippage_bps` (default 25%); normal exits stay at
        the resolved exit slippage from speed_mode/config."""
        if self._is_panic_exit(reason):
            panic = int(getattr(self.config, "panic_exit_slippage_bps", 2500) or 2500)
            return max(panic, base_exit_slip_bps)
        return base_exit_slip_bps

    # ---------- Intelligent Exit v2 helpers ----------
    def _compute_auto_exit_slip_bps(self, *, panic: bool, pool_depth_sol: float,
                                    recent_vol_pct: float | None) -> int:
        """Exchange-style exit slippage: base 3% + thin-pool/vol/panic adders,
        hard-capped. Replaces the flat panic_exit_slippage_bps=2500 (25%).

        Same formula for pumpfun AND pumpswap — both protocols expose pool depth
        in SOL (vsr for pumpfun, quote_reserves for pumpswap).
        """
        cfg = self.config
        bps = int(cfg.auto_exit_slip_base_bps)
        if pool_depth_sol > 0 and pool_depth_sol < cfg.auto_exit_slip_thin_pool_sol:
            bps += int(cfg.auto_exit_slip_thin_pool_extra_bps)
        if recent_vol_pct is not None and recent_vol_pct >= cfg.auto_exit_slip_vol_threshold_pct:
            bps += int(cfg.auto_exit_slip_high_vol_extra_bps)
        if panic:
            bps += int(cfg.auto_exit_slip_panic_extra_bps)
        return min(bps, int(cfg.auto_exit_slip_cap_bps))

    def _recent_vol_pct(self, samples: list[tuple[float, float]],
                        window_s: int) -> float | None:
        """Std-dev / mean × 100 over the last `window_s` seconds of price
        samples. None if insufficient data."""
        if not samples or len(samples) < 4:
            return None
        now = time.time()
        recent = [p for (ts, p) in samples if now - ts <= window_s]
        if len(recent) < 4:
            return None
        mean = sum(recent) / len(recent)
        if mean <= 0:
            return None
        var = sum((p - mean) ** 2 for p in recent) / len(recent)
        std = var ** 0.5
        return (std / mean) * 100.0

    def _pool_depth_sol(self, state: dict, protocol: str) -> float:
        """Effective SOL liquidity depth, protocol-agnostic.
        - pumpfun: virtual SOL reserves of the bonding curve
        - pumpswap: WSOL reserves of the AMM pool"""
        if not state:
            return 0.0
        if protocol == "pumpswap":
            return (state.get("quote_reserves") or 0) / LAMPORTS_PER_SOL
        return (state.get("virtual_sol_reserves") or 0) / LAMPORTS_PER_SOL

    def _check_breach_persistence(self, slot: dict, *, kind: str, breached: bool,
                                  persistence_ms: int, min_samples: int,
                                  severity_pct: float = 0.0,
                                  severity_threshold_pct: float = 0.0) -> bool:
        """Returns True iff an exit condition (`kind` = "sl" or "ts") has been
        continuously breached long enough to fire.

        Logic:
        - First breach → record start timestamp + sample count = 1
        - Subsequent breaches → increment sample count
        - Recovery (breached=False) → CLEAR the breach state (resets the timer)
        - Returns True only when BOTH age >= persistence_ms AND samples >= min_samples
        - **Severity override**: if `severity_pct` exceeds `severity_threshold_pct`
          (e.g. price has fallen 5%+ BEYOND the SL trigger), fire IMMEDIATELY.
          Persistence is meant to filter millisecond blips; a sustained sharp
          dump shouldn't be made worse by waiting another 1.2s to confirm.
        """
        key_since = f"{kind}_breached_since"
        key_count = f"{kind}_breached_samples"
        now = time.time()
        if not breached:
            # Price recovered — reset
            if slot.get(key_since) is not None:
                slot[key_since] = None
                slot[key_count] = 0
            return False
        # Still in breach
        # Severity override: dump is already much worse than the gate trigger
        # → fire fast to cap the bleed.
        #
        # 2026-02-08: Even severity overrides require AT LEAST 2 samples within
        # ~300ms before firing. Paper data caught a "SL hit -62%" event where
        # the raw entry→exit move was +0.61% (a single downward wick from a
        # bad RPC quote / one outsized sell event). Without the 2-sample
        # floor, a single bad tick could close a profitable position at
        # invented severity.
        if severity_threshold_pct > 0 and severity_pct >= severity_threshold_pct:
            if slot.get(key_since) is None:
                slot[key_since] = now
                slot[key_count] = 1
                return False
            slot[key_count] = (slot.get(key_count) or 0) + 1
            return slot[key_count] >= 2
        if slot.get(key_since) is None:
            slot[key_since] = now
            slot[key_count] = 1
            return False
        slot[key_count] = (slot.get(key_count) or 0) + 1
        age_ms = (now - slot[key_since]) * 1000.0
        return age_ms >= persistence_ms and slot[key_count] >= min_samples

    # ---------- Graceful stop ----------
    async def begin_graceful_stop(self):
        """Stop opening new positions immediately, but let active trades ride
        to their natural TP/SL/trailing/timeout exits. A background watcher
        finalises the stop (flips `enabled=False`) once all positions close.

        Idempotent — calling twice is a no-op.
        """
        if self.stopping_gracefully:
            return
        self.stopping_gracefully = True
        await hub.broadcast("bot_stopping_graceful", {
            "active_positions": len(self.active_trades),
        })
        # Spin up the finaliser (or reuse the existing one)
        if self._graceful_stop_task is None or self._graceful_stop_task.done():
            self._graceful_stop_task = asyncio.create_task(self._graceful_stop_finaliser())
        # If there are no active positions, finalise immediately
        if not self.active_trades:
            await self._finalise_graceful_stop()

    async def cancel_graceful_stop(self):
        """User pressed Start while we were in graceful-stop mode — abort the
        wind-down and resume normal trading."""
        if not self.stopping_gracefully:
            return
        self.stopping_gracefully = False
        await hub.broadcast("bot_stopping_cancelled", {})

    async def _graceful_stop_finaliser(self):
        """Polls until active_trades drains, then flips enabled=False."""
        try:
            while self.stopping_gracefully:
                if not self.active_trades:
                    await self._finalise_graceful_stop()
                    return
                await asyncio.sleep(2.0)
        except asyncio.CancelledError:
            raise
        except Exception as e:
            logger.exception(f"graceful stop finaliser error: {e}")

    async def _finalise_graceful_stop(self):
        if not self.stopping_gracefully:
            return
        self.config.enabled = False
        self.stopping_gracefully = False
        await self.save_config()
        await hub.broadcast("bot_stopped", {"reason": "graceful_complete"})

    async def hard_stop(self):
        """Immediate hard stop — disable trading AND force-exit every open
        position right now. Used for emergencies or when the user can't wait
        for natural exits."""
        self.stopping_gracefully = False
        self.config.enabled = False
        await self.save_config()
        mints = list(self.active_trades.keys())
        await hub.broadcast("bot_hard_stop", {"closing": len(mints)})
        for mint in mints:
            try:
                await self._exit(mint, reason="hard-stop (user requested)")
            except Exception as e:
                logger.exception(f"hard-stop exit failed for {mint}: {e}")

    async def save_config(self):
        await self.db.bot_config.update_one(
            {"_id": "current"},
            {"$set": {**self.config.model_dump(), "_id": "current"}},
            upsert=True,
        )

    async def save_rules(self):
        await self.db.classifier_rules.update_one(
            {"_id": "current"},
            {"$set": {**self.rules.model_dump(), "_id": "current"}},
            upsert=True,
        )

    async def _active_trades_reconciler_loop(self):
        """Every 15s, find DB rows with status=active whose mint is NOT in
        `self.active_trades` (orphaned by unhandled exit exceptions, prior
        bugs, etc.) OR whose monitor heartbeat is stale (dead monitor task)
        and re-attach a fresh monitor.
        """
        await asyncio.sleep(10.0)  # let load() finish first
        while True:
            try:
                await self._reattach_orphaned_active_rows()
            except asyncio.CancelledError:
                raise
            except Exception as e:
                logger.exception(f"active_trades reconciler error: {e}")
            await asyncio.sleep(15.0)

    async def _reattach_orphaned_active_rows(self):
        # Sweep expired cooldowns from the in-memory maps. Cheap O(N) walk —
        # the maps are naturally bounded by recent exits in their windows.
        now = time.time()
        for mint in list(self.sl_cooldown_until.keys()):
            if self.sl_cooldown_until[mint] <= now:
                del self.sl_cooldown_until[mint]
        for mint in list(self.recent_exit_until.keys()):
            if self.recent_exit_until[mint] <= now:
                del self.recent_exit_until[mint]

        cursor = self.db.trades.find({"status": "active"}, {"_id": 0})
        reattached = 0
        respawned = 0
        seen_mints = set()
        async for t in cursor:
            mint = t.get("mint")
            if not mint:
                continue
            seen_mints.add(mint)
            slot = self.active_trades.get(mint)
            if slot is None:
                # Orphan — in DB but not in memory. Rebuild slot + monitor.
                self.active_trades[mint] = {
                    "trade": t,
                    "protocol": t.get("protocol", "pumpfun"),
                    "pumpswap_pool": t.get("pumpswap_pool") or "",
                }
                asyncio.create_task(self._monitor_position(mint))
                reattached += 1
                logger.warning(
                    f"reattached orphaned active row for {t.get('symbol','?')} "
                    f"({mint}) — was in DB but not in active_trades"
                )
            else:
                # In memory — check if monitor is alive. Dead monitors leave
                # `last_monitor_tick` stale. Respawn if last tick > 15s ago.
                last_tick = float(slot.get("last_monitor_tick") or 0.0)
                if time.time() - last_tick > 15.0:
                    asyncio.create_task(self._monitor_position(mint))
                    respawned += 1
                    logger.warning(
                        f"respawned dead monitor for {t.get('symbol','?')} ({mint}) — "
                        f"last_monitor_tick was {time.time() - last_tick:.0f}s ago"
                    )
        if reattached or respawned:
            logger.warning(
                f"active-trades reconciler: reattached={reattached} orphans, "
                f"respawned={respawned} dead monitors"
            )

    async def _sweep_legacy_active_without_protocol(self):
        """Active trades persisted before the `protocol` field was added to
        the Trade model have no routing info — a respawned monitor would
        default to pumpfun, which is wrong for any graduated/PumpSwap mint.
        Mark them as terminally-failed so they stop counting against the
        position cap. The user keeps the tokens in their wallet and can
        recover via any standard Solana UI."""
        stuck_mints = [
            m for m, slot in self.active_trades.items()
            if not slot["trade"].get("protocol")
        ]
        if not stuck_mints:
            return
        for mint in stuck_mints:
            slot = self.active_trades.pop(mint, None)
            if not slot:
                continue
            tid = slot["trade"].get("id")
            sym = slot["trade"].get("symbol") or "?"
            await self.db.trades.update_one(
                {"id": tid},
                {"$set": {
                    "status": "exit_failed_terminal",
                    "exit_time": now_utc().isoformat(),
                    "exit_reason": "stuck active row from older code path (no protocol field) — bot can't safely re-monitor; recover tokens manually via your wallet",
                    "pnl_sol": 0.0,
                    "pnl_usd": 0.0,
                    "pnl_pct": 0.0,
                }},
            )
            logger.warning(
                f"force-closed stuck active row for {sym} ({mint}) — "
                f"missing protocol field, manual token recovery required"
            )

    async def _sweep_duplicate_active_rows(self):
        """For each mint with multiple `status=active` rows in the DB, keep the
        most-recent one (assumed to be the one held in `self.active_trades`)
        and mark the rest as `zombie_duplicate` with pnl=0. Runs once at load.

        This is a recovery path for historical races — the new
        `_entry_gate_lock` prevents fresh duplicates from being created.
        """
        pipeline = [
            {"$match": {"status": "active"}},
            {"$group": {"_id": "$mint", "n": {"$sum": 1}, "ids": {"$push": "$id"}}},
            {"$match": {"n": {"$gt": 1}}},
        ]
        groups: list[dict] = []
        async for g in self.db.trades.aggregate(pipeline):
            groups.append(g)
        if not groups:
            return
        total_zombied = 0
        for g in groups:
            mint = g["_id"]
            ids = g["ids"]
            # Keep the row that we loaded into active_trades (its `id` is the
            # most-recently-inserted, since dict assignment overwrites and the
            # DB find returns insertion order). Mark all others as zombies.
            keep_id = self.active_trades.get(mint, {}).get("trade", {}).get("id")
            for tid in ids:
                if tid == keep_id:
                    continue
                await self.db.trades.update_one(
                    {"id": tid},
                    {"$set": {
                        "status": "zombie_duplicate",
                        "exit_time": now_utc().isoformat(),
                        "exit_reason": "orphaned duplicate row from race (no monitor was watching this row)",
                        "pnl_sol": 0.0,
                        "pnl_usd": 0.0,
                        "pnl_pct": 0.0,
                    }},
                )
                total_zombied += 1
        logger.warning(
            f"swept {total_zombied} duplicate active rows across "
            f"{len(groups)} mints — these had no monitor watching them"
        )

    async def daily_pnl_usd(self, mode: str | None = None) -> float:
        """Sum of pnl_usd for trades closed today (UTC). Pass mode='live' or
        'paper' to filter; default (None) returns the combined total.

        IMPORTANT: the kill switch must use mode='live' because we don't want
        paper losses to trip the real-money bot, and we don't want paper
        winnings to mask real losses.

        When `bot_config.live_pnl_reset_at` is set, the live-mode aggregation
        starts from that timestamp instead of today's 00:00 UTC. Used to wipe
        poisoned counters without deleting trade rows.
        """
        start = datetime.now(timezone.utc).replace(
            hour=0, minute=0, second=0, microsecond=0
        )
        cutoff_iso = start.isoformat()
        if mode == "live" and self.config.live_pnl_reset_at:
            try:
                reset = datetime.fromisoformat(self.config.live_pnl_reset_at)
                if reset.tzinfo is None:
                    reset = reset.replace(tzinfo=timezone.utc)
                if reset > start:
                    cutoff_iso = reset.isoformat()
            except Exception:
                pass
        query: dict = {"status": "closed", "exit_time": {"$gte": cutoff_iso}}
        if mode in ("live", "paper"):
            query["mode"] = mode
        cursor = self.db.trades.find(query, {"_id": 0, "pnl_usd": 1})
        total = 0.0
        async for d in cursor:
            total += float(d.get("pnl_usd", 0.0))
        return total

    async def check_kill_switch(self) -> bool:
        # Live-only — paper trades must never trip the real-money kill switch
        pnl = await self.daily_pnl_usd(mode="live")
        if pnl <= -abs(self.config.daily_kill_switch_usd):
            self.kill_switch_tripped = True
            self.config.enabled = False
            await self.save_config()
            return True
        return False

    # ---------- Re-entry on winners ----------
    async def _reentry_watcher(self):
        """Background scanner: for every closed-profitable mint on the watchlist,
        watch the price for a pullback. If pullback >= configured pct AND the
        token has not died (real_sol_reserves still growing), re-enter at a
        smaller size. Capped by max_attempts per mint."""
        while True:
            try:
                now = time.time()
                to_remove: list[str] = []
                for mint, w in list(self.reentry_watch.items()):
                    if not self.config.reentry_enabled:
                        to_remove.append(mint)
                        continue
                    if now - w["exit_time"] > w["window_s"]:
                        to_remove.append(mint)
                        continue
                    if w["attempts"] >= w["max_attempts"]:
                        to_remove.append(mint)
                        continue
                    if mint in self.active_trades:
                        continue  # already re-entered; wait for that to close
                    if not self.config.enabled or self.kill_switch_tripped:
                        continue
                    if await self.check_kill_switch():
                        continue

                    state = await pumpfun.fetch_bonding_curve_state(mint)
                    if not state or state["complete"]:
                        to_remove.append(mint)
                        continue
                    cur_price = state["virtual_sol_reserves"] / state["virtual_token_reserves"] / LAMPORTS_PER_SOL
                    # Track peak after exit so we measure pullback from local high
                    if cur_price > w["peak_price_after_exit"]:
                        w["peak_price_after_exit"] = cur_price
                    pullback = (w["peak_price_after_exit"] - cur_price) / max(w["peak_price_after_exit"], 1e-18) * 100
                    if pullback >= w["pullback_pct"]:
                        # Optional sanity: only re-enter if curve still has inflow
                        # (compare cur_real_sol vs threshold). Skip strict check for now.
                        try:
                            await self._attempt_reentry(w)
                        except Exception as e:
                            logger.exception(f"reentry attempt failed for {mint}: {e}")
                for m in to_remove:
                    self.reentry_watch.pop(m, None)
                    await hub.broadcast("reentry_watch_remove", {"mint": m})
            except Exception as e:
                logger.debug(f"reentry watcher loop error: {e}")
            await asyncio.sleep(2.0)

    async def _attempt_reentry(self, w: dict):
        mint = w["mint"]
        # Don't open new positions during a graceful stop
        if self.stopping_gracefully:
            return
        # Atomic reservation — same pattern as _enter
        async with self._entry_gate_lock:
            if mint in self.active_trades or mint in self._pending_entry_mints:
                return
            cap = max(1, self.config.max_concurrent_positions)
            in_flight = len(self.active_trades) + len(self._pending_entry_mints)
            if in_flight >= cap:
                return
            # SL cooldown applies to re-entry watcher too — if the previous
            # exit was SL, give the price action time to settle.
            cd_until = self.sl_cooldown_until.get(mint, 0.0)
            if cd_until and time.time() < cd_until:
                return
            self._pending_entry_mints.add(mint)
        try:
            await self._attempt_reentry_impl(w)
        finally:
            self._pending_entry_mints.discard(mint)

    async def _attempt_reentry_impl(self, w: dict):
        mint = w["mint"]
        sol_price = await get_sol_usd_price()
        base_usd = max(self.config.min_trade_usd, self.config.max_trade_usd)
        trade_usd = max(self.config.min_trade_usd, base_usd * w["size_multiplier"])
        trade_sol = trade_usd / sol_price if sol_price > 0 else 0
        sol_in_lamports = int(trade_sol * LAMPORTS_PER_SOL)
        if sol_in_lamports <= 0:
            return
        state = await pumpfun.fetch_bonding_curve_state(mint)
        if not state or state["complete"]:
            return
        real_sol = state["real_sol_reserves"] / LAMPORTS_PER_SOL
        if real_sol < self.config.min_curve_liquidity_sol:
            return
        eff_priority, eff_slip, _ = self._resolve_fees()
        tokens_out, max_sol = pumpfun.quote_buy_tokens(state, sol_in_lamports, eff_slip)
        if tokens_out <= 0:
            return
        entry_price_sol = sol_in_lamports / tokens_out / LAMPORTS_PER_SOL
        mode = "live" if self.config.live_trading else "paper"
        est_entry_fee_sol = estimate_tx_fee_sol(eff_priority, CU_PUMPFUN)
        creator_str = w.get("creator") or ""
        trade = Trade(
            mint=mint,
            creator=creator_str or None,
            name=w.get("name"),
            symbol=w.get("symbol"),
            status="active",
            mode=mode,
            entry_sol=trade_sol,
            entry_usd=trade_sol * sol_price,
            entry_tokens=tokens_out,
            entry_price_sol=entry_price_sol,
            entry_fee_sol=est_entry_fee_sol,
            speed_mode_at_entry=self.config.speed_mode,
            risk_score=40,
            classifier_action="reentry",
        )
        if mode == "live":
            if not creator_str:
                logger.error(f"re-entry skipped {mint}: missing creator (required for creator_vault PDA)")
                return
            try:
                kp = get_keypair()
                user = get_pubkey()
                mint_pk = Pubkey.from_string(mint)
                bc_state = await pumpfun.fetch_bonding_curve_state(mint)
                curve_creator = (bc_state or {}).get("creator") or creator_str
                creator_pk = Pubkey.from_string(curve_creator)
                trade.creator = curve_creator
                tp = await pumpfun.get_mint_token_program(mint)
                ixs = [
                    pumpfun.build_create_ata_ix(user, user, mint_pk, tp),
                    await pumpfun.build_buy_ix(user, mint_pk, tokens_out, max_sol, creator_pk, tp),
                ]
                sig = await pumpfun.send_versioned_tx(kp, ixs, eff_priority)
                trade.entry_sig = sig
            except Exception as e:
                logger.exception(f"Live re-entry buy failed for {mint}: {e}")
                trade.status = "failed"
                trade.exit_reason = f"reentry buy failed: {e}"
                await self._persist_trade(trade)
                return
        await self._persist_trade(trade)
        w["attempts"] += 1
        self.active_trades[mint] = {"trade": trade.model_dump(), "launch": {"creator": w.get("creator"), "mint": mint}}
        await hub.broadcast("trade_enter", trade.model_dump())
        await hub.broadcast("reentry_attempted", {"mint": mint, "attempts": w["attempts"]})
        asyncio.create_task(self._monitor_position(mint))

    # ---------- Listener handlers ----------
    async def on_launch(self, launch_data: dict):
        """New Pump.fun token created."""
        launch = Launch(
            mint=launch_data["mint"],
            creator=launch_data["creator"],
            bonding_curve=launch_data["bonding_curve"],
            name=launch_data.get("name"),
            symbol=launch_data.get("symbol"),
            signature=launch_data.get("signature"),
        )

        # Record into creator history & get rug count
        creator_doc = await record_new_launch(self.db, launch.creator, launch.mint)
        creator_rugs = derive_rug_count(creator_doc)

        # Phase 2.8 / user feedback — refresh the creator's greylist score on
        # every observed launch. Without this, creators sitting in the F-band
        # (5 ≤ tokens_failed < 80) only get scored when (a) they graduate,
        # (b) we close a trade on them, or (c) the 6h failure-sweep cycle
        # touches one of their new failures. For creators we've NEVER traded
        # — which is most of them — that meant they were absent from the
        # greylist surface despite being prime candidates. Now every fresh
        # launch keeps the score current. Cheap — Mongo-only.
        if (creator_doc and self.config.creator_greylist_enabled
                and (creator_doc.get("tokens_failed") or 0) >= int(self.config.creator_greylist_min_fails)):
            try:
                from creator_greylist import update_creator_score
                await update_creator_score(
                    self.db, launch.creator,
                    min_fails=int(self.config.creator_greylist_min_fails),
                    max_fails=int(self.config.creator_greylist_max_fails),
                    tp_buffer=float(self.config.pattern_tp_buffer_pct),
                )
            except Exception as e:
                logger.debug(f"greylist refresh on launch skipped: {e}")

        # Initial baseline classification (no metrics yet, but we have rug count)
        verdict = classify(
            {"curve_fill_pct": 0, "elapsed_s": 0, "unique_buyers": 0,
             "sol_inflow": 0, "creator_rugs": creator_rugs, "social_score": 0},
            self.rules.model_dump(),
        )
        launch.classifier_action = verdict["action"]
        launch.classifier_risk = verdict["risk"]
        launch.classifier_reasons = verdict["reasons"]

        doc = launch.model_dump()
        doc["detected_at"] = doc["detected_at"].isoformat()
        # Surface a few creator stats inline on the launch doc
        doc["creator_tokens_created"] = (creator_doc or {}).get("tokens_created", 1)
        doc["creator_tokens_failed"] = (creator_doc or {}).get("tokens_failed", 0)
        doc["creator_tokens_graduated"] = (creator_doc or {}).get("tokens_graduated", 0)
        await self.db.launches.insert_one({**doc, "_id": launch.id})
        self.recent_launches.insert(0, doc)
        # Aging — keep the 50 most recent unpinned launches PLUS all pinned
        # ones (Phase 2.9). Pinned launches survive aging until manually
        # unpinned via /api/launch/{id}/unpin. Cap pinned at 200 as a safety
        # net so a stuck unpin can't grow the list unbounded.
        unpinned: list[dict] = []
        pinned: list[dict] = []
        for r in self.recent_launches:
            if r.get("pinned"):
                pinned.append(r)
            else:
                unpinned.append(r)
        self.recent_launches = pinned[:200] + unpinned[:50]

        # WS push
        await hub.broadcast("launch", doc)

        # Start in-memory metric tracker for this mint
        self.tracking[launch.mint] = {
            "launch_id": launch.id,
            "creator": launch.creator,
            "start": time.time(),
            "buyers": set(),
            "buy_events": deque(maxlen=500),  # (ts, sol_lamports, user)
            "sol_inflow_lamports": 0,
            "buy_count": 0,
            "curve_fill_pct": 0.0,
            "social_score": 0,
            "social_sources": {},
            "last_persist": 0.0,
            "name": launch.name,
            "symbol": launch.symbol,
            "creator_rugs": creator_rugs,
            "first_seen_price_sol": 0.0,  # filled on first TradeEvent / curve fetch
            "last_price_sol": 0.0,
            # Throttled price-time samples (~1Hz) for the entry-velocity gate
            # 1h of history capacity at ~30s spacing (after the first 60s of
            # dense 1Hz sampling). Used for rolling growth-% and entry-velocity.
            "price_samples": deque(maxlen=120),  # adaptive: 1Hz first 60s, then 1/30s
            "last_price_sample_ts": 0.0,
            "scanner_eligible": True,
            "scanner_last_attempt": 0.0,
            # Social proof — populated asynchronously by _fetch_socials below
            # (Pump.fun indexes the mint a few seconds after creation)
            "reply_count": 0,
            "twitter": "",
            "telegram": "",
            "website": "",
        }
        # LRU-style cap: drop oldest if over the limit
        if len(self.tracking) > MAX_TRACKED_MINTS:
            oldest = min(self.tracking.items(), key=lambda kv: kv[1]["start"])[0]
            self.tracking.pop(oldest, None)

        asyncio.create_task(self._compute_social(launch.mint))
        asyncio.create_task(self._fetch_pumpfun_socials(launch.mint))
        asyncio.create_task(self._assess_and_enter(launch, creator_rugs))
        asyncio.create_task(self._tracker_cleanup(launch.mint))
        # Greylist Sniper — fires for greylisted creators regardless of
        # momentum (the WHOLE point of the greylist is sniping these
        # predictable-pattern creators on their curve, NOT waiting for
        # them to pump organically — which they rarely do).
        asyncio.create_task(self._attempt_greylist_snipe(launch, creator_doc))

    async def on_trade(self, trade_data: dict):
        """A buy/sell event was observed on Pump.fun."""
        mint = trade_data["mint"]
        bucket = self.tracking.get(mint)
        # Track price via virtual reserves first (so active-trade fast path can use it)
        vsr = trade_data.get("virtual_sol_reserves", 0)
        vtr = trade_data.get("virtual_token_reserves", 0)
        cur_price = None
        if vsr and vtr:
            cur_price = vsr / vtr / LAMPORTS_PER_SOL

        # FAST EXIT PATH: if we hold this mint, check TP/SL on every trade event
        # (sub-100ms reaction instead of 800ms poll loop — eliminates SL overshoot)
        if cur_price and mint in self.active_trades:
            asyncio.create_task(self._check_fast_exit(mint, cur_price))

        if not bucket:
            return
        now = time.time()
        if trade_data.get("is_buy"):
            bucket["buyers"].add(trade_data["user"])
            bucket["sol_inflow_lamports"] += int(trade_data.get("sol_amount", 0))
            bucket["buy_count"] += 1
            bucket["buy_events"].append((now, int(trade_data.get("sol_amount", 0)), trade_data["user"]))
        if cur_price:
            if bucket["first_seen_price_sol"] <= 0:
                bucket["first_seen_price_sol"] = cur_price
            bucket["last_price_sol"] = cur_price
            bucket["last_vsr_lamports"] = vsr  # legacy: virtual SOL reserves
            # Pump.fun bonding curves have a 30 SOL virtual offset baked in,
            # so real_sol = virtual_sol - 30. Mempool buy events only fire
            # for non-graduated tokens, so this subtraction is always valid
            # here (graduated tokens get `last_real_sol_lamports` set directly
            # in discovery.py).
            real_sol = max(0, vsr - 30_000_000_000)
            bucket["last_real_sol_lamports"] = real_sol
            bucket["curve_fill_pct"] = min(
                100.0, max(0.0, (vsr - 30_000_000_000) / (85_000_000_000) * 100)
            )
            # Adaptive price sampling — sample at 1Hz for the first 60s of
            # tracking (entry-velocity gate needs dense data) then drop to one
            # sample every 30s so the 120-slot deque covers ~1 hour of history
            # for the rolling growth-pct computation.
            age = now - bucket.get("start", now)
            sample_interval = 1.0 if age < 60 else 30.0
            if now - bucket.get("last_price_sample_ts", 0) >= sample_interval:
                bucket["last_price_sample_ts"] = now
                samples = bucket.get("price_samples")
                if samples is not None:
                    samples.append((now, cur_price))

        if now - bucket.get("last_persist", 0) >= PERSIST_INTERVAL_S:
            bucket["last_persist"] = now
            await self._persist_metrics(mint)

    def _exit_param(self, slot: dict, key: str, default: float) -> float:
        """Read an exit-side parameter for a position, preferring the trade's
        per-position greylist override (when present) over BotConfig.
        Used so an open greylist-tier position keeps the override it was
        opened with even if BotConfig changes mid-flight, AND so two
        concurrent positions on different tiers each get their own values.
        `key` is one of: tp_pct, sl_pct, trail_pct, trail_arm_pct."""
        try:
            ov = (slot or {}).get("greylist_overrides") or {}
            if key in ov and ov[key] is not None:
                return float(ov[key])
        except Exception:
            pass
        return float(default)

    def _is_snipe(self, slot: dict) -> bool:
        """True iff this position was entered via the Greylist Sniper path.
        Snipes follow a fundamentally different exit philosophy from
        momentum trades (pattern-based exits, no entry-loss SL, no max-hold).
        See `_check_snipe_pattern_exit` for the actual exit logic."""
        trade = (slot or {}).get("trade") or {}
        return (trade.get("classifier_action") == "greylist_snipe"
                and bool(self.config.greylist_snipe_pattern_exits))

    async def _compute_creator_snipe_ctx_fallback(self, creator: str) -> dict | None:
        """Fallback for snipe_pattern_ctx when the creator doc lacks the
        scorer-populated `expected_peak_mc_usd` / `expected_rug_window_pct`
        aggregates. Reads the creator's failed launches directly and
        computes medians for:

          - `expected_peak_mc_usd`  — median `final_peak_mc_usd` across
            failed launches (excluding failed_instant since those have
            tiny peaks and would drag the median artificially low)
          - `expected_rug_curve_pct` — median `curve_fill_pct` at the
            point each launch died (excludes launches that never started
            filling, i.e. `curve_fill_pct < 1.0`)

        Bounded scan (max 60 docs, projection-only) → ~1-5ms per snipe.
        Returns None when the creator has fewer than 2 usable failed
        launches.
        """
        if not creator:
            return None
        import statistics
        try:
            cursor = self.db.launches.find(
                {"creator": creator, "outcome": "failed"},
                {"_id": 0, "final_peak_mc_usd": 1, "curve_fill_pct": 1,
                 "fail_class": 1},
            ).limit(60)
        except Exception:
            return None
        peaks: list[float] = []
        rugs: list[float] = []
        async for d in cursor:
            mc = d.get("final_peak_mc_usd")
            if mc and float(mc) > 0 and d.get("fail_class") != "failed_instant":
                peaks.append(float(mc))
            cv = d.get("curve_fill_pct")
            if cv is not None and float(cv) >= 1.0:
                rugs.append(float(cv))
        out: dict = {}
        if len(peaks) >= 2:
            out["expected_peak_mc_usd"] = round(statistics.median(peaks), 0)
        if len(rugs) >= 2:
            med_rug = statistics.median(rugs)
            # Refuse to return a rug-curve target that's lower than the
            # buffer used by the exit gate (+5pp safety cushion). Otherwise
            # the gate fires the moment the launch starts filling — that
            # was the 2026-05-26 instant-exit bug. Creators whose tokens
            # all die at <15% curve fill are effectively untradeable_rug
            # and shouldn't have a curve-based exit at all.
            buffer_pp = float(self.config.greylist_snipe_curve_buffer_pct or 0)
            min_floor = buffer_pp + 10.0  # 10pp room above the buffer
            if med_rug >= min_floor:
                out["expected_rug_curve_pct"] = round(med_rug, 1)
        return out or None

    def _snipe_velocity_signals(self, bucket: dict) -> dict | None:
        """Compute SOL inflow rate and new-holder rate over two rolling
        windows: a RECENT window (`velocity_window_s`) and a BASELINE
        window (`velocity_baseline_s`, ending right before the recent
        window starts).

        Returns dict with:
          - `recent_sol_per_s`, `baseline_sol_per_s` (SOL inflow rate)
          - `recent_holders_per_s`, `baseline_holders_per_s` (unique new buyers / sec)
          - `recent_buys`, `baseline_buys` (raw counts — for cold-start protection)

        New-holder rate is computed against buyers UNIQUE to that window —
        i.e. a buyer who already appeared in the baseline doesn't count as
        new in the recent window. This is the "fresh FOMO" signal.

        Returns `None` if the tracking bucket lacks the `buy_events` deque.
        """
        events = bucket.get("buy_events") if bucket else None
        if not events:
            return None
        cfg = self.config
        win = max(1.0, float(cfg.greylist_snipe_velocity_window_s or 15))
        baseline_s = max(1.0, float(cfg.greylist_snipe_velocity_baseline_s or 60))
        now = time.time()
        recent_lo = now - win
        baseline_lo = recent_lo - baseline_s
        recent_lamports = 0
        baseline_lamports = 0
        recent_buyers: set = set()
        baseline_buyers: set = set()
        recent_buys = 0
        baseline_buys = 0
        # Single pass — events are append-only ordered by ts asc.
        for ts, sol_lamp, user in events:
            if ts >= recent_lo:
                recent_lamports += int(sol_lamp or 0)
                recent_buys += 1
                if user:
                    recent_buyers.add(user)
            elif ts >= baseline_lo:
                baseline_lamports += int(sol_lamp or 0)
                baseline_buys += 1
                if user:
                    baseline_buyers.add(user)
        # New holders in recent = buyers NOT seen in baseline
        new_recent_holders = len(recent_buyers - baseline_buyers)
        baseline_unique_holders = len(baseline_buyers)
        return {
            "recent_sol_per_s": (recent_lamports / LAMPORTS_PER_SOL) / win,
            "baseline_sol_per_s": (baseline_lamports / LAMPORTS_PER_SOL) / baseline_s,
            "recent_holders_per_s": new_recent_holders / win,
            "baseline_holders_per_s": baseline_unique_holders / baseline_s,
            "recent_buys": recent_buys,
            "baseline_buys": baseline_buys,
        }

    def _check_snipe_pattern_exit(self, slot: dict, cur_price_sol: float) -> tuple[bool, str]:
        """Pattern-based exit decision for greylist snipes. Returns
        `(should_exit, reason)`.

        Per user spec for greylist plays:
          - NO entry-loss SL
          - NO max-hold timeout
          - NO momentum trailing stop
          - YES exit when curve fill approaches creator's typical rug point
          - YES exit when current MC approaches creator's typical peak
          - YES rip-cord on catastrophic drawdown from OBSERVED peak (rug
            already happened; ride it out is futile)
          - YES pattern-suggested TP (lock profit on parabolic moves)

        All thresholds are configurable. Conservative defaults: exit at 85%
        of expected peak MC, exit when curve is within 5pp of expected rug
        curve %, rip-cord at 60% drawdown from peak observed (sustained 8s).
        """
        ctx = (slot or {}).get("snipe_pattern_ctx")
        if ctx is None:
            # No snipe context at all — not a sniper trade, nothing to do.
            return False, ""
        cfg = self.config

        # 0a. PROFIT RIPCORD — the highest-priority exit. Fires the moment the
        # position is up >= `greylist_snipe_profit_ripcord_pct` from entry,
        # regardless of pattern. Paper data: snipes that hit +29-33% TP
        # then gave it all back to a partial-trail runner. A FULL-EXIT
        # ripcord at +30% (default) realizes those wins. Set to 0 to disable.
        trade = slot["trade"]
        entry_p = trade.get("entry_price_sol") or 0
        pct_change = 0.0
        if entry_p > 0:
            pct_change = (cur_price_sol - entry_p) / entry_p * 100
            profit_ripcord = float(cfg.greylist_snipe_profit_ripcord_pct or 0)
            if profit_ripcord > 0 and pct_change >= profit_ripcord:
                return True, (f"snipe profit-ripcord (+{pct_change:.1f}% ≥ "
                              f"{profit_ripcord:.0f}% — locking profit before rug)")

        # 0b. STALE-SNIPE TIME FAIL-SAFE — paper data showed 10-30 min holds
        # drifting to -20-45%. A snipe that hasn't popped within ~90s is
        # almost always going to die. Exit if held > stale_seconds AND
        # the position has not climbed at least stale_min_profit_pct above
        # entry. Set stale_seconds=0 to disable.
        stale_s = int(cfg.greylist_snipe_stale_seconds or 0)
        if stale_s > 0 and entry_p > 0:
            entry_ts = trade.get("_entry_ts_mono") or slot.get("_entry_ts_mono")
            if entry_ts is None:
                # Lazily stamp on first call — _enter_impl doesn't currently
                # set this and we don't want to backfill every call site.
                entry_ts = time.time()
                slot["_entry_ts_mono"] = entry_ts
            age = time.time() - entry_ts
            stale_min = float(cfg.greylist_snipe_stale_min_profit_pct or 0)
            if age >= stale_s and pct_change < stale_min:
                return True, (f"snipe stale-exit (held {age:.0f}s ≥ {stale_s}s "
                              f"@ {pct_change:+.1f}% < required +{stale_min:.0f}%)")

        # 1. Pattern-suggested TP. If the creator has a known pattern with a
        # suggested exit %, lock in profit there. Falls through to other
        # gates if not hit.
        if entry_p > 0:
            pct_change = (cur_price_sol - entry_p) / entry_p * 100
            pattern_tp = trade.get("greylist_pattern_suggested_tp_pct")
            if pattern_tp is not None and pct_change >= float(pattern_tp):
                return True, f"snipe pattern-TP hit (+{pct_change:.1f}% ≥ {pattern_tp:.1f}%)"

        # 1b. Velocity-decay exits. The rug is preceded by SOL inflow rate
        # collapsing and/or new-holder rate collapsing. Compare the LAST
        # `velocity_window_s` of trade activity against the PRIOR
        # `velocity_baseline_s` (rolling baseline). If the recent rate has
        # dropped below `(1 - drop_pct/100)` of the baseline rate AND the
        # baseline has enough samples, exit — the pump is exhausting.
        bucket = self.tracking.get(trade["mint"], {})
        if cfg.greylist_snipe_velocity_exits_enabled:
            sig = self._snipe_velocity_signals(bucket)
            if sig is not None:
                # Only check decay AFTER baseline window has had enough buys
                # — protects against cold-start false positives.
                if sig["baseline_buys"] >= int(cfg.greylist_snipe_velocity_min_buys or 0):
                    sol_drop_floor = max(0.0, 1.0 - float(cfg.greylist_snipe_sol_vel_drop_pct or 0) / 100.0)
                    hol_drop_floor = max(0.0, 1.0 - float(cfg.greylist_snipe_holder_vel_drop_pct or 0) / 100.0)
                    if (sig["baseline_sol_per_s"] > 0
                            and sig["recent_sol_per_s"] / sig["baseline_sol_per_s"] <= sol_drop_floor):
                        return True, (
                            f"snipe SOL-velocity decay "
                            f"({sig['recent_sol_per_s']:.3f} SOL/s recent vs "
                            f"{sig['baseline_sol_per_s']:.3f} baseline = "
                            f"-{(1 - sig['recent_sol_per_s']/sig['baseline_sol_per_s'])*100:.0f}%)"
                        )
                    if (sig["baseline_holders_per_s"] > 0
                            and sig["recent_holders_per_s"] / sig["baseline_holders_per_s"] <= hol_drop_floor):
                        return True, (
                            f"snipe new-holder velocity decay "
                            f"({sig['recent_holders_per_s']:.2f}/s recent vs "
                            f"{sig['baseline_holders_per_s']:.2f}/s baseline = "
                            f"-{(1 - sig['recent_holders_per_s']/sig['baseline_holders_per_s'])*100:.0f}%)"
                        )

        # 2. Curve fill proximity to typical rug curve %. The creator's
        # `expected_rug_curve_pct` is the median curve fill at which their
        # past launches rugged. We exit when we're within `curve_buffer_pct`
        # of that — gives us a head-start before the dump.
        #
        # IMPORTANT: if rug_curve is below the buffer (e.g. creator rugs at
        # 3% curve fill, buffer is 5pp), `max(0, rug-buffer) = 0` would
        # trigger this gate IMMEDIATELY on any non-zero curve fill —
        # producing the instant-exit bug observed 2026-05-26. The fix is
        # to refuse to fire the gate at all when rug_curve <= buffer + 5pp
        # cushion; those creators are essentially untradeable_rug and have
        # no entry-to-exit window.
        curve_pct = bucket.get("curve_fill_pct") or 0.0
        rug_curve = ctx.get("expected_rug_curve_pct")
        if rug_curve is not None and curve_pct > 0:
            buffer_pp = float(cfg.greylist_snipe_curve_buffer_pct or 0)
            if float(rug_curve) > buffer_pp + 5.0:
                trigger_at = float(rug_curve) - buffer_pp
                if curve_pct >= trigger_at:
                    return True, (f"snipe curve-fill exit ({curve_pct:.1f}% ≥ "
                                  f"rug curve {rug_curve:.1f}% − {buffer_pp:.1f}pp buffer)")

        # 3. Peak MC proximity. If the creator's typical peak MC is known
        # and the current MC is within `peak_mc_proximity_pct` of it, exit
        # before the predicted rug. Uses LIVE MC from the tracking bucket.
        cur_mc = float(bucket.get("usd_market_cap") or 0.0)
        exp_peak = ctx.get("expected_peak_mc_usd")
        if exp_peak and exp_peak > 0 and cur_mc > 0:
            proximity_pct = float(cfg.greylist_snipe_peak_mc_proximity_pct or 85.0)
            trigger_mc = float(exp_peak) * (proximity_pct / 100.0)
            if cur_mc >= trigger_mc:
                return True, (f"snipe peak-MC exit (${cur_mc:,.0f} ≥ "
                              f"{proximity_pct:.0f}% of expected ${exp_peak:,.0f})")

        # 4. Rip-cord — catastrophic drawdown FROM OBSERVED PEAK (NOT from
        # entry). If the price has been below `1 - ripcord_drawdown_pct`
        # of the observed peak for `ripcord_grace_seconds`, the rug already
        # happened and there's nothing left to salvage. Bail.
        peak = slot.get("peak_price_sol", entry_p)
        if cur_price_sol > peak:
            peak = cur_price_sol
            slot["peak_price_sol"] = peak
        if peak > 0:
            drawdown_pct = (peak - cur_price_sol) / peak * 100
            ripcord_thresh = float(cfg.greylist_snipe_ripcord_drawdown_pct or 60.0)
            if drawdown_pct >= ripcord_thresh:
                # First breach starts the grace timer; subsequent breaches
                # check elapsed.
                first = slot.get("_snipe_ripcord_start")
                now = time.time()
                if first is None:
                    slot["_snipe_ripcord_start"] = now
                else:
                    grace = float(cfg.greylist_snipe_ripcord_grace_seconds or 8)
                    if now - first >= grace:
                        return True, (f"snipe rip-cord ({drawdown_pct:.1f}% drawdown "
                                      f"from peak sustained {now - first:.0f}s)")
            else:
                # Recovered above threshold — clear the timer.
                slot.pop("_snipe_ripcord_start", None)

        return False, ""

    async def _check_fast_exit(self, mint: str, cur_price_sol: float):
        """Real-time TP/SL/trailing-stop check fired by on_trade.
        Idempotent: only acts once per mint."""
        slot = self.active_trades.get(mint)
        if not slot:
            return
        # Per-position exit mutex — prevents a partial-TP from running
        # concurrently with a full-exit when the monitor and fast-exit
        # paths both detect an exit condition on the same tick. Without
        # this, both built sell IXs for the same trade, the partial
        # drained the balance, and the full exit reverted with
        # Custom:6023 (NotEnoughTokensToSell).
        if slot.get("exit_in_progress"):
            return
        trade_doc = slot["trade"]
        entry_p = trade_doc.get("entry_price_sol", 0)
        if entry_p <= 0:
            return
        # Update peak for trailing stop
        peak = slot.get("peak_price_sol", entry_p)
        if cur_price_sol > peak:
            peak = cur_price_sol
            slot["peak_price_sol"] = peak
        # Cache last seen price for the UI's live-PnL panel.
        slot["_last_price_sol"] = cur_price_sol

        # Greylist snipes use pattern-based exits, NOT entry-loss SL/TP/trail
        # ladder. Short-circuit the whole standard exit block here.
        if self._is_snipe(slot):
            should_exit, reason = self._check_snipe_pattern_exit(slot, cur_price_sol)
            if should_exit:
                slot["exit_in_progress"] = True
                try:
                    await self._exit(mint, reason=reason + " [fast]")
                    return
                finally:
                    slot["exit_in_progress"] = False
            return

        pct_change = (cur_price_sol - entry_p) / entry_p * 100

        # Per-position thresholds — greylist overrides win when present.
        tp_pct = self._exit_param(slot, "tp_pct", self.config.take_profit_pct)
        sl_pct = self._exit_param(slot, "sl_pct", self.config.stop_loss_pct)
        trail_pct_cfg = self._exit_param(slot, "trail_pct", self.config.trailing_stop_pct)
        trail_arm_pct_cfg = self._exit_param(slot, "trail_arm_pct", self.config.trailing_arm_pct)

        # Take profit — either full exit or partial-then-tighten-trailing.
        # NOTE: after a successful partial, we skip the TP check entirely — the
        # runner is governed by the tightened trailing stop only.
        #
        # 2026-02-08: wrapped in persistence check. Paper data showed TP
        # firing on single-tick wicks (+15-23% message vs 0-5% raw move
        # entry→exit) — one outsized buy event spikes curve, TP fires,
        # then price reverts before the sell settles. Persistence kills
        # the false positives without delaying real TP moves more than ~800ms.
        tp_breached = pct_change >= tp_pct
        tp_should_fire = False
        if tp_breached and not slot.get("partial_done"):
            if self.config.intelligent_exit_v2:
                tp_should_fire = self._check_breach_persistence(
                    slot, kind="tp", breached=tp_breached,
                    persistence_ms=self.config.tp_persistence_ms,
                    min_samples=self.config.tp_persistence_min_samples,
                )
            else:
                tp_should_fire = True
        elif not tp_breached:
            # Recovery — clear the TP breach state so next breach restarts the clock.
            self._check_breach_persistence(
                slot, kind="tp", breached=False,
                persistence_ms=self.config.tp_persistence_ms,
                min_samples=self.config.tp_persistence_min_samples,
            )
        if tp_should_fire:
            slot["exit_in_progress"] = True
            try:
                ptp = self.config.partial_tp_pct
                if 0 < ptp < 100:
                    # Reserve the partial flag synchronously to prevent concurrent
                    # fast-exit invocations from racing into the same partial.
                    slot["partial_done"] = True
                    did = await self._partial_exit(
                        mint, ptp / 100.0,
                        reason=f"partial-tp ({ptp:.0f}%) at +{pct_change:.1f}% [fast]",
                    )
                    if did:
                        slot["peak_price_sol"] = cur_price_sol
                        return
                    # Partial failed — clear the reservation and fall through to full exit
                    slot["partial_done"] = False
                await self._exit(mint, reason=f"take-profit hit (+{pct_change:.1f}%) [fast]")
                return
            finally:
                slot["exit_in_progress"] = False
        # Hard stop loss FIRST — protects against rugs that would otherwise
        # be misattributed to trailing-stop with a tiny peak.
        # v2: require sustained breach (kills millisecond dips from MEV/bad RPC quotes).
        # v2.1: severity override — if price has fallen ≥ stop_loss + 5% beyond the
        # gate, fire immediately. On thin pump.fun pools price can fall another
        # 30% during the 1.2s persistence window, turning a -10% SL into a -40%
        # actual exit. The override caps that bleed.
        if self.config.intelligent_exit_v2:
            sl_breached = pct_change <= -sl_pct
            sl_severity = -pct_change - sl_pct  # positive when worse than SL
            if self._check_breach_persistence(
                slot, kind="sl", breached=sl_breached,
                persistence_ms=self.config.sl_persistence_ms,
                min_samples=self.config.sl_persistence_min_samples,
                severity_pct=sl_severity,
                severity_threshold_pct=5.0,
            ):
                slot["exit_in_progress"] = True
                try:
                    await self._exit(mint, reason=f"stop-loss hit ({pct_change:.1f}%) [fast]")
                    return
                finally:
                    slot["exit_in_progress"] = False
        elif pct_change <= -sl_pct:
            slot["exit_in_progress"] = True
            try:
                await self._exit(mint, reason=f"stop-loss hit ({pct_change:.1f}%) [fast]")
                return
            finally:
                slot["exit_in_progress"] = False
        # Trailing stop — only ARM once the trade has shown a real peak (above
        # `trailing_arm_pct`). Below that, a +0.5% peak followed by a -10%
        # drop would otherwise fire trailing instead of letting the SL handle
        # it. After partial TP, use the tighter trail to lock in runner gains.
        trail_pct = (
            self.config.partial_tp_trail_tighten_pct
            if slot.get("partial_done") and self.config.partial_tp_trail_tighten_pct > 0
            else trail_pct_cfg
        )
        # peak_pct in % terms relative to entry
        peak_pct = (peak - entry_p) / entry_p * 100 if entry_p > 0 else 0.0
        arm_pct = trail_arm_pct_cfg if not slot.get("partial_done") else 0.0
        if trail_pct > 0 and peak > entry_p and peak_pct >= arm_pct:
            trail_drop = (peak - cur_price_sol) / peak * 100
            ts_breached = trail_drop >= trail_pct
            # v2: require sustained breach for trailing stop too
            should_fire = False
            if self.config.intelligent_exit_v2:
                should_fire = self._check_breach_persistence(
                    slot, kind="ts", breached=ts_breached,
                    persistence_ms=self.config.ts_persistence_ms,
                    min_samples=self.config.ts_persistence_min_samples,
                )
            else:
                should_fire = ts_breached
            if should_fire:
                slot["exit_in_progress"] = True
                try:
                    await self._exit(
                        mint,
                        reason=f"trailing-stop hit (peak +{peak_pct:.1f}%, now +{pct_change:.1f}%) [fast]",
                    )
                finally:
                    slot["exit_in_progress"] = False
                return
        else:
            # Trail not armed yet → clear any pending TS breach state
            if slot.get("ts_breached_since") is not None:
                slot["ts_breached_since"] = None
                slot["ts_breached_samples"] = 0

    async def _persist_metrics(self, mint: str):
        b = self.tracking.get(mint)
        if not b:
            return
        # Track peak MC across the lifetime of this launch. This is the
        # signal Greylist Phase 1 needs: "what's the highest MC this creator's
        # past mints reached before failing?" — averaged per creator.
        cur_mc = float(b.get("usd_market_cap") or 0.0)
        prev_peak = float(b.get("peak_mc_usd") or 0.0)
        if cur_mc > prev_peak:
            b["peak_mc_usd"] = cur_mc
            # Stamp peak time so we can later compute profit_window_seconds
            # (peak → rug delta) — Bing reference §3.B.
            b["peak_mc_usd_at"] = now_utc().isoformat()
        update = {
            "unique_buyers": len(b["buyers"]),
            "sol_inflow": b["sol_inflow_lamports"] / LAMPORTS_PER_SOL,
            "buy_count": b["buy_count"],
            "curve_fill_pct": b["curve_fill_pct"],
            "social_score": b["social_score"],
            "social_sources": b["social_sources"],
            "peak_mc_usd": b.get("peak_mc_usd", 0.0),
        }
        if b.get("peak_mc_usd_at"):
            update["peak_mc_usd_at"] = b["peak_mc_usd_at"]
        await self.db.launches.update_one({"_id": b["launch_id"]}, {"$set": update})
        for r in self.recent_launches:
            if r.get("id") == b["launch_id"]:
                r.update(update)
                break
        # WS push — THROTTLED to once per 5s per mint. Without this the
        # frontend gets ~75 launch_update events/sec when 150+ mints are
        # tracked (every persist tick fires one), which is enough to OOM
        # mobile Chrome on a battery-constrained device. The DB write
        # above still happens at the underlying 2s cadence so the scanner
        # sees fresh metrics — only the wire broadcast is rate-limited.
        now_b = time.time()
        last_bcast = b.get("last_ws_broadcast", 0)
        if now_b - last_bcast >= 5.0:
            b["last_ws_broadcast"] = now_b
            await hub.broadcast("launch_update", {"id": b["launch_id"], "mint": mint, **update})

    async def _compute_social(self, mint: str):
        b = self.tracking.get(mint)
        if not b:
            return
        try:
            res = await score_term(b.get("name"), b.get("symbol"))
            b["social_score"] = int(res.get("score", 0))
            b["social_sources"] = res.get("sources", {})
            await self._persist_metrics(mint)
        except Exception as e:
            logger.debug(f"social score failed for {mint}: {e}")

    async def _fetch_pumpfun_socials(self, mint: str):
        """Pull on-chain social proof fields (reply_count, twitter, telegram,
        website) from Pump.fun's `/coins/{mint}` endpoint. Used by the entry
        gate. Re-tries a few times because the mint is only indexed by Pump's
        API after the first trade event hits it (usually 2-10s post-creation).
        """
        import httpx
        b = self.tracking.get(mint)
        if not b:
            return
        url = f"https://frontend-api-v3.pump.fun/coins/{mint}"
        # Up to 4 attempts with backoff (2s, 6s, 14s, 30s — covers ~50s window)
        for delay in (2.0, 6.0, 14.0, 30.0):
            await asyncio.sleep(delay)
            if mint not in self.tracking:
                return  # bucket evicted
            try:
                async with httpx.AsyncClient(timeout=8.0) as client:
                    r = await client.get(url, headers={"accept": "application/json"})
                    if r.status_code != 200:
                        continue
                    c = r.json() or {}
                    b["reply_count"] = int(c.get("reply_count") or 0)
                    b["twitter"] = (c.get("twitter") or "").strip()
                    b["telegram"] = (c.get("telegram") or "").strip()
                    b["website"] = (c.get("website") or "").strip()
                    # We got a real response — exit early. Any later updates
                    # will be picked up by the discovery refresh loop once the
                    # mint becomes discoverable.
                    return
            except Exception as e:
                logger.debug(f"social fetch retry for {mint}: {e}")

    async def _tracker_cleanup(self, mint: str):
        await asyncio.sleep(TRACK_DURATION_S)
        # Final metric flush for the heavy-tracking window
        await self._persist_metrics(mint)
        # Determine launch outcome at the 60s mark — ONLY mark "graduated".
        # We deliberately do NOT mark instant-rug failures here: per the
        # rug-patterns spec (memory/RUG_PATTERNS.md), "Dead in 60s" launches
        # are the USELESS pattern we don't want to greylist. The fizzled-out
        # tokens we DO want to capture take days to surface, so we delegate
        # failure detection to the background sweep below.
        b = self.tracking.get(mint)
        if b:
            try:
                state = await pumpfun.fetch_bonding_curve_state(mint)
                if state and state["complete"]:
                    await mark_outcome(self.db, b["creator"], "graduated")
                    # Derive per-launch behavioral signatures so the
                    # creator's repeatability aggregator sees consistent
                    # data across both failed and graduated launches.
                    from launch_signatures import derive_signatures, accel_signature_v2
                    grad_outcome_at = now_utc().isoformat()
                    peak_at = b.get("peak_mc_usd_at")
                    launch_for_sig = {
                        "sol_inflow": b.get("sol_inflow_lamports", 0) / LAMPORTS_PER_SOL,
                        "buy_count": b.get("buy_count") or 0,
                        "unique_buyers": len(b.get("buyers") or []),
                        "detected_at": b.get("start"),
                        "outcome": "graduated",
                        "outcome_at": grad_outcome_at,
                        "peak_mc_usd_at": peak_at,
                    }
                    sig_fields = derive_signatures(launch_for_sig)
                    # Delta-based accel signature (parabolic / bot_swarm / whale_led)
                    av2 = accel_signature_v2(list(b.get("buy_events") or []))
                    if av2:
                        sig_fields["accel_signature_v2"] = av2
                    update_doc = {
                        "outcome": "graduated",
                        "outcome_at": grad_outcome_at,
                        "final_peak_mc_usd": float(b.get("peak_mc_usd") or 0.0),
                        **sig_fields,
                    }
                    if peak_at:
                        update_doc["peak_mc_usd_at"] = peak_at
                    await self.db.launches.update_one(
                        {"_id": b["launch_id"]},
                        {"$set": update_doc},
                    )
                    try:
                        from creator_greylist import update_creator_score
                        await update_creator_score(
                            self.db, b.get("creator"),
                            min_fails=int(self.config.creator_greylist_min_fails),
                            max_fails=int(self.config.creator_greylist_max_fails),
                            tp_buffer=float(self.config.pattern_tp_buffer_pct),
                        )
                    except Exception as e:
                        logger.debug(f"greylist post-graduation refresh: {e}")
            except Exception as e:
                logger.debug(f"outcome check failed for {mint}: {e}")
        # Schedule final removal at cfg.scanner_window_hours (honors live config)
        async def _final_drop():
            try:
                window_h = max(1, int(self.config.scanner_window_hours))
            except Exception:
                window_h = SCANNER_TRACK_HOURS
            remaining = max(0, window_h * 3600 - TRACK_DURATION_S)
            await asyncio.sleep(remaining)
            self.tracking.pop(mint, None)
        asyncio.create_task(_final_drop())

    # ---------- Entry decision (assess only — entry handled by MomentumScanner) ----------
    async def _assess_and_enter(self, launch: Launch, creator_rugs: int = 0):
        """Runs the classifier on the early-window metrics so the Recent Launches
        feed shows a verdict, but does NOT auto-enter. All entries now flow
        through the momentum scanner (both new and seasoned bands)."""
        try:
            await asyncio.sleep(ASSESS_DELAY_S)
            b = self.tracking.get(launch.mint, {})
            metrics = {
                "elapsed_s": time.time() - b.get("start", time.time()),
                "curve_fill_pct": b.get("curve_fill_pct", 0.0),
                "unique_buyers": len(b.get("buyers", set())),
                "sol_inflow": b.get("sol_inflow_lamports", 0) / LAMPORTS_PER_SOL,
                "creator_rugs": creator_rugs,
                "social_score": b.get("social_score", 0),
            }
            verdict = classify(metrics, self.rules.model_dump())
            await self.db.launches.update_one(
                {"_id": launch.id},
                {"$set": {
                    "classifier_action": verdict["action"],
                    "classifier_risk": verdict["risk"],
                    "classifier_reasons": verdict["reasons"],
                }},
            )
            for r in self.recent_launches:
                if r.get("id") == launch.id:
                    r["classifier_action"] = verdict["action"]
                    r["classifier_risk"] = verdict["risk"]
                    r["classifier_reasons"] = verdict["reasons"]
                    break
        except Exception as e:
            logger.exception(f"assess failed for {launch.mint}: {e}")

    async def _attempt_greylist_snipe(self, launch: Launch, creator_doc: dict | None):
        """Greylist Sniper — opens a position on EVERY new launch from a
        creator that scored ≥ greylist_snipe_min_score on the greylist.
        Bypasses momentum gates inside `_enter_impl` (signaled by
        `action="greylist_snipe"`) since greylisted creators rarely pump
        organically. Still gated by all safety checks: kill switch,
        max_concurrent_positions, recent_exit cooldown, doctor pause,
        per-hour rate cap, paper-vs-live mode.

        Decision flow:
          1. Master enabled? Else return.
          2. Bot enabled + not stopping_gracefully?
          3. Creator on greylist with score ≥ min_score AND not blacklisted?
          4. Per-hour fire cap not exceeded?
          5. Settle delay (let tracking bucket populate liquidity/socials).
          6. Call `_enter(launch, risk_score=0, action="greylist_snipe")`.

        Safety: a wave of N greylist launches in one minute can NOT all
        fire — the rate cap clamps to `greylist_snipe_max_per_hour`. The
        existing position cap in `_enter` provides a second safety net.
        """
        try:
            if not self.config.greylist_snipe_enabled:
                return
            if not self.config.creator_greylist_enabled:
                return
            if not self.config.enabled or self.stopping_gracefully:
                return
            if not launch.creator:
                return
            # Per-hour fire cap (rolling 1h window).
            now = time.time()
            self._greylist_snipe_fires = [
                t for t in self._greylist_snipe_fires if now - t < 3600
            ]
            cap = max(0, int(self.config.greylist_snipe_max_per_hour or 0))
            if cap > 0 and len(self._greylist_snipe_fires) >= cap:
                logger.info(
                    f"greylist_snipe: rate cap {cap}/hr hit, "
                    f"skipping {launch.mint[:8]}…"
                )
                return
            # Score gate. We use the LIVE (decayed) score, not the raw
            # persisted value — a creator's predictability fades if they
            # haven't launched in a while. Research mode lowers the bar to
            # `greylist_snipe_research_min_score` for blacklisted-as-noisy creators.
            # creator_doc passed in is from `record_new_launch` which uses
            # `creators.find_one_and_update(..., return_document=AFTER)`,
            # so it has the LATEST tokens_failed but might NOT have the
            # greylist_score (we refresh it in on_launch after this returns).
            # Re-read to be safe.
            gc = await self.db.creators.find_one(
                {"_id": launch.creator},
                {"_id": 0, "greylist_score": 1, "greylist_score_updated_at": 1,
                 "greylist_blacklisted": 1, "greylist_out_of_band": 1,
                 "greylist_pattern": 1},
            )
            if not gc:
                return
            is_research = False
            min_score = float(self.config.greylist_snipe_min_score or 0)
            if gc.get("greylist_blacklisted") or gc.get("greylist_out_of_band"):
                # Research-mode escape hatch — when ON, the sniper ALSO
                # fires on `unpredictable_rug` creators (currently
                # blacklisted). Other blacklist reasons (untradeable_rug
                # / out_of_band) stay blocked because they're harder evidence.
                pat = gc.get("greylist_pattern")
                if (self.config.greylist_snipe_research_mode
                        and pat == "unpredictable_rug"
                        and not gc.get("greylist_out_of_band")):
                    is_research = True
                    min_score = float(self.config.greylist_snipe_research_min_score or 35.0)
                else:
                    return
            from creator_greylist import apply_decay
            eff = apply_decay(gc.get("greylist_score"),
                              gc.get("greylist_score_updated_at"))
            if eff < min_score:
                return
            # Pattern gate — paper data showed 45/45 snipes fired on
            # `unknown` or null patterns with 4/45 wins. The "predictable
            # curve" thesis only holds when the creator HAS a classified
            # pattern. Research-mode bypasses this (it deliberately targets
            # the noisy bucket).
            pat = gc.get("greylist_pattern")
            if (not is_research
                    and self.config.greylist_snipe_require_classified_pattern
                    and pat in (None, "unknown", "")):
                logger.info(
                    f"greylist_snipe: skipping {launch.mint[:8]}… — "
                    f"creator has no classified pattern (pat={pat!r}) and "
                    f"require_classified_pattern=True"
                )
                return
            # Settle wait — give the tracking bucket a moment to populate
            # liquidity / first price so the entry doesn't fire pre-curve.
            settle = max(1, int(self.config.greylist_snipe_settle_seconds or 5))
            await asyncio.sleep(settle)
            # Re-check enabled state after the sleep (user might have stopped).
            if not self.config.enabled or self.stopping_gracefully:
                return
            if launch.mint in self.active_trades or launch.mint in self._pending_entry_mints:
                return
            # Stamp the fire BEFORE _enter so concurrent triggers see the
            # accurate count (max_per_hour is a SOFT cap; we'll over-fire by
            # at most a few during contention which is acceptable).
            self._greylist_snipe_fires.append(time.time())
            logger.info(
                f"greylist_snipe: firing{' [RESEARCH]' if is_research else ''} on "
                f"{launch.symbol or launch.mint[:8]}… "
                f"creator={launch.creator[:8]}… score={eff:.0f} "
                f"pattern={gc.get('greylist_pattern')}"
            )
            await hub.broadcast("greylist_snipe_fire", {
                "mint": launch.mint, "symbol": launch.symbol,
                "creator": launch.creator, "score": round(eff, 1),
                "pattern": gc.get("greylist_pattern"),
                "is_research": is_research,
            })
            # Stash research flag for `_enter_impl` to pick up via the
            # creator_doc passed in (cheaper than a kwarg cascade).
            self._snipe_research_flags = getattr(self, "_snipe_research_flags", {})
            self._snipe_research_flags[launch.mint] = is_research
            try:
                await self._enter(launch, risk_score=0, action="greylist_snipe")
            finally:
                self._snipe_research_flags.pop(launch.mint, None)
        except Exception as e:
            logger.exception(f"greylist_snipe failed for {launch.mint}: {e}")

    # ---------- Entry / exit (live + paper) ----------
    async def _enter(self, launch: Launch, risk_score: int, action: str):
        # Smart-stop: refuse new entries while we're winding down
        if self.stopping_gracefully:
            return
        # Doctor circuit-breaker: refuse new entries while the Doctor (or the
        # user manually via the UI) has paused trading. Existing positions
        # continue to be monitored normally — only NEW entries are blocked.
        # When `doctor_advisory_only` is True the pause is observed (logged
        # + reflected in UI status) but new entries are NOT blocked — user
        # is actively supervising and decides when to stop.
        pause_until = float(getattr(self.config, "doctor_pause_until_ts", 0) or 0)
        if (pause_until and time.time() < pause_until
                and not getattr(self.config, "doctor_advisory_only", False)):
            logger.debug(
                f"doctor pause: skipping entry for {launch.mint[:8]}… "
                f"({int(pause_until - time.time())}s remaining)"
            )
            return
        # === Creator-greylist telemetry (Phase 1 — logs only) ===
        # The actual fetch + override resolution happens in `_enter_impl`
        # (where size_mult and TP/SL slots live). This stub stays here as a
        # placeholder so the gate-time codepath is obvious during review.
        pass
        # Reservation gate — serialized so concurrent scanner attempts can't
        # all race past max_concurrent_positions. Holds the lock only for the
        # gate check + reservation (microseconds), not the tx.
        async with self._entry_gate_lock:
            # Already entering this mint? (race between concurrent triggers
            # for the same mint, e.g., scanner + sniper firing in parallel)
            if launch.mint in self.active_trades or launch.mint in self._pending_entry_mints:
                return
            cap = max(1, self.config.max_concurrent_positions)
            in_flight = len(self.active_trades) + len(self._pending_entry_mints)
            if in_flight >= cap:
                return
            # SL cooldown — if this mint just exited via stop-loss, refuse to
            # re-enter for the configured window. Buying back into a freshly
            # SL-tripped mint is the textbook "buy the exit" anti-pattern.
            cd_until = self.sl_cooldown_until.get(launch.mint, 0.0)
            if cd_until and time.time() < cd_until:
                return
            # Universal post-exit cooldown — prevents re-entry on the SAME
            # mint within the cooldown window after ANY exit. Fixes the
            # "4 GSD positions in 3 min" pattern where monitors for stale
            # slots raced against the new position's exits.
            rx_until = self.recent_exit_until.get(launch.mint, 0.0)
            if rx_until and time.time() < rx_until:
                return
            # Re-entry watchlist lockout: if this mint just exited profitably
            # and is being watched for a pullback re-entry, the regular scanner
            # must NOT re-buy it from the front-running side. The re-entry
            # watcher owns this mint until the window expires (or `_attempt_reentry`
            # fires, which routes through this same gate and is allowed because
            # the watcher removes the mint from `reentry_watch` before calling).
            if launch.mint in self.reentry_watch:
                return
            # Reserve a slot — released in the finally below
            self._pending_entry_mints.add(launch.mint)

        try:
            await self._enter_impl(launch, risk_score, action)
        finally:
            self._pending_entry_mints.discard(launch.mint)

    async def _enter_impl(self, launch: Launch, risk_score: int, action: str):
        """The actual entry pipeline. Called from `_enter` after the
        position-count reservation has been taken atomically."""
        # === Creator-greylist (Phase 2: apply OR log strategy overrides) ===
        # Resolved once at entry, then carried through sizing + slot extras
        # so the exit logic (TP/SL/trail) can read overrides per-trade.
        # Mode "live" → applies overrides to size/TP/SL/trail.
        # Mode "telemetry" → logs only; standard config values used.
        greylist_ctx: dict = {"strategy": None, "score": None, "overrides": {},
                              "pattern": None, "pattern_tp_pct": None,
                              "expected_peak_mc_usd": None,
                              "expected_peak_mc_stddev": None,
                              "expected_rug_curve_pct": None}
        try:
            if self.config.creator_greylist_enabled and launch.creator:
                gc = await self.db.creators.find_one(
                    {"_id": launch.creator},
                    {"_id": 0, "greylist_score": 1,
                     "greylist_score_updated_at": 1,
                     "expected_rug_window_pct": 1,
                     "expected_peak_mc_usd": 1,
                     "greylist_n_failed": 1,
                     "greylist_pattern": 1,
                     "greylist_pattern_suggested_exit": 1,
                     "greylist_blacklisted": 1},
                )
                if gc and gc.get("greylist_score") and not gc.get("greylist_blacklisted"):
                    from creator_greylist import (
                        apply_decay, recommended_strategy, strategy_overrides,
                    )
                    eff = apply_decay(gc.get("greylist_score"),
                                      gc.get("greylist_score_updated_at"))
                    strat = recommended_strategy(eff)
                    mode = (self.config.creator_greylist_mode or "telemetry").lower()
                    applied = (mode == "live") and (strat != "standard")
                    overrides = strategy_overrides(strat) if applied else {}
                    # === Pattern-aware TP override (Phase 2.7) ===
                    # For the two tightly-bounded tradeable patterns
                    # (slow_rug / predictable_dump), the classifier exports
                    # a `suggested_exit_pct=(lo, hi)` derived from the
                    # creator's own rug-window median. Use the LOWER bound
                    # as the TP — exits just BEFORE the typical rug window
                    # opens, which is the whole point of pattern-aware
                    # micro-sniping. Only applied when greylist mode is live
                    # AND the pattern is one of the precision tradeable
                    # buckets. `fake_hype_tradeable` deliberately keeps the
                    # tier override because rug timing there is governed by
                    # mempool, not curve %.
                    pattern = gc.get("greylist_pattern")
                    sug_exit = gc.get("greylist_pattern_suggested_exit")
                    pattern_tp = None
                    if (applied and pattern in {"slow_rug_tradeable",
                                                 "predictable_dump_tradeable"}
                            and isinstance(sug_exit, (list, tuple))
                            and len(sug_exit) == 2):
                        try:
                            pattern_tp = float(sug_exit[0])  # lower bound
                            # Sanity: refuse insane values (< 5 or > 60)
                            if 5.0 <= pattern_tp <= 60.0:
                                overrides = {**overrides, "tp_pct": pattern_tp}
                            else:
                                pattern_tp = None
                        except (TypeError, ValueError):
                            pattern_tp = None
                    if strat != "standard":
                        rw = gc.get("expected_rug_window_pct") or {}
                        pmc = gc.get("expected_peak_mc_usd") or {}
                        pat_info = (f" pattern={pattern} pat_tp={pattern_tp:.1f}%"
                                    if pattern_tp is not None else
                                    f" pattern={pattern or 'n/a'}")
                        logger.info(
                            f"GREYLIST {'APPLY' if applied else 'telemetry'}: "
                            f"strategy='{strat}' for {launch.mint[:8]}… "
                            f"(creator={launch.creator[:8]}…, score={eff:.0f},"
                            f"{pat_info}, expected_peak_MC=$"
                            f"{int(pmc.get('mean_peak_mc_usd', 0)):,} "
                            f"(±${int(pmc.get('stddev_peak_mc_usd', 0)):,}, "
                            f"n_failed={gc.get('greylist_n_failed', 0)}), "
                            f"expected_rug=~{rw.get('median_rug_pct', '?')}%). "
                            f"Mode={mode}; "
                            f"{'applying overrides=' + str(overrides) if applied else 'executing standard logic.'}"
                        )
                    greylist_ctx = {
                        "strategy": strat,
                        "score": round(float(eff), 1),
                        "overrides": overrides,
                        "pattern": pattern,
                        "pattern_tp_pct": pattern_tp,
                        # Snipe-exit data — `_check_snipe_pattern_exit()` reads
                        # these to know when to bail based on the creator's
                        # OBSERVED rug pattern instead of our entry loss.
                        "expected_peak_mc_usd": pmc.get("mean_peak_mc_usd"),
                        "expected_peak_mc_stddev": pmc.get("stddev_peak_mc_usd"),
                        "expected_rug_curve_pct": rw.get("median_rug_pct"),
                    }
                    # On-demand fallback: when the scorer hasn't populated
                    # `expected_*` on the creator doc (only ~25-45% of
                    # greylisted creators have them per the 24h paper data),
                    # compute medians directly from their failed launches
                    # so the snipe pattern-exit ladder has something to fire
                    # on. Without this fallback the ladder degrades to
                    # ripcord-only — which catches at 60-90% drawdown.
                    if (action == "greylist_snipe"
                            and (greylist_ctx.get("expected_peak_mc_usd") is None
                                 or greylist_ctx.get("expected_rug_curve_pct") is None)):
                        try:
                            fb = await self._compute_creator_snipe_ctx_fallback(launch.creator)
                            if fb:
                                if greylist_ctx.get("expected_peak_mc_usd") is None:
                                    greylist_ctx["expected_peak_mc_usd"] = fb.get("expected_peak_mc_usd")
                                if greylist_ctx.get("expected_rug_curve_pct") is None:
                                    greylist_ctx["expected_rug_curve_pct"] = fb.get("expected_rug_curve_pct")
                                logger.info(
                                    f"snipe ctx fallback for {launch.creator[:8]}…: "
                                    f"peak_mc=${greylist_ctx.get('expected_peak_mc_usd') or 0:,.0f} "
                                    f"rug_curve={greylist_ctx.get('expected_rug_curve_pct')}%"
                                )
                        except Exception as e:
                            logger.debug(f"snipe ctx fallback failed: {e}")
        except Exception as e:
            logger.debug(f"greylist context skipped: {e}")

        sol_price = await get_sol_usd_price()
        # Risk-based position sizing: borderline classifications get smaller
        # trades. The bleed analysis showed many losers were borderline
        # entries with risk_score 30-60 that classifier would re-evaluate
        # as exit_early once curve filled. Halving size on these caps
        # downside without giving up the rare winner.
        if risk_score <= 30:
            size_mult = 1.0           # green light
        elif risk_score <= 60:
            size_mult = 0.6           # borderline — half-size
        else:
            size_mult = 0.3           # high-risk — third-size
        # Greylist size override (Phase 2 live mode only). Layered multiplicatively
        # on top of the risk bucket. Capped at 2× the configured max_trade_usd
        # so a hot creator can't blow the risk envelope (a 1.5× override on a
        # 1.0× risk bucket lands at 1.5×, still inside the 2× ceiling).
        gl_size_mult = float((greylist_ctx.get("overrides") or {}).get("size_mult") or 1.0)
        if gl_size_mult != 1.0:
            size_mult *= gl_size_mult
        # Research-mode snipes use a reduced size multiplier — these are
        # experimental positions on currently-blacklisted-as-noisy
        # creators, so we cap exposure while collecting the win-rate data.
        is_research_snipe = (action == "greylist_snipe"
                             and getattr(self, "_snipe_research_flags", {}).get(launch.mint, False))
        if is_research_snipe:
            size_mult *= float(self.config.greylist_snipe_research_size_mult or 0.5)
        size_mult = min(size_mult, 2.0)
        base_usd = self.config.max_trade_usd * size_mult
        trade_usd = max(self.config.min_trade_usd, base_usd)
        trade_sol = trade_usd / sol_price if sol_price > 0 else 0
        sol_in_lamports = int(trade_sol * LAMPORTS_PER_SOL)
        if sol_in_lamports <= 0:
            return

        # Route by protocol — graduated tokens trade on PumpSwap AMM
        bucket = self.tracking.get(launch.mint, {})
        protocol = bucket.get("protocol") or "pumpfun"
        pumpswap_state: dict | None = None
        if protocol == "pumpswap":
            pool = bucket.get("pumpswap_pool") or (await pumpswap.find_pool_for_mint(launch.mint))
            if not pool:
                logger.info(f"skip {launch.mint} [pumpswap]: no pool found")
                return
            pumpswap_state = await pumpswap.fetch_pool_state(pool)
            if not pumpswap_state:
                logger.info(f"skip {launch.mint} [pumpswap]: pool state unavailable")
                return
            bucket["pumpswap_pool"] = pool
            state = {
                "real_sol_reserves": pumpswap_state["quote_reserves"],
                "complete": False,
            }
        else:
            state = await pumpfun.fetch_bonding_curve_state(launch.mint)
            if not state or state["complete"]:
                return

        # Resolve band-specific gates: "new" (action=momentum_new) uses tighter
        # thresholds, "seasoned" (action=scanner_momentum) uses base thresholds.
        is_new_band = action == "momentum_new"
        # Greylist Sniper bypasses MOMENTUM-side gates entirely. The whole
        # point of the greylist is sniping creators on predictable curves,
        # so growth/inflow/buyer/velocity gates would defeat the strategy.
        # SAFETY gates already ran in `_enter()` (kill switch, max_concurrent,
        # cooldowns, doctor pause). Pool state checks above also already ran.
        is_greylist_snipe = action == "greylist_snipe"
        if is_greylist_snipe:
            logger.info(
                f"greylist_snipe: bypassing momentum gates for {launch.mint[:8]}… "
                f"(creator={launch.creator[:8]}…, protocol={protocol})"
            )

        # Classifier-action whitelist gate. When non-empty, only the listed
        # actions are allowed to enter. Strategy Doctor populates this when
        # a clear outperforming bucket emerges (rule_classifier_bucket_focus).
        wl = self.config.classifier_action_whitelist or []
        if wl and action not in wl and not is_greylist_snipe:
            logger.info(f"skip {launch.mint} [{action}]: action not in whitelist {wl}")
            await hub.broadcast("scanner_skip", {
                "mint": launch.mint, "symbol": launch.symbol,
                "band": "new" if is_new_band else "seasoned",
                "reason": "classifier_whitelist",
                "details": [f"action '{action}' not in whitelist"],
            })
            return

        min_liq = self.config.min_curve_liquidity_sol_new if is_new_band else self.config.min_curve_liquidity_sol
        min_buyers = self.config.min_buyers_for_entry_new if is_new_band else self.config.min_buyers_for_entry

        # Liquidity gate: skip entry if curve has too little real SOL.
        # Greylist snipes use a much looser floor (0.1 SOL) — fresh curves
        # haven't had time to accumulate liquidity but the snipe is on the
        # creator pattern, not the curve depth.
        real_sol = state["real_sol_reserves"] / LAMPORTS_PER_SOL
        effective_min_liq = 0.1 if is_greylist_snipe else min_liq
        if real_sol < effective_min_liq:
            logger.info(f"skip {launch.mint} [{action}]: liquidity {real_sol:.2f} SOL < min {effective_min_liq}")
            return

        # Buyer gate — works for BOTH bands now:
        # - NEW band: Helius mempool buy events populate `buyers` set (live count
        #   of unique wallets that bought this in the tracking window).
        # - SEASONED band: Helius doesn't cover PumpSwap, so use `buy_count`
        #   from Pump.fun's `/coins/{mint}` endpoint (cumulative since launch,
        #   refreshed by discovery polling).
        # Both signal "real interest" but at different time scales — that's OK;
        # `min_buyers_for_entry` should be set higher than `_new` accordingly.
        if min_buyers > 0 and not is_greylist_snipe:
            b = self.tracking.get(launch.mint, {})
            if is_new_band:
                buyers = len(b.get("buyers", set()))
            else:
                buyers = int(b.get("buy_count") or 0)
            if buyers < min_buyers:
                logger.info(f"skip {launch.mint} [{action}]: only {buyers} buyers < min {min_buyers}")
                await hub.broadcast("scanner_skip", {
                    "mint": launch.mint, "symbol": launch.symbol,
                    "band": "new" if is_new_band else "seasoned",
                    "reason": "buyers",
                    "details": [f"{buyers} < min {min_buyers}"],
                })
                return

        # Pre-trade classifier gate (NEW band PumpFun only — seasoned/PumpSwap
        # tokens don't have mempool metrics so the classifier would spuriously
        # abort them). If the classifier would abort/exit_early *immediately*
        # post-entry, refuse to enter — saves entry fees + exit slippage on a
        # certain loser.
        if is_new_band and protocol == "pumpfun" and not is_greylist_snipe:
            b = self.tracking.get(launch.mint, {})
            metrics = {
                "elapsed_s": time.time() - b.get("start", time.time()),
                "curve_fill_pct": b.get("curve_fill_pct", 0.0),
                "unique_buyers": len(b.get("buyers", set())),
                "sol_inflow": b.get("sol_inflow_lamports", 0) / LAMPORTS_PER_SOL,
                "creator_rugs": b.get("creator_rugs", 0),
                "social_score": b.get("social_score", 0),
            }
            verdict = classify(metrics, self.rules.model_dump())
            # Reject abort/exit_early outright AND reject hold_briefly when
            # risk_score > 50 — those trades were the bulk of our last 50
            # exits via `classifier abort` at -13% to -25%, where the
            # classifier flipped from hold_briefly→exit_early as the curve
            # filled.
            veto_reason = None
            if verdict["action"] in ("abort_trade", "exit_early"):
                veto_reason = verdict["action"]
            elif verdict["action"] == "hold_briefly" and risk_score > 50:
                veto_reason = f"hold_briefly + high risk ({risk_score})"
            if veto_reason:
                logger.info(
                    f"skip {launch.mint} [{action}]: pre-trade classifier "
                    f"{veto_reason} — {verdict['reasons']}"
                )
                await hub.broadcast("scanner_skip", {
                    "mint": launch.mint, "symbol": launch.symbol,
                    "band": "new", "reason": veto_reason,
                    "details": verdict["reasons"],
                })
                return

        # Entry-velocity gate (pattern-mining insight: SL exits dominate losers
        # 39% vs winners 2%). Reject "dead-cat" entries by requiring a minimum
        # price velocity over the configured window right before entry. Uses
        # `price_samples` populated by either on_trade (NEW band) or the
        # discovery refresh loop (SEASONED band). Skips silently if we don't
        # yet have enough history to span the requested window — safer than
        # using a partial-window reading to abort.
        b = self.tracking.get(launch.mint, {})
        samples = b.get("price_samples")
        vel_window = max(5, int(self.config.scanner_entry_velocity_window_s))
        velocity = velocity_pct_strict(samples, time.time(), vel_window) if samples else None
        if not is_greylist_snipe and velocity is not None and velocity < self.config.scanner_entry_velocity_min_pct:
            logger.info(
                f"skip {launch.mint} [{action}]: entry velocity "
                f"{velocity:+.2f}% over {vel_window}s < min "
                f"{self.config.scanner_entry_velocity_min_pct:.2f}% (dead-cat filter)"
            )
            await hub.broadcast("scanner_skip", {
                "mint": launch.mint, "symbol": launch.symbol,
                "band": "new" if is_new_band else "seasoned",
                "reason": "entry_velocity",
                "details": [
                    f"velocity {velocity:+.2f}% over {vel_window}s < "
                    f"{self.config.scanner_entry_velocity_min_pct:.2f}%"
                ],
            })
            return

        # On-chain social-proof gate. When enabled, require at least one
        # social link (twitter / telegram / website) AND reply_count >= min.
        # Discovery refresh and `_fetch_pumpfun_socials` both populate these
        # fields, but on a brand-new launch the first fetch may not have
        # completed yet. If we have no metadata, BLOCK for up to 3s to do
        # a synchronous fetch — better to wait briefly than silently reject
        # every fresh launch.
        if self.config.gate_socials_required and not is_greylist_snipe:
            b = self.tracking.get(launch.mint, {})
            reply_count = int(b.get("reply_count") or 0)
            has_social = bool((b.get("twitter") or b.get("telegram") or b.get("website") or "").strip())
            # No metadata yet → block-fetch once (max 3s).
            if reply_count == 0 and not has_social:
                try:
                    import httpx
                    async with httpx.AsyncClient(timeout=3.0) as client:
                        r = await client.get(
                            f"https://frontend-api-v3.pump.fun/coins/{launch.mint}",
                            headers={"accept": "application/json"},
                        )
                        if r.status_code == 200:
                            c = r.json() or {}
                            b["reply_count"] = int(c.get("reply_count") or 0)
                            b["twitter"] = (c.get("twitter") or "").strip()
                            b["telegram"] = (c.get("telegram") or "").strip()
                            b["website"] = (c.get("website") or "").strip()
                            reply_count = b["reply_count"]
                            has_social = bool((b["twitter"] or b["telegram"] or b["website"]).strip())
                except Exception as e:
                    logger.debug(f"socials prefetch failed for {launch.mint}: {e}")
            min_replies = max(0, int(self.config.gate_min_reply_count))
            if not has_social or reply_count < min_replies:
                logger.info(
                    f"skip {launch.mint} [{action}]: socials gate — "
                    f"reply_count={reply_count} (min {min_replies}), "
                    f"has_social={has_social}"
                )
                await hub.broadcast("scanner_skip", {
                    "mint": launch.mint, "symbol": launch.symbol,
                    "band": "new" if is_new_band else "seasoned",
                    "reason": "socials",
                    "details": [
                        f"reply_count={reply_count} < min {min_replies}"
                        if reply_count < min_replies
                        else "no twitter / telegram / website link"
                    ],
                })
                return

        # Resolve effective fees from speed_mode (e.g. ECO/NORMAL/FAST/AUTO)
        eff_priority, eff_slip, _eff_exit_slip = self._resolve_fees()

        # Dynamic entry slippage by curve depth — thinner curves (early in
        # the bonding-curve lifecycle) move PRICE much faster, so a static
        # slippage tolerance was triggering Custom:6002 (TooMuchSolRequired)
        # reverts. Auto-widen slippage for thin curves. Even deep curves
        # need a 8% floor on hot launches because a few sniper buys land
        # in the same slot and move price >3% before our tx confirms.
        if protocol == "pumpfun":
            vsr_sol = state.get("virtual_sol_reserves", 0) / LAMPORTS_PER_SOL
            if vsr_sol < 32:        # very early — first ~2 SOL of buy pressure
                eff_slip = max(eff_slip, 2500)  # 25%
            elif vsr_sol < 40:      # early — first ~10 SOL
                eff_slip = max(eff_slip, 2000)  # 20%
            elif vsr_sol < 55:      # mid — first ~25 SOL
                eff_slip = max(eff_slip, 1500)  # 15%
            else:                   # deep curve — still need a floor
                eff_slip = max(eff_slip, 1000)  # 10% minimum for any entry
        elif protocol == "pumpswap":
            # PumpSwap AMM pools also need a depth-aware entry-slip floor.
            # `quote_reserves` (WSOL side) is the real liquidity. Thin pools
            # move price faster per SOL of order, so the same floor strategy
            # applies as Pump.fun curves — just based on WSOL reserves
            # instead of virtual SOL reserves. Without this floor, Custom:6002
            # (excess slippage) reverts at entry on hot graduated tokens.
            quote_sol = (pumpswap_state.get("quote_reserves") or 0) / LAMPORTS_PER_SOL
            if quote_sol < 5:        # ultra-thin AMM pool — rare, but real
                eff_slip = max(eff_slip, 2500)  # 25%
            elif quote_sol < 15:     # thin pool — typical fresh-graduate
                eff_slip = max(eff_slip, 1800)  # 18%
            elif quote_sol < 40:     # medium depth
                eff_slip = max(eff_slip, 1200)  # 12%
            else:                    # deep pool — still need floor for sniper races
                eff_slip = max(eff_slip, 800)   # 8% minimum

        tokens_out, max_sol = (
            pumpswap.quote_buy_tokens(pumpswap_state, sol_in_lamports, eff_slip)
            if protocol == "pumpswap"
            else pumpfun.quote_buy_tokens(state, sol_in_lamports, eff_slip)
        )
        if tokens_out <= 0:
            return

        entry_price_sol = sol_in_lamports / tokens_out / LAMPORTS_PER_SOL
        mode = "live" if self.config.live_trading else "paper"

        # Structured entry decision log — captures every factor that fed the
        # buy so post-trade analysis can correlate inputs to outcomes.
        try:
            vsr_log = (state or {}).get("virtual_sol_reserves", 0) / LAMPORTS_PER_SOL
            real_sol_log = (state or {}).get("real_sol_reserves", 0) / LAMPORTS_PER_SOL
            logger.info(
                f"ENTRY_DECISION mint={launch.mint[:8]}… sym={launch.symbol!r} "
                f"action={action} risk={risk_score} size_mult={size_mult:.2f} "
                f"trade_usd={trade_usd:.3f} trade_sol={trade_sol:.5f} "
                f"protocol={protocol} vsr_sol={vsr_log:.2f} real_sol={real_sol_log:.2f} "
                f"eff_slip_bps={eff_slip} eff_priority_uL={eff_priority} "
                f"tokens_out={tokens_out} entry_price_sol={entry_price_sol:.3e}"
            )
        except Exception:
            pass

        cu = CU_PUMPSWAP if protocol == "pumpswap" else CU_PUMPFUN
        est_entry_fee_sol = estimate_tx_fee_sol(eff_priority, cu)

        # Use the creator stored ON the bonding curve, NOT launch metadata.
        # For Pump.fun trades the program checks creator_vault PDA seeds against
        # this; using launch.creator fails with ConstraintSeeds (2006) when the
        # launch was deployed via a service that reports a different creator.
        curve_creator = (state or {}).get("creator") if protocol != "pumpswap" else None
        trade_creator = curve_creator or launch.creator

        trade = Trade(
            mint=launch.mint,
            creator=trade_creator,
            name=launch.name,
            symbol=launch.symbol,
            status="active",
            mode=mode,
            entry_sol=trade_sol,
            entry_usd=trade_sol * sol_price,
            entry_tokens=tokens_out,
            entry_price_sol=entry_price_sol,
            entry_fee_sol=est_entry_fee_sol,
            speed_mode_at_entry=self.config.speed_mode,
            risk_score=risk_score,
            classifier_action=action,
            protocol=protocol,
            pumpswap_pool=bucket.get("pumpswap_pool") or None,
            greylist_strategy_at_entry=greylist_ctx.get("strategy"),
            greylist_score_at_entry=greylist_ctx.get("score"),
            greylist_overrides_at_entry=greylist_ctx.get("overrides") or {},
            greylist_pattern_at_entry=greylist_ctx.get("pattern"),
            greylist_pattern_suggested_tp_pct=greylist_ctx.get("pattern_tp_pct"),
            is_research_snipe=is_research_snipe,
        )
        # Stash protocol on the trade dict (kept in active_trades) so _exit can route
        trade_extras = {
            "protocol": protocol,
            "pumpswap_pool": bucket.get("pumpswap_pool", ""),
            # Per-trade greylist override slot — exit logic reads `greylist_overrides`
            # via `_exit_param()` helper so each position respects ITS own creator's
            # tier (a hot greylist + a standard mint can be open concurrently).
            "greylist_overrides": greylist_ctx.get("overrides") or {},
            "greylist_strategy": greylist_ctx.get("strategy"),
            # Snipe pattern context — read by `_check_snipe_pattern_exit()`
            # to drive curve-fill / peak-MC / rip-cord exits instead of the
            # standard SL/TP/trailing ladder.
            "snipe_pattern_ctx": {
                "expected_peak_mc_usd": greylist_ctx.get("expected_peak_mc_usd"),
                "expected_peak_mc_stddev": greylist_ctx.get("expected_peak_mc_stddev"),
                "expected_rug_curve_pct": greylist_ctx.get("expected_rug_curve_pct"),
                "pattern": greylist_ctx.get("pattern"),
            } if action == "greylist_snipe" else None,
        }

        if mode == "live":
            try:
                kp = get_keypair()
                user = get_pubkey()
                mint_pk = Pubkey.from_string(launch.mint)
                if protocol == "pumpswap":
                    # PumpSwap pools can hold Token-2022 base mints (like ETB).
                    # Failing to thread the correct token program through the
                    # ATA + buy IX produces IncorrectProgramId reverts.
                    base_tp = await pumpfun.get_mint_token_program(launch.mint)
                    user_token_ata = pumpswap.get_associated_token_address(user, mint_pk, base_tp)
                    wsol_acc, wsol_ixs = pumpswap.build_wsol_wrap_ixs(user, max_sol)
                    ixs = [
                        pumpswap.build_create_ata_ix(user, user, mint_pk, base_tp),
                        *wsol_ixs,
                        pumpswap.build_buy_ix(
                            user, pumpswap_state, user_token_ata, wsol_acc,
                            base_amount_out=tokens_out,
                            max_quote_amount_in=max_sol,
                            base_token_program=base_tp,
                        ),
                        pumpswap.build_close_wsol_ix(user, wsol_acc),
                    ]
                    sig = await pumpfun.send_versioned_tx(
                        kp, ixs, eff_priority,
                        compute_unit_limit=400_000,
                    )
                else:
                    # Use curve creator (already fetched into trade.creator)
                    if not trade_creator:
                        raise RuntimeError("missing creator (required for creator_vault PDA)")
                    creator_pk = Pubkey.from_string(trade_creator)
                    tp = await pumpfun.get_mint_token_program(launch.mint)
                    ixs = [
                        pumpfun.build_create_ata_ix(user, user, mint_pk, tp),
                        await pumpfun.build_buy_ix(user, mint_pk, tokens_out, max_sol, creator_pk, tp),
                    ]
                    sig = await pumpfun.send_versioned_tx(
                        kp, ixs, eff_priority
                    )
                trade.entry_sig = sig
            except Exception as e:
                logger.exception(f"Live buy failed for {launch.mint}: {e}")
                trade.status = "failed"
                trade.exit_reason = f"buy failed: {e}"
                await self._persist_trade(trade)
                # Cooldown the mint in the scanner so we don't retry the same
                # broken tx every pass.
                b = self.tracking.get(launch.mint)
                if b is not None:
                    b["scanner_last_attempt"] = time.time()
                # Hard 60s cross-system cooldown — without this the scanner
                # re-evaluates the mint within 30s, re-passes the gates, and
                # the bot burns another $0.05 in gas for the same failure
                # (observed with ETB / IncorrectProgramId — 3 retries in 2 min
                # cost $0.15 with zero chance of any of them succeeding).
                self.recent_exit_until[launch.mint] = time.time() + 60.0
                return

        await self._persist_trade(trade)
        # Phase 2.9 — pin the mint in the scanner feed when entered on a
        # greylisted creator. Card stays pinned (at top, with a badge) until
        # manually unpinned, surviving the normal scanner aging logic.
        # `pin_exited` flips later in `_exit` so the card greys out.
        launch_update = {"entered": True, "entry_action": action}
        if greylist_ctx.get("strategy") and greylist_ctx["strategy"] != "standard":
            launch_update.update({
                "pinned": True,
                "pinned_at": datetime.now(timezone.utc).isoformat(),
                "pin_reason": "greylist_entry",
                "pin_creator_pattern": greylist_ctx.get("pattern"),
                "pin_strategy": greylist_ctx.get("strategy"),
                "pin_exited": False,
            })
        await self.db.launches.update_one({"_id": launch.id}, {"$set": launch_update})
        for r in self.recent_launches:
            if r.get("id") == launch.id:
                r.update(launch_update)
                break

        self.active_trades[launch.mint] = {
            "trade": trade.model_dump(),
            "launch": launch.model_dump(),
            "_entry_ts_mono": time.time(),  # for greylist_snipe stale-exit gate
            **trade_extras,
        }
        await hub.broadcast("trade_enter", trade.model_dump())
        asyncio.create_task(self._monitor_position(launch.mint))

    async def _persist_trade(self, trade: Trade):
        doc = trade.model_dump()
        doc["entry_time"] = doc["entry_time"].isoformat()
        if doc.get("exit_time"):
            doc["exit_time"] = doc["exit_time"].isoformat()
        await self.db.trades.update_one(
            {"_id": trade.id}, {"$set": {**doc, "_id": trade.id}}, upsert=True
        )

    async def _monitor_position(self, mint: str):
        slot = self.active_trades.get(mint)
        if not slot:
            return
        # Mark this monitor as the live one for this slot — if another monitor
        # was running it'll see a different uid on the next tick and exit.
        # Combined with `last_monitor_tick`, the reconciler can detect and
        # respawn dead monitors without spawning duplicates.
        import uuid as _uuid
        monitor_uid = _uuid.uuid4().hex[:8]
        slot["monitor_uid"] = monitor_uid
        slot["last_monitor_tick"] = time.time()
        trade_doc = slot["trade"]
        start = time.time()
        max_hold = self.config.hold_max_seconds
        last_classify = 0.0
        # Rolling (ts, price_sol) samples for the velocity-aware timeout check.
        # Survives across this monitor's lifetime; reset if a new monitor takes over.
        if "monitor_price_samples" not in slot:
            slot["monitor_price_samples"] = []
        last_extend_log = 0.0

        # === LaserStream WebSocket wake-up channel ===
        # Subscribe to the on-chain account that mutates on every buy/sell
        # for this position. Pump.fun → bonding curve PDA. PumpSwap → pool
        # account. The Event fires every time Helius pushes new state.
        # We KEEP polling as a safety net (the .wait_for_change timeout
        # still expires every 0.8s) — WSS is purely a "wake earlier" path,
        # so a stale WSS never blocks SL/TP from firing.
        from account_event_bus import account_event_bus
        if slot.get("protocol") == "pumpswap":
            watch_account = slot.get("pumpswap_pool") or ""
        else:
            # Try (in order): explicit field on slot, launch dict, trade dict,
            # derive PDA from mint as a deterministic fallback (works even
            # for restored-from-DB trades that lost the launch object).
            watch_account = (
                slot.get("bonding_curve")
                or (slot.get("launch") or {}).get("bonding_curve")
                or (slot.get("trade") or {}).get("bonding_curve")
                or ""
            )
            if not watch_account:
                try:
                    watch_account = str(pumpfun.derive_bonding_curve(Pubkey.from_string(mint)))
                except Exception:
                    watch_account = ""
        if watch_account:
            account_event_bus.subscribe(watch_account)
            # Cache on the slot so _exit can unsubscribe the same address
            # without re-deriving (avoids drift if derive logic ever changes).
            slot["watch_account"] = watch_account

        while True:
            # Liveness heartbeat — refreshed every tick. Reconciler uses this
            # to detect dead monitors and respawn them.
            slot = self.active_trades.get(mint)
            if not slot or slot.get("monitor_uid") != monitor_uid:
                return  # slot evicted OR another monitor took over
            slot["last_monitor_tick"] = time.time()
            elapsed = time.time() - start

            try:
                # Greylist snipes use pattern-based exits — short-circuit
                # the entire standard SL/TP/trailing/max-hold ladder. Curve
                # state still needs to be fetched (below) so we have
                # cur_price_sol for the pattern check.
                is_snipe = self._is_snipe(slot)
                if is_snipe:
                    # We still need cur_price_sol for the pattern check, so
                    # fall through to the protocol-aware price polling below.
                    # The standard SL/TP/trailing block past line ~2099 is
                    # gated on `not is_snipe` so snipes skip it.
                    pass
                elif elapsed > max_hold:
                    # Velocity-aware timeout: if the price is still trending up
                    # over the last N seconds, defer the cutoff rather than
                    # cutting a winner mid-pump. TP/SL/trailing keep guarding,
                    # so this only stretches the *hard* timeout, never disables
                    # protections.
                    extend_ok = False
                    if self.config.hold_timeout_velocity_extend_enabled:
                        win_s = max(3, int(self.config.hold_timeout_velocity_window_s))
                        samples = slot.get("monitor_price_samples") or []
                        v = velocity_pct_strict(samples, time.time(), win_s) if samples else None
                        if v is not None and v >= self.config.hold_timeout_velocity_min_pct:
                            extend_ok = True
                            now_t = time.time()
                            if now_t - last_extend_log > 5.0:
                                logger.info(
                                    f"timeout extended for {mint}: velocity {v:+.2f}% over {win_s}s "
                                    f">= {self.config.hold_timeout_velocity_min_pct:.2f}% — riding pump"
                                )
                                last_extend_log = now_t
                    if not extend_ok:
                        slot["exit_in_progress"] = True
                        try:
                            await self._exit(mint, reason=f"timeout after {max_hold}s")
                            return
                        finally:
                            slot["exit_in_progress"] = False

                # Protocol-aware price polling
                protocol = slot.get("protocol", "pumpfun")
                if protocol == "pumpswap":
                    pool = slot.get("pumpswap_pool") or ""
                    pool_state = await pumpswap.fetch_pool_state(pool) if pool else None
                    if not pool_state:
                        await asyncio.sleep(1.0)
                        continue
                    cur_price_sol = pumpswap.price_sol_per_raw_token(pool_state)
                else:
                    state = await pumpfun.fetch_bonding_curve_state(mint)
                    if not state:
                        await asyncio.sleep(1.0)
                        continue
                    if state["complete"]:
                        slot["exit_in_progress"] = True
                        try:
                            await self._exit(mint, reason="bonding curve completed (LP about to deploy)")
                            return
                        finally:
                            slot["exit_in_progress"] = False
                    cur_price_sol = state["virtual_sol_reserves"] / state["virtual_token_reserves"] / LAMPORTS_PER_SOL

                pct_change = (cur_price_sol - trade_doc["entry_price_sol"]) / max(trade_doc["entry_price_sol"], 1e-18) * 100

                # Cache last seen price on the slot so `/api/trades/active`
                # can surface live PnL% to the UI without re-fetching curve
                # state on every poll.
                slot["_last_price_sol"] = cur_price_sol

                # Bail out of this tick if another exit (fast-exit path or a
                # prior monitor tick) is already in flight — they're operating
                # on the same slot and would race for the same wallet balance,
                # producing Custom:6023 reverts on whichever loses.
                if slot.get("exit_in_progress"):
                    # Same wait pattern — push-based wake or 0.4s safety net
                    if watch_account:
                        await account_event_bus.wait_for_change(watch_account, timeout=0.4)
                    else:
                        await asyncio.sleep(0.4)
                    continue

                # Snipes: pattern-based exit ONLY. Skip the rest of the
                # standard exit ladder entirely (no SL, no max-hold trail,
                # no live classifier abort — see _check_snipe_pattern_exit
                # for what we DO check).
                if is_snipe:
                    should_exit, reason = self._check_snipe_pattern_exit(slot, cur_price_sol)
                    if should_exit:
                        slot["exit_in_progress"] = True
                        try:
                            await self._exit(mint, reason=reason)
                            return
                        finally:
                            slot["exit_in_progress"] = False
                    # Push-based wake / safety sleep, then back to top.
                    if watch_account:
                        await account_event_bus.wait_for_change(watch_account, timeout=0.8)
                    else:
                        await asyncio.sleep(0.8)
                    continue

                # Track price samples for the velocity-aware timeout (cap window ~ 2x velocity window)
                _now = time.time()
                samples = slot.get("monitor_price_samples")
                if samples is not None:
                    samples.append((_now, cur_price_sol))
                    cutoff = _now - max(30, int(self.config.hold_timeout_velocity_window_s) * 3)
                    # Trim from the front
                    while samples and samples[0][0] < cutoff:
                        samples.pop(0)

                # Per-position thresholds — greylist overrides win when present.
                m_tp_pct = self._exit_param(slot, "tp_pct", self.config.take_profit_pct)
                m_sl_pct = self._exit_param(slot, "sl_pct", self.config.stop_loss_pct)

                if pct_change >= m_tp_pct and not slot.get("partial_done"):
                    slot["exit_in_progress"] = True
                    try:
                        ptp = self.config.partial_tp_pct
                        if 0 < ptp < 100:
                            slot["partial_done"] = True
                            did = await self._partial_exit(
                                mint, ptp / 100.0,
                                reason=f"partial-tp ({ptp:.0f}%) at +{pct_change:.1f}%",
                            )
                            if did:
                                slot["peak_price_sol"] = cur_price_sol
                                await asyncio.sleep(0.5)
                                continue
                            slot["partial_done"] = False
                        await self._exit(mint, reason=f"take-profit hit (+{pct_change:.1f}%)")
                        return
                    finally:
                        slot["exit_in_progress"] = False
                if pct_change <= -m_sl_pct:
                    # v2: persistence — only fire if breach has been sustained.
                    # v2.1: severity override (see _check_exit_conditions_realtime).
                    if self.config.intelligent_exit_v2:
                        fire = self._check_breach_persistence(
                            slot, kind="sl", breached=True,
                            persistence_ms=self.config.sl_persistence_ms,
                            min_samples=self.config.sl_persistence_min_samples,
                            severity_pct=(-pct_change - m_sl_pct),
                            severity_threshold_pct=5.0,
                        )
                    else:
                        fire = True
                    if fire:
                        slot["exit_in_progress"] = True
                        try:
                            await self._exit(mint, reason=f"stop-loss hit ({pct_change:.1f}%)")
                            return
                        finally:
                            slot["exit_in_progress"] = False
                elif self.config.intelligent_exit_v2:
                    # Recovered above SL → clear breach state
                    self._check_breach_persistence(
                        slot, kind="sl", breached=False,
                        persistence_ms=self.config.sl_persistence_ms,
                        min_samples=self.config.sl_persistence_min_samples,
                    )

                # Classifier monitoring applies only to NEW band entries (fresh
                # mempool launches). Seasoned/PumpSwap trades skip it because
                # `curve_fill_pct=100` would trigger spurious exit_early signals
                # and we don't have meaningful mempool metrics for them.
                if (
                    time.time() - last_classify > 2.0
                    and trade_doc.get("classifier_action") == "momentum_new"
                    and protocol == "pumpfun"
                ):
                    last_classify = time.time()
                    b = self.tracking.get(mint, {})
                    metrics = {
                        "elapsed_s": elapsed + ASSESS_DELAY_S,
                        "curve_fill_pct": b.get("curve_fill_pct", 0.0),
                        "unique_buyers": len(b.get("buyers", set())),
                        "sol_inflow": b.get("sol_inflow_lamports", 0) / LAMPORTS_PER_SOL,
                        "creator_rugs": b.get("creator_rugs", 0),
                        "social_score": b.get("social_score", 0),
                    }
                    verdict = classify(metrics, self.rules.model_dump())
                    trade_doc["risk_score"] = verdict["risk"]
                    if verdict["action"] == "abort_trade":
                        slot["exit_in_progress"] = True
                        try:
                            await self._exit(mint, reason=f"classifier abort: {verdict['reasons']}")
                            return
                        finally:
                            slot["exit_in_progress"] = False
                    if verdict["action"] == "exit_early" and elapsed > 3:
                        slot["exit_in_progress"] = True
                        try:
                            await self._exit(mint, reason=f"classifier exit_early: {verdict['reasons']}")
                            return
                        finally:
                            slot["exit_in_progress"] = False

                # Push-based wake: returns instantly if Helius pushes a new
                # account state for the bonding curve / pool (i.e. a trade
                # just landed), otherwise falls through after 0.8s — same
                # cadence as the previous unconditional sleep. SL/TP/trailing
                # checks above are unchanged; we just react sooner when the
                # market moves and stay quiet when it doesn't.
                if watch_account:
                    await account_event_bus.wait_for_change(watch_account, timeout=0.8)
                else:
                    await asyncio.sleep(0.8)
            except asyncio.CancelledError:
                raise
            except Exception as e:
                # Transient RPC error (429, ConnectTimeout, etc.) — DON'T let
                # this kill the monitor. Sleep with backoff and retry next tick.
                # The previous behaviour exited the task entirely on the first
                # blip, leaving the position orphaned with the dict still
                # holding the slot. Reconciler now detects dead monitors via
                # last_monitor_tick but it's cheaper to just survive the blip.
                logger.warning(
                    f"monitor transient error for {mint}: {type(e).__name__}: {e} — retrying"
                )
                await asyncio.sleep(2.0)
                continue

    async def _partial_exit(self, mint: str, fraction: float, reason: str):
        """Sell `fraction` of the remaining position. Banks realized PnL onto
        the trade doc, reduces `entry_tokens`, and keeps the slot active so the
        runner can ride further with a tightened trailing stop.

        Returns True if a partial exit was actually performed."""
        slot = self.active_trades.get(mint)
        if not slot:
            return False
        if slot.get("partial_persisted"):
            return False  # already partialled — don't re-partial
        if not (0.0 < fraction < 1.0):
            return False
        trade_doc = slot["trade"]
        protocol = slot.get("protocol", "pumpfun")
        sol_price = await get_sol_usd_price()
        pumpswap_state = None
        if protocol == "pumpswap":
            pool = slot.get("pumpswap_pool") or ""
            pumpswap_state = await pumpswap.fetch_pool_state(pool) if pool else None
            state = pumpswap_state
        else:
            state = await pumpfun.fetch_bonding_curve_state(mint)
        if not state:
            return False

        held = int(trade_doc["entry_tokens"])
        sell_tokens = int(held * fraction)
        if sell_tokens <= 0:
            return False

        # For live trades, cap by ACTUAL wallet balance. Use Token-2022-aware
        # ATA derivation — most Pump.fun mints post-2026-04-28 are Token-2022,
        # and the legacy derive_associated_token() reads an empty/non-existent
        # ATA which leaves sell_tokens at the (possibly oversized) entry value.
        if trade_doc["mode"] == "live":
            try:
                user = get_pubkey()
                mint_pk = Pubkey.from_string(mint)
                if protocol == "pumpswap":
                    ata = pumpswap.get_associated_token_address(user, mint_pk, pumpswap.TOKEN_PROGRAM)
                else:
                    tp = await pumpfun.get_mint_token_program(mint)
                    ata = pumpfun.derive_associated_token_for_program(user, mint_pk, tp)
                actual = await pumpswap.get_token_balance(ata)
                if actual > 0:
                    # 0.5% shave protects against Custom:6023 (NotEnoughTokensToSell)
                    # when on-chain reserves shift between read and land.
                    sell_tokens = min(sell_tokens, int(actual * 0.995))
                    if sell_tokens <= 0:
                        sell_tokens = actual
                elif actual == 0:
                    logger.warning(f"partial-sell aborted for {mint}: ATA balance is 0")
                    return False
            except Exception as e:
                logger.warning(f"partial balance read failed for {mint}: {e}")

        eff_priority, _eff_slip, eff_exit_slip = self._resolve_fees()
        is_panic_partial = self._is_panic_exit(reason)
        # Intelligent Exit v2: partial-TP usually fires on positive news, so
        # auto-slip stays at base 3% unless pool depth is thin.
        if self.config.intelligent_exit_v2:
            depth_sol = self._pool_depth_sol(state, protocol)
            vol_pct = self._recent_vol_pct(
                slot.get("monitor_price_samples") or [],
                int(self.config.auto_exit_slip_vol_window_s),
            )
            exit_slip = self._compute_auto_exit_slip_bps(
                panic=is_panic_partial, pool_depth_sol=depth_sol, recent_vol_pct=vol_pct,
            )
            if is_panic_partial:
                eff_priority = max(eff_priority, int(self.config.panic_exit_priority_microlamports))
        else:
            exit_slip = self._exit_slip_for(reason, eff_exit_slip)
        if protocol == "pumpswap":
            sol_out, min_sol = pumpswap.quote_sell_sol(pumpswap_state, sell_tokens, exit_slip)
        else:
            sol_out, min_sol = pumpfun.quote_sell_sol(state, sell_tokens, exit_slip)
        if sol_out <= 0:
            return False
        partial_sol = sol_out / LAMPORTS_PER_SOL

        partial_sig = None
        if trade_doc["mode"] == "live":
            try:
                kp = get_keypair()
                user = get_pubkey()
                mint_pk = Pubkey.from_string(mint)
                # Slip-escalation ladder for the partial (same pattern as full-exit)
                slip_ladder = [exit_slip]
                if self.config.intelligent_exit_v2:
                    for floor_bps in (self.config.auto_exit_retry_slip_floors_bps or []):
                        if int(floor_bps) > slip_ladder[-1]:
                            slip_ladder.append(int(floor_bps))
                last_err: Exception | None = None
                for attempt_idx, attempt_slip in enumerate(slip_ladder):
                    if protocol == "pumpswap":
                        _, attempt_min_sol = pumpswap.quote_sell_sol(pumpswap_state, sell_tokens, attempt_slip)
                    else:
                        _, attempt_min_sol = pumpfun.quote_sell_sol(state, sell_tokens, attempt_slip)
                    if protocol == "pumpswap":
                        # Token-2022 pools require explicit base_token_program;
                        # default classic SPL is wrong and reverts with IncorrectProgramId.
                        # ALSO: PumpSwap sell requires the canonical user WSOL ATA, not
                        # a seed-derived temp account, or reverts with Custom:6053.
                        base_tp = await pumpfun.get_mint_token_program(mint)
                        user_token_ata = pumpswap.get_associated_token_address(user, mint_pk, base_tp)
                        wsol_ata, wsol_ixs = pumpswap.build_wsol_ata_idempotent_ixs(user)
                        ixs = [
                            pumpswap.build_create_ata_ix(user, user, mint_pk, base_tp),
                            *wsol_ixs,
                            pumpswap.build_sell_ix(
                                user, pumpswap_state, user_token_ata, wsol_ata,
                                base_amount_in=sell_tokens, min_quote_amount_out=attempt_min_sol,
                                base_token_program=base_tp,
                            ),
                            pumpswap.build_close_wsol_ix(user, wsol_ata),
                        ]
                        try:
                            partial_sig = await pumpfun.send_versioned_tx(
                                kp, ixs, eff_priority, compute_unit_limit=400_000,
                            )
                        except Exception as _se:
                            last_err = _se
                            if "Custom': 6003" in str(_se) and attempt_idx + 1 < len(slip_ladder):
                                logger.info(
                                    f"partial slip-retry {attempt_idx + 1}/{len(slip_ladder) - 1} "
                                    f"for {mint[:8]} after {attempt_slip}bps: escalating"
                                )
                                continue
                            raise
                    else:
                        creator_str = trade_doc.get("creator") or (slot.get("launch") or {}).get("creator") or ""
                        if state and state.get("creator"):
                            creator_str = state["creator"]
                        if not creator_str:
                            raise RuntimeError("missing creator for partial-sell creator_vault PDA")
                        creator_pk = Pubkey.from_string(creator_str)
                        tp = await pumpfun.get_mint_token_program(mint)
                        is_cashback = bool((state or {}).get("is_cashback", False))
                        ix = await pumpfun.build_sell_ix(user, mint_pk, sell_tokens, attempt_min_sol, creator_pk, tp, cashback=is_cashback)
                        try:
                            partial_sig = await pumpfun.send_versioned_tx(
                                kp, [ix], eff_priority
                            )
                        except Exception as _se:
                            last_err = _se
                            if "Custom': 6003" in str(_se) and attempt_idx + 1 < len(slip_ladder):
                                logger.info(
                                    f"partial slip-retry {attempt_idx + 1}/{len(slip_ladder) - 1} "
                                    f"for {mint[:8]} after {attempt_slip}bps: escalating"
                                )
                                continue
                            raise
                    # Landed — record actual slip used
                    exit_slip = attempt_slip
                    break
                else:
                    if last_err:
                        raise last_err
            except Exception as e:
                logger.exception(f"partial sell failed for {mint}: {e}")
                return False

        # Compute realized contribution from this partial
        entry_sol_per_token = trade_doc["entry_sol"] / max(trade_doc["entry_tokens"], 1)
        partial_cost_sol = entry_sol_per_token * sell_tokens
        realized_sol = partial_sol - partial_cost_sol
        realized_usd = realized_sol * sol_price

        # Update trade doc — reduce remaining position, bank realized PnL
        cu = CU_PUMPSWAP if protocol == "pumpswap" else CU_PUMPFUN
        partial_fee_sol = estimate_tx_fee_sol(eff_priority, cu)
        trade_doc["partial_done"] = True
        trade_doc["partial_sell_tokens"] = sell_tokens
        trade_doc["partial_sell_sol"] = partial_sol
        trade_doc["partial_sell_usd"] = partial_sol * sol_price
        trade_doc["partial_realized_sol"] = realized_sol
        trade_doc["partial_realized_usd"] = realized_usd
        trade_doc["partial_sig"] = partial_sig
        trade_doc["partial_reason"] = reason
        trade_doc["partial_fee_sol"] = partial_fee_sol
        trade_doc["entry_tokens"] = held - sell_tokens
        trade_doc["entry_sol"] = trade_doc["entry_sol"] - partial_cost_sol
        trade_doc["entry_usd"] = trade_doc["entry_sol"] * sol_price
        slot["partial_done"] = True
        slot["partial_persisted"] = True
        # Block re-exit for 3s — gives Helius RPC time to propagate the
        # post-partial wallet balance. Without this, a fast trailing-stop
        # tick reads the stale pre-partial balance and oversells (6023).
        slot["exit_blocked_until"] = time.time() + 3.0

        await self.db.trades.update_one(
            {"_id": trade_doc["id"]}, {"$set": trade_doc}, upsert=True
        )
        await hub.broadcast("trade_partial", {
            "id": trade_doc["id"], "mint": mint, "symbol": trade_doc.get("symbol"),
            "partial_realized_usd": realized_usd, "fraction": fraction, "reason": reason,
        })
        logger.info(
            f"partial exit {mint} ({fraction*100:.0f}%): banked ${realized_usd:+.2f}, "
            f"remaining {trade_doc['entry_tokens']} tokens"
        )
        return True

    async def _attempt_emergency_pumpswap_sell(
        self,
        *,
        mint: str,
        kp,
        user,
        mint_pk,
        tokens_in: int,
    ) -> tuple[str, int, dict] | None:
        """Last-resort sell via PumpSwap AMM. Used to auto-recover from:
          - Bonding-curve 6005 (BondingCurveComplete) mid-sell — token just
            graduated; classic curve sell will never work again.
          - Normal-flow sell-ladder exhaustion (3 retries) on either protocol —
            one final brute-force attempt before we give up and dump the
            position into the stuck list.

        Brute-force settings:
          * 50% slippage (5000 bps) — accept whatever the pool gives us
          * 5M µLamp priority fee — maximum landing odds
          * 600k compute-unit limit — wide budget for the 4-ix combo
          * 60s confirmation timeout — give Helius plenty of room

        Returns (sig, sol_out_lamports, pool_state) on success, None if no
        pool exists or the tx never lands. Never raises — failures are
        logged and the caller decides whether to mark the position terminal.
        """
        try:
            pool = await pumpswap.find_pool_for_mint(mint)
            if not pool:
                logger.warning(
                    f"emergency sell aborted for {mint[:8]}…: no PumpSwap pool"
                )
                return None
            pool_state = await pumpswap.fetch_pool_state(pool)
            if not pool_state:
                logger.warning(
                    f"emergency sell aborted for {mint[:8]}…: pool state unavailable"
                )
                return None
            # 0.5% shave guards against late-landing balance drift
            sell_amount = max(int(tokens_in * 0.995), 1)
            sol_out, min_sol = pumpswap.quote_sell_sol(pool_state, sell_amount, 5000)
            base_tp = await pumpfun.get_mint_token_program(mint)
            user_token_ata = pumpswap.get_associated_token_address(user, mint_pk, base_tp)
            wsol_ata, wsol_ixs = pumpswap.build_wsol_ata_idempotent_ixs(user)
            ixs = [
                pumpswap.build_create_ata_ix(user, user, mint_pk, base_tp),
                *wsol_ixs,
                pumpswap.build_sell_ix(
                    user, pool_state, user_token_ata, wsol_ata,
                    base_amount_in=sell_amount,
                    min_quote_amount_out=min_sol,
                    base_token_program=base_tp,
                ),
                pumpswap.build_close_wsol_ix(user, wsol_ata),
            ]
            # Try Helius Sender first (dual routing → validators + Jito,
            # maximum landing odds; 0.0002 SOL tip overhead). Fall back to
            # standard RPC submit if Sender errors so we never leave a
            # position truly stuck for a transient network blip.
            try:
                from helius_sender import send_via_sender
                sig = await send_via_sender(
                    kp, ixs,
                    priority_fee_microlamports=5_000_000,
                    compute_unit_limit=600_000,
                    mode="dual",
                    confirm_timeout_s=60.0,
                )
                logger.warning(
                    f"EMERGENCY PUMPSWAP SELL (via Sender) succeeded for {mint[:8]}… — "
                    f"sig={sig[:12]} sold={sell_amount} sol_out~={sol_out/1e9:.6f}"
                )
                return sig, sol_out, pool_state
            except Exception as sender_err:
                logger.warning(
                    f"sender path failed for emergency sell {mint[:8]}…: {sender_err} "
                    f"— falling back to standard RPC submit"
                )
            sig = await pumpfun.send_versioned_tx(
                kp, ixs, priority_fee_microlamports=5_000_000,
                compute_unit_limit=600_000, confirm_timeout_s=60.0,
            )
            logger.warning(
                f"EMERGENCY PUMPSWAP SELL (rpc fallback) succeeded for {mint[:8]}… — "
                f"sig={sig[:12]} sold={sell_amount} sol_out~={sol_out/1e9:.6f}"
            )
            return sig, sol_out, pool_state
        except Exception as e:
            logger.warning(
                f"emergency pumpswap sell failed for {mint[:8]}…: {e}"
            )
            return None

    async def _exit(self, mint: str, reason: str):
        # Atomically pop the slot so concurrent monitors don't both try to exit
        # the same position. If anything below raises before we've persisted a
        # terminal status, we re-insert in the finally so the slot isn't lost.
        slot = self.active_trades.pop(mint, None)
        if not slot:
            return
        # Drop the LaserStream account subscription for this position now
        # that the slot is being torn down. Safe to call even if the bus
        # was never started or the account wasn't tracked. Done here (not
        # in _monitor_position) so EVERY exit path — including reconciler-
        # driven force-closes — releases the WSS slot cleanly.
        try:
            from account_event_bus import account_event_bus
            watch_account = (
                slot.get("pumpswap_pool")
                if slot.get("protocol") == "pumpswap"
                else slot.get("bonding_curve")
            )
            if watch_account:
                account_event_bus.unsubscribe(watch_account)
        except Exception:
            pass
        # Reserve the mint while the exit is in flight so the scanner can't
        # race into a new entry between pop and re-insert. This prevents the
        # "4 positions in 3 min on same mint" pattern: previously a failing
        # exit would pop the slot, attempt the sell, fail, then re-insert —
        # but during the gap, the scanner saw the mint as "free" and opened
        # another position, orphaning the prior monitor.
        self._pending_entry_mints.add(mint)
        try:
            await self._exit_impl(mint, reason, slot)
        except Exception as e:
            # Unhandled error (RPC blip, network timeout, pool fetch fail, etc.)
            # Keep the position alive — re-insert into active_trades so the
            # monitor's next tick will retry. Without this, the dict-vs-DB
            # desync grows every time a 429 hits during exit.
            logger.exception(
                f"_exit unhandled error for {mint} ({reason!r}) — re-inserting "
                f"into active_trades for retry: {e}"
            )
            self.active_trades[mint] = slot
        finally:
            self._pending_entry_mints.discard(mint)

    async def _exit_impl(self, mint: str, reason: str, slot: dict):
        trade_doc = slot["trade"]
        # Respect any post-partial RPC-propagation block. Without this, a
        # trailing-stop tick within 1-2s of a partial sell reads the stale
        # pre-partial wallet balance and oversells (6023 NotEnoughTokensToSell).
        blocked_until = float(slot.get("exit_blocked_until") or 0.0)
        if blocked_until > 0:
            wait = blocked_until - time.time()
            if wait > 0:
                logger.info(f"exit deferred for {mint}: waiting {wait:.1f}s for post-partial RPC sync")
                await asyncio.sleep(min(wait, 5.0))
        sol_price = await get_sol_usd_price()
        protocol = slot.get("protocol", "pumpfun")
        # Resolve sell quote per protocol
        pumpswap_state = None
        if protocol == "pumpswap":
            pool = slot.get("pumpswap_pool") or ""
            pumpswap_state = await pumpswap.fetch_pool_state(pool) if pool else None
            state = pumpswap_state
        else:
            state = await pumpfun.fetch_bonding_curve_state(mint)
        if not state:
            trade_doc["status"] = "closed"
            trade_doc["exit_reason"] = f"{reason} | {protocol} state unavailable"
            trade_doc["exit_time"] = now_utc().isoformat()
            await self.db.trades.update_one(
                {"_id": trade_doc["id"]}, {"$set": trade_doc}, upsert=True
            )
            self.recent_exit_until[mint] = time.time() + 90.0
            return

        tokens_in = int(trade_doc["entry_tokens"])
        # Resolve effective fees from speed_mode for this exit, then widen
        # slippage to panic tier when the exit reason demands fast landing
        # (stop-loss, hard-stop, classifier abort, bonding-curve complete).
        eff_priority, _eff_slip, eff_exit_slip = self._resolve_fees()
        is_panic = self._is_panic_exit(reason)

        # Intelligent Exit v2: auto-slip from pool depth + volatility +
        # panic tier. Replaces flat panic_exit_slippage_bps=2500 (25%).
        # Same formula for pumpfun and pumpswap — depth comes from state.
        if self.config.intelligent_exit_v2:
            depth_sol = self._pool_depth_sol(state, protocol)
            vol_pct = self._recent_vol_pct(
                slot.get("monitor_price_samples") or [],
                int(self.config.auto_exit_slip_vol_window_s),
            )
            exit_slip = self._compute_auto_exit_slip_bps(
                panic=is_panic, pool_depth_sol=depth_sol, recent_vol_pct=vol_pct,
            )
            # Priority-fee bump for panic-tier exits — real front-run defense
            # (faster landing) without the MEV-sandwich invitation of wide slip.
            if is_panic:
                eff_priority = max(eff_priority, int(self.config.panic_exit_priority_microlamports))
        else:
            exit_slip = self._exit_slip_for(reason, eff_exit_slip)

        # For live trades, size the sell by the ACTUAL wallet balance — fees
        # taken at buy time mean our balance is usually a touch lower than
        # entry_tokens, and trying to sell more than we hold reverts the tx
        # with Custom:6023 (NotEnoughTokensToSell).
        # IMPORTANT: Pump.fun tokens are now Token-2022 — must derive ATA with
        # the correct token program, else we read the wrong (empty) ATA and
        # fall back to entry_tokens, which oversells after a partial-TP.
        if trade_doc["mode"] == "live":
            try:
                user = get_pubkey()
                mint_pk = Pubkey.from_string(mint)
                if protocol == "pumpswap":
                    ata = pumpswap.get_associated_token_address(user, mint_pk, pumpswap.TOKEN_PROGRAM)
                    actual = await pumpswap.get_token_balance(ata)
                else:
                    tp = await pumpfun.get_mint_token_program(mint)
                    ata = pumpfun.derive_associated_token_for_program(user, mint_pk, tp)
                    actual = await pumpswap.get_token_balance(ata)
                if actual == 0:
                    # We hold zero of this mint — close the trade WITHOUT
                    # attempting to sell. Sending a 0-amount sell IX would
                    # revert with Custom:6022 (SellZeroAmount) and waste gas.
                    logger.warning(
                        f"sell skipped for {mint}: ATA balance is 0 "
                        f"(already sold or never bought) — closing trade"
                    )
                    trade_doc["status"] = "closed"
                    trade_doc["exit_reason"] = f"{reason} | balance was 0 at exit"
                    trade_doc["exit_time"] = now_utc().isoformat()
                    trade_doc["exit_sol"] = 0.0
                    trade_doc["exit_usd"] = 0.0
                    trade_doc["exit_price_sol"] = 0.0
                    await self.db.trades.update_one(
                        {"_id": trade_doc["id"]}, {"$set": trade_doc}, upsert=True
                    )
                    await hub.broadcast("trade_exit", {
                        "id": trade_doc["id"], "mint": mint,
                        "symbol": trade_doc.get("symbol"),
                        "reason": trade_doc["exit_reason"],
                    })
                    self.recent_exit_until[mint] = time.time() + 90.0
                    return
                # Post-partial exits need a WIDER shave to absorb RPC
                # propagation lag. If the partial sell just landed (~1-2s
                # ago), Helius might still serve the pre-partial balance.
                # Sending a sell sized to the stale balance reverts with
                # Custom:6023 (NotEnoughTokensToSell).
                # Treat the wallet read as authoritative AND apply 5% shave
                # after a partial; otherwise the standard 0.5% safety.
                # propagation lag. If the partial sell just landed (~1-2s
                # ago), Helius might still serve the pre-partial balance.
                # Sending a sell sized to the stale balance reverts with
                # Custom:6023 (NotEnoughTokensToSell).
                # Treat the wallet read as authoritative AND apply 5% shave
                # after a partial; otherwise the standard 0.5% safety.
                pre_shave_tokens = tokens_in
                if slot.get("partial_done"):
                    tokens_in = int(actual * 0.95)
                    shave_label = "partial-5%"
                else:
                    tokens_in = min(tokens_in, int(actual * 0.995))
                    shave_label = "normal-0.5%"
                if tokens_in <= 0:
                    tokens_in = actual  # very small position: send full balance
                logger.info(
                    f"EXIT_DECISION mint={mint[:8]}… sym={trade_doc.get('symbol')!r} "
                    f"reason={reason!r} panic={self._is_panic_exit(reason)} "
                    f"exit_slip_bps={exit_slip} eff_priority_uL={eff_priority} "
                    f"db_entry_tokens={pre_shave_tokens} on_chain={actual} "
                    f"shave={shave_label} sell_tokens={tokens_in}"
                )
            except Exception as e:
                logger.warning(f"balance read failed for {mint}, falling back to entry_tokens: {e}")

        if protocol == "pumpswap":
            sol_out, min_sol = pumpswap.quote_sell_sol(pumpswap_state, tokens_in, exit_slip)
        else:
            sol_out, min_sol = pumpfun.quote_sell_sol(state, tokens_in, exit_slip)
        exit_sol = sol_out / LAMPORTS_PER_SOL
        exit_price_sol = sol_out / tokens_in / LAMPORTS_PER_SOL if tokens_in > 0 else 0

        exit_sig = None
        if trade_doc["mode"] == "live":
            # Last-line defence against Custom:6022 (SellZeroAmount): even if
            # the balance-read path above failed/skipped, refuse to build a
            # zero-amount sell IX. Booking $0 exit is strictly safer than
            # burning gas on a reverting tx.
            if tokens_in <= 0:
                logger.warning(
                    f"sell aborted for {mint}: tokens_in resolved to 0 "
                    f"after balance read — closing trade with zero PnL"
                )
                trade_doc["status"] = "closed"
                trade_doc["exit_reason"] = f"{reason} | zero token amount at exit"
                trade_doc["exit_time"] = now_utc().isoformat()
                trade_doc["exit_sol"] = 0.0
                trade_doc["exit_usd"] = 0.0
                trade_doc["exit_price_sol"] = 0.0
                await self.db.trades.update_one(
                    {"_id": trade_doc["id"]}, {"$set": trade_doc}, upsert=True
                )
                await hub.broadcast("trade_exit", {
                    "id": trade_doc["id"], "mint": mint,
                    "symbol": trade_doc.get("symbol"),
                    "reason": trade_doc["exit_reason"],
                })
                self.recent_exit_until[mint] = time.time() + 90.0
                return
            try:
                kp = get_keypair()
                user = get_pubkey()
                mint_pk = Pubkey.from_string(mint)
                # Build slip-escalation ladder. Attempt 0 = current exit_slip.
                # Attempts 1..N escalate to auto_exit_retry_slip_floors_bps if
                # the on-chain tx reverts with Custom:6003 (slippage).
                slip_ladder = [exit_slip]
                if self.config.intelligent_exit_v2:
                    for floor_bps in (self.config.auto_exit_retry_slip_floors_bps or []):
                        if int(floor_bps) > slip_ladder[-1]:
                            slip_ladder.append(int(floor_bps))
                last_err: Exception | None = None
                exit_sig = None
                for attempt_idx, attempt_slip in enumerate(slip_ladder):
                    # Recompute min_sol for THIS attempt's slip — both protocols
                    if protocol == "pumpswap":
                        _, attempt_min_sol = pumpswap.quote_sell_sol(pumpswap_state, tokens_in, attempt_slip)
                    else:
                        _, attempt_min_sol = pumpfun.quote_sell_sol(state, tokens_in, attempt_slip)
                    if protocol == "pumpswap":
                        # Token-2022 pools require explicit base_token_program +
                        # canonical user WSOL ATA (not seed-derived temp). Sells
                        # with a temp WSOL revert with Custom:6053 (seeds mismatch).
                        base_tp = await pumpfun.get_mint_token_program(mint)
                        user_token_ata = pumpswap.get_associated_token_address(user, mint_pk, base_tp)
                        wsol_ata, wsol_ixs = pumpswap.build_wsol_ata_idempotent_ixs(user)
                        ixs = [
                            pumpswap.build_create_ata_ix(user, user, mint_pk, base_tp),
                            *wsol_ixs,
                            pumpswap.build_sell_ix(
                                user, pumpswap_state, user_token_ata, wsol_ata,
                                base_amount_in=tokens_in,
                                min_quote_amount_out=attempt_min_sol,
                                base_token_program=base_tp,
                            ),
                            # Unwrap wSOL → native SOL in the user wallet.
                            pumpswap.build_close_wsol_ix(user, wsol_ata),
                        ]
                        try:
                            exit_sig = await pumpfun.send_versioned_tx(
                                kp, ixs, eff_priority,
                                compute_unit_limit=400_000,
                            )
                        except Exception as _se:
                            last_err = _se
                            if "Custom': 6003" in str(_se) and attempt_idx + 1 < len(slip_ladder):
                                logger.info(
                                    f"sell slip-retry {attempt_idx + 1}/{len(slip_ladder) - 1} "
                                    f"for {mint[:8]} after {attempt_slip}bps: escalating"
                                )
                                continue
                            raise
                    else:
                        creator_str = trade_doc.get("creator") or (slot.get("launch") or {}).get("creator") or ""
                        if state and state.get("creator"):
                            creator_str = state["creator"]
                        if not creator_str:
                            raise RuntimeError("missing creator for final-sell creator_vault PDA")
                        creator_pk = Pubkey.from_string(creator_str)
                        tp = await pumpfun.get_mint_token_program(mint)
                        is_cashback = bool((state or {}).get("is_cashback", False))
                        ix = await pumpfun.build_sell_ix(user, mint_pk, tokens_in, attempt_min_sol, creator_pk, tp, cashback=is_cashback)
                        try:
                            exit_sig = await pumpfun.send_versioned_tx(
                                kp, [ix], eff_priority
                            )
                        except Exception as _se:
                            last_err = _se
                            if "Custom': 6003" in str(_se) and attempt_idx + 1 < len(slip_ladder):
                                logger.info(
                                    f"sell slip-retry {attempt_idx + 1}/{len(slip_ladder) - 1} "
                                    f"for {mint[:8]} after {attempt_slip}bps: escalating"
                                )
                                continue
                            raise
                    # Success — record the slip that actually landed and bail
                    exit_slip = attempt_slip
                    break
                else:
                    # ladder exhausted — re-raise the final error
                    if last_err:
                        raise last_err
            except Exception as e:
                err_str = str(e)
                logger.exception(f"Live sell failed: {e}")
                trade_doc["exit_reason"] = f"{reason} | sell failed: {e}"
                # Custom:6005 (BondingCurveComplete) — the token graduated to
                # Raydium/PumpSwap mid-sell. Auto-fallback to PumpSwap AMM
                # right here so the user doesn't have to manually recover.
                if "Custom': 6005" in err_str or "'Custom': 6005" in err_str:
                    logger.warning(
                        f"6005 BondingCurveComplete on {mint[:8]}… — attempting "
                        f"emergency PumpSwap fallback in-place"
                    )
                    emergency = await self._attempt_emergency_pumpswap_sell(
                        mint=mint, kp=kp, user=user, mint_pk=mint_pk,
                        tokens_in=tokens_in,
                    )
                    if emergency is not None:
                        # Successful fallback — patch state so the rest of
                        # _exit_impl books PnL on the PumpSwap proceeds.
                        exit_sig, sol_out, pumpswap_state = emergency
                        protocol = "pumpswap"
                        exit_sol = sol_out / LAMPORTS_PER_SOL
                        exit_price_sol = sol_out / tokens_in / LAMPORTS_PER_SOL if tokens_in > 0 else 0
                        exit_slip = 5000  # for accounting
                        # Bump effective priority so the fee accounting is honest
                        eff_priority = max(eff_priority, 5_000_000)
                    else:
                        trade_doc["status"] = "exit_failed_terminal"
                        trade_doc["exit_time"] = now_utc().isoformat()
                        trade_doc["exit_reason"] = (
                            f"{reason} | bonding curve completed mid-sell AND "
                            f"emergency PumpSwap fallback failed — manual "
                            f"recovery needed (export privkey to recover)"
                        )
                        trade_doc["pnl_sol"] = 0.0
                        trade_doc["pnl_usd"] = 0.0
                        trade_doc["pnl_pct"] = 0.0
                        await self.db.trades.update_one(
                            {"id": trade_doc["id"]}, {"$set": trade_doc}, upsert=True
                        )
                        self.active_trades.pop(mint, None)
                        self.recent_exit_until[mint] = time.time() + 90.0
                        await hub.broadcast("trade_exit_terminal", trade_doc)
                        logger.warning(
                            f"GRADUATED+UNRECOVERABLE mint {mint[:8]}… — curve "
                            f"complete and PumpSwap fallback failed. Position "
                            f"terminal; user must recover with exported privkey."
                        )
                        return

        cu = CU_PUMPSWAP if protocol == "pumpswap" else CU_PUMPFUN
        exit_fee_sol = estimate_tx_fee_sol(eff_priority, cu)
        trade_doc["exit_fee_sol"] = exit_fee_sol

        # ----- PHANTOM-PNL GUARD -----
        # If we attempted a live sell but the tx never landed (exit_sig is None),
        # we still OWN the tokens. Booking a PnL based on the quoted exit price
        # would be a phantom — the wallet didn't actually receive that SOL.
        # Keep the position in active_trades and let the monitor retry.
        if trade_doc["mode"] == "live" and exit_sig is None:
            retries = int(trade_doc.get("exit_retries", 0)) + 1
            trade_doc["exit_retries"] = retries
            trade_doc["last_exit_attempt"] = now_utc().isoformat()
            trade_doc["last_exit_attempt_reason"] = reason
            # Gas IS gone whether or not the sell landed — track it
            trade_doc["exit_fee_sol_failed_attempts"] = (
                float(trade_doc.get("exit_fee_sol_failed_attempts") or 0.0) + exit_fee_sol
            )
            # If we've burned too many tries on this position, attempt one
            # emergency brute-force PumpSwap sell BEFORE giving up. Most
            # "stuck" positions are recoverable on PumpSwap with 50% slip
            # and a 5M µL priority fee — we just never tried that combo in
            # the slip-ladder. This is the difference between "stuck position
            # the user has to babysit" and "system always exits".
            if retries >= 3:
                kp_em = get_keypair()
                user_em = get_pubkey()
                mint_pk_em = Pubkey.from_string(mint)
                emergency = await self._attempt_emergency_pumpswap_sell(
                    mint=mint, kp=kp_em, user=user_em, mint_pk=mint_pk_em,
                    tokens_in=tokens_in,
                )
                if emergency is not None:
                    sig_em, sol_out_em, _ = emergency
                    # Book the emergency exit as a successful close
                    exit_sol_final = sol_out_em / LAMPORTS_PER_SOL
                    trade_doc["status"] = "closed"
                    trade_doc["exit_time"] = now_utc().isoformat()
                    trade_doc["exit_sig"] = sig_em
                    trade_doc["exit_sol"] = exit_sol_final
                    trade_doc["exit_usd"] = exit_sol_final * (sol_price or 100.0)
                    trade_doc["exit_price_sol"] = sol_out_em / tokens_in / LAMPORTS_PER_SOL if tokens_in > 0 else 0
                    trade_doc["exit_reason"] = (
                        f"{reason} | EMERGENCY pumpswap fallback after {retries} "
                        f"failed sells — recovered {exit_sol_final:.6f} SOL"
                    )
                    trade_doc["protocol"] = "pumpswap"
                    pnl_sol_em = exit_sol_final - trade_doc["entry_sol"]
                    partial_realized_sol = float(trade_doc.get("partial_realized_sol") or 0.0)
                    partial_realized_usd = float(trade_doc.get("partial_realized_usd") or 0.0)
                    trade_doc["pnl_sol"] = pnl_sol_em + partial_realized_sol
                    trade_doc["pnl_usd"] = trade_doc["pnl_sol"] * (sol_price or 100.0)
                    trade_doc["pnl_pct"] = (
                        (trade_doc["pnl_sol"] / trade_doc["entry_sol"]) * 100.0
                        if trade_doc.get("entry_sol") else 0.0
                    )
                    await self.db.trades.update_one(
                        {"id": trade_doc["id"]}, {"$set": trade_doc}, upsert=True
                    )
                    self.active_trades.pop(mint, None)
                    self.recent_exit_until[mint] = time.time() + 90.0
                    await hub.broadcast("trade_exit", trade_doc)
                    logger.warning(
                        f"RESCUED {mint[:8]}… via emergency pumpswap sell after "
                        f"{retries} normal-flow failures — pnl_sol={pnl_sol_em:+.6f}"
                    )
                    return
                # Emergency also failed — mark terminal as before
                trade_doc["status"] = "exit_failed_terminal"
                trade_doc["exit_time"] = now_utc().isoformat()
                trade_doc["exit_reason"] = f"GAVE UP after {retries} sell retries + emergency pumpswap failed — export privkey to recover manually: {reason}"
                trade_doc["pnl_sol"] = 0.0
                trade_doc["pnl_usd"] = 0.0
                trade_doc["pnl_pct"] = 0.0
                await self.db.trades.update_one(
                    {"id": trade_doc["id"]}, {"$set": trade_doc}, upsert=True
                )
                self.active_trades.pop(mint, None)
                # Block re-entry on this mint for the cooldown window — even
                # though the exit failed, the position is now considered
                # abandoned and the bot must NOT re-buy it (we still hold
                # stranded tokens that the user needs to recover manually).
                self.recent_exit_until[mint] = time.time() + 90.0
                await hub.broadcast("trade_exit_terminal", trade_doc)
                logger.warning(
                    f"GIVING UP on {mint} after {retries} failed sells + "
                    f"emergency pumpswap fallback — manual recovery required"
                )
            else:
                # Persist retry counter but keep position open
                trade_doc["status"] = "active"  # ensure DB shows the truth
                await self.db.trades.update_one(
                    {"id": trade_doc["id"]}, {"$set": trade_doc}, upsert=True
                )
                # CRITICAL: re-insert into active_trades since _exit pop'd it
                # at the top. Without this re-insert, the in-memory dict no
                # longer tracks the mint → scanner thinks the slot is free →
                # opens DUPLICATE position with a new trade.id → DB ends up
                # with N "active" rows for the same mint, each with its own
                # phantom monitor. This was the root cause of the 36-active /
                # 12-in-memory desync.
                self.active_trades[mint] = slot
                await hub.broadcast("trade_exit_failed", {
                    "mint": mint, "symbol": trade_doc.get("symbol"),
                    "retries": retries, "reason": reason,
                })
                logger.warning(
                    f"sell failed for {mint} (retry {retries}/3) — keeping "
                    f"position active for monitor retry"
                )
            return

        pnl_sol = exit_sol - trade_doc["entry_sol"]
        # Combine with any earlier partial-TP realised PnL so the trade's
        # reported total reflects both legs.
        partial_realized_sol = float(trade_doc.get("partial_realized_sol") or 0.0)
        partial_realized_usd = float(trade_doc.get("partial_realized_usd") or 0.0)
        # Subtract gas fees so the displayed PnL matches actual wallet movement.
        # The on-chain reconciler will refine this shortly with real deltas,
        # but the initial display should already be fee-net to avoid confusion.
        entry_fee_sol = float(trade_doc.get("entry_fee_sol") or 0.0)
        partial_fee_sol = float(trade_doc.get("partial_fee_sol") or 0.0)
        fees_total_sol = entry_fee_sol + exit_fee_sol + partial_fee_sol
        total_pnl_sol = pnl_sol + partial_realized_sol - fees_total_sol
        total_pnl_usd = (
            (pnl_sol * sol_price)
            + partial_realized_usd
            - (fees_total_sol * sol_price)
        )
        # PnL % is over the ORIGINAL cost basis so it stays comparable to
        # non-partial trades. We reconstruct original cost = current entry_sol +
        # partial cost basis (= partial_sell_sol - partial_realized_sol).
        orig_cost_sol = (
            trade_doc["entry_sol"]
            + (float(trade_doc.get("partial_sell_sol") or 0.0) - partial_realized_sol)
        )
        pnl_pct = (total_pnl_sol / orig_cost_sol * 100) if orig_cost_sol > 0 else 0

        trade_doc.update(
            {
                "status": "closed",
                "exit_time": now_utc().isoformat(),
                "exit_sol": exit_sol,
                "exit_usd": exit_sol * sol_price,
                "exit_price_sol": exit_price_sol,
                "exit_sig": exit_sig,
                "exit_reason": reason,
                "pnl_sol": total_pnl_sol,
                "pnl_usd": total_pnl_usd,
                "pnl_pct": pnl_pct,
            }
        )
        await self.db.trades.update_one(
            {"_id": trade_doc["id"]}, {"$set": trade_doc}, upsert=True
        )
        # Phase 2.9 — grey-out the pinned launch card (but DON'T remove the
        # pin; user manually unpins when done watching). The card stays at
        # the top of its feed but renders dimmed so the user knows the bot
        # is no longer in the position. If the launch was never pinned the
        # update is a no-op on a non-existent doc — cheap.
        try:
            exit_pnl_pct = trade_doc.get("pnl_pct")
            await self.db.launches.update_one(
                {"mint": mint, "pinned": True, "pin_exited": False},
                {"$set": {
                    "pin_exited": True,
                    "pin_exited_at": datetime.now(timezone.utc).isoformat(),
                    "exit_pnl_pct": exit_pnl_pct,
                    "exit_reason": reason,
                }},
            )
            # In-memory mirror so the scanner UI flips immediately without
            # waiting for the next /api/launches poll.
            for r in self.recent_launches:
                if r.get("mint") == mint and r.get("pinned") and not r.get("pin_exited"):
                    r["pin_exited"] = True
                    r["pin_exited_at"] = datetime.now(timezone.utc).isoformat()
                    r["exit_pnl_pct"] = exit_pnl_pct
                    r["exit_reason"] = reason
                    break
        except Exception as e:
            logger.debug(f"pin-exit update skipped: {e}")
        # Universal post-exit cooldown — block re-entry on this mint for 90s
        # regardless of exit reason. This prevents the "4 positions in 3 min"
        # pattern where the scanner immediately re-bought the mint we just
        # sold, orphaning the monitor task on the prior slot. The cooldown
        # gives the in-memory state (and the prior monitor) time to fully
        # tear down before any new entry can race a stale exit.
        self.recent_exit_until[mint] = time.time() + 90.0
        # SL cooldown — if this exit was triggered by stop-loss, lock the
        # mint out of new entries for `sl_cooldown_minutes`. The check applies
        # to fresh scanner entries AND the re-entry watcher. Buying back a
        # mint immediately after SL is statistically the worst time — momentum
        # has just reversed.
        if reason.lower().startswith("stop-loss hit"):
            cd_min = float(self.config.sl_cooldown_minutes)
            if cd_min > 0:
                self.sl_cooldown_until[mint] = time.time() + cd_min * 60.0
                logger.info(
                    f"SL cooldown set for {trade_doc.get('symbol','?')} "
                    f"({mint[:8]}…) — locked out for {cd_min:.1f} min"
                )
        # === Creator-greylist instrumentation ===
        # Compute rug metrics at close time so the greylist scorer has the
        # data it needs WITHOUT a separate analytics pipeline. Only meaningful
        # for losing trades (positive PnL = winner archetype, not rug-snipe).
        try:
            entry_p = float(trade_doc.get("entry_price_sol") or 0)
            exit_p = float(trade_doc.get("exit_price_sol") or 0)
            peak_sol = float(slot.get("peak_price_sol") or entry_p)
            if entry_p > 0 and peak_sol > entry_p:
                peak_pct_pre_rug = (peak_sol - entry_p) / entry_p * 100.0
                trade_doc["peak_pct_pre_rug"] = round(peak_pct_pre_rug, 2)
                if exit_p > 0 and exit_p < peak_sol:
                    rug_pct = (peak_sol - exit_p) / peak_sol * 100.0
                    trade_doc["rug_pct_from_peak"] = round(rug_pct, 2)
            entry_ts_iso = trade_doc.get("entry_time")
            launch_doc = await self.db.launches.find_one(
                {"mint": mint}, {"_id": 0, "first_seen": 1},
            )
            if entry_ts_iso and launch_doc and launch_doc.get("first_seen"):
                from datetime import datetime as _dt
                t_entry = _dt.fromisoformat(entry_ts_iso.replace("Z", "+00:00")).timestamp()
                t_first = _dt.fromisoformat(launch_doc["first_seen"].replace("Z", "+00:00")).timestamp()
                trade_doc["rug_seconds_from_launch"] = max(0, int(t_entry - t_first))
            await self.db.trades.update_one(
                {"id": trade_doc["id"]},
                {"$set": {k: trade_doc[k] for k in
                          ("peak_pct_pre_rug", "rug_pct_from_peak", "rug_seconds_from_launch")
                          if k in trade_doc}},
            )
        except Exception as e:
            logger.debug(f"greylist instrumentation skipped for {mint[:8]}…: {e}")
        try:
            from creator_greylist import update_creator_score
            await update_creator_score(
                self.db, trade_doc.get("creator"),
                min_fails=int(self.config.creator_greylist_min_fails),
                max_fails=int(self.config.creator_greylist_max_fails),
                tp_buffer=float(self.config.pattern_tp_buffer_pct),
            )
        except Exception as e:
            logger.debug(f"greylist update skipped: {e}")
        await hub.broadcast("trade_exit", trade_doc)
        # Re-entry watchlist: if we exited profitably and curve hasn't graduated, watch for a pullback.
        # During a graceful stop we don't queue any new re-entries — the user
        # is winding down, the watchlist would just open another position.
        if (
            self.config.reentry_enabled
            and not self.stopping_gracefully
            and total_pnl_sol > 0
            and not state.get("complete", False)
        ):
            self.reentry_watch[mint] = {
                "mint": mint,
                "name": trade_doc.get("name"),
                "symbol": trade_doc.get("symbol"),
                "exit_price_sol": exit_price_sol,
                "exit_time": time.time(),
                "attempts": 0,
                "max_attempts": self.config.reentry_max_attempts,
                "window_s": self.config.reentry_window_seconds,
                "pullback_pct": self.config.reentry_pullback_pct,
                "size_multiplier": self.config.reentry_size_multiplier,
                "original_pnl_usd": total_pnl_usd,
                "peak_price_after_exit": exit_price_sol,
                "creator": (slot.get("launch") or {}).get("creator"),
            }
            await hub.broadcast("reentry_watch_add", self.reentry_watch[mint])
        await self.check_kill_switch()
        # If a graceful stop is in progress, the finaliser watches active_trades
        # and will flip enabled=False on its own. Wake it eagerly so the UI
        # transitions from "Stopping (N positions)…" → "Stopped" without
        # waiting for the next 2s tick.
        if self.stopping_gracefully and not self.active_trades:
            await self._finalise_graceful_stop()
