"""
PnL reconciliation against on-chain truth.

The bot computes pnl_usd from quoted pool prices + slippage tolerance. That's
an *estimate* — real fills can differ due to:
  - Actual slippage in the pool at tx land time vs quote time
  - Tx fees (base sig + priority compute)
  - Tx failure (gas paid, no balance change on the trade itself)

This module periodically replays each closed live trade against the chain:
  - Fetches getTransaction for entry / exit / partial signatures
  - Reads the user's wallet pre/post lamport balance from tx meta
  - Computes `real_pnl_sol = real_exit_received + real_partial_received + real_entry_cost`
    where entry_cost is NEGATIVE (lamports spent)
  - Overwrites `pnl_sol` / `pnl_usd` / `pnl_pct` in place so daily_pnl_usd
    matches actual wallet movement.

Trades marked as reconciled (`pnl_reconciled=True`) are skipped on future
passes.
"""
import asyncio
import logging
import os
from datetime import datetime, timedelta, timezone

from solana_client import (
    LAMPORTS_PER_SOL,
    get_sol_usd_price,
    get_tx_wallet_delta_lamports,
)

logger = logging.getLogger("pnl_reconciler")

POLL_S = 30.0
# Look back this far for un-reconciled live trades
LOOKBACK_MIN = 60
# Per-pass throttle so we don't slam the RPC
MAX_TRADES_PER_PASS = 25


class PnLReconciler:
    def __init__(self, state):
        self.state = state
        self._task: asyncio.Task | None = None

    def start(self):
        if self._task is None or self._task.done():
            self._task = asyncio.create_task(self._loop())

    async def _loop(self):
        # Stagger first run so other startup tasks settle
        await asyncio.sleep(15.0)
        while True:
            try:
                await self.reconcile_pass()
            except asyncio.CancelledError:
                raise
            except Exception as e:
                logger.exception(f"reconcile pass error: {e}")
            await asyncio.sleep(POLL_S)

    async def reconcile_pass(self):
        cutoff = (datetime.now(timezone.utc) - timedelta(minutes=LOOKBACK_MIN)).isoformat()
        cursor = self.state.db.trades.find(
            {
                "mode": "live",
                "status": "closed",
                "exit_time": {"$gte": cutoff},
                "$or": [
                    {"pnl_reconciled": {"$exists": False}},
                    {"pnl_reconciled": False},
                ],
            },
            {
                "_id": 0,
                "id": 1, "entry_sig": 1, "exit_sig": 1, "partial_sig": 1,
                "entry_sol": 1, "exit_sol": 1, "partial_sell_sol": 1,
                "partial_realized_sol": 1, "pnl_sol": 1, "pnl_usd": 1,
            },
        ).limit(MAX_TRADES_PER_PASS)
        wallet = os.environ.get("WALLET_PUBKEY") or ""
        if not wallet:
            # Fall back: derive from secret. The bot has a helper; import lazily.
            try:
                from wallet import get_pubkey
                wallet = str(get_pubkey())
            except Exception:
                logger.warning("WALLET_PUBKEY not set — can't reconcile")
                return
        sol_price = await get_sol_usd_price()
        n_done = 0
        async for t in cursor:
            try:
                await self._reconcile_one(t, wallet, sol_price)
                n_done += 1
            except Exception as e:
                logger.warning(f"reconcile failed for {t.get('id')}: {e}")
        if n_done:
            logger.info(f"reconciled {n_done} live trades against chain")

    async def _reconcile_one(self, t: dict, wallet: str, sol_price: float):
        """Read wallet deltas for entry/partial/exit sigs and overwrite pnl
        with reality. Only the SOL movement caused by the trade leg matters;
        we explicitly DON'T add the fee back in, because what the user cares
        about is "how much did my wallet change" — which is fee-inclusive."""
        entry_sig = t.get("entry_sig")
        exit_sig = t.get("exit_sig")
        partial_sig = t.get("partial_sig")

        # Need at least an entry signature to be worth reconciling. If no
        # exit_sig (sell failed), the position is still open in chain terms —
        # we can still measure the entry leg's real cost but PnL is unrealised.
        if not entry_sig:
            return

        entry_delta = await get_tx_wallet_delta_lamports(entry_sig, wallet)
        if entry_delta is None:
            return  # tx not yet indexed by RPC

        exit_delta = 0
        if exit_sig:
            ex = await get_tx_wallet_delta_lamports(exit_sig, wallet)
            if ex is None:
                return
            exit_delta = ex

        partial_delta = 0
        if partial_sig:
            pd = await get_tx_wallet_delta_lamports(partial_sig, wallet)
            if pd is None:
                return
            partial_delta = pd

        # entry_delta is negative (lamports left wallet). exit/partial positive.
        net_lamports = entry_delta + partial_delta + exit_delta
        real_pnl_sol = net_lamports / LAMPORTS_PER_SOL
        real_pnl_usd = real_pnl_sol * sol_price
        # PnL % off the original buy cost (abs of entry_delta)
        cost_sol = abs(entry_delta) / LAMPORTS_PER_SOL if entry_delta else 0
        real_pnl_pct = (real_pnl_sol / cost_sol * 100) if cost_sol > 0 else 0.0

        # Ghost-position guard: if the BUY tx only debited the signature fee
        # (|entry_delta| < ~200k lamports = 0.0002 SOL), the on-chain buy
        # actually failed (Custom:XXXX / IncorrectProgramId / etc.) but the
        # bot's send_versioned_tx mis-detected it as success. Don't pollute
        # PnL stats with "-300%" gas-on-gas rows — flag explicitly.
        # Real entries cost >= ~$0.04 worth of SOL even at $0.50 trade sizes.
        is_ghost_entry = abs(entry_delta) < 200_000

        # Recompute `entry_price_sol` from REAL on-chain cost. The original
        # value stored at entry uses quoted sol_in/tokens_out (audit #3) —
        # off by ~0.5-2% due to priority fee + slippage drift. Backfilling
        # here means any downstream UI / dashboard read sees the price the
        # wallet actually paid, not the optimistic pre-trade quote.
        entry_tokens = int(t.get("entry_tokens") or 0)
        real_entry_price = None
        if entry_tokens > 0 and not is_ghost_entry:
            real_entry_price = (abs(entry_delta) / LAMPORTS_PER_SOL) / entry_tokens

        update = {
            "real_entry_cost_sol": abs(entry_delta) / LAMPORTS_PER_SOL,
            "real_exit_received_sol": exit_delta / LAMPORTS_PER_SOL,
            "real_partial_received_sol": partial_delta / LAMPORTS_PER_SOL,
            "real_pnl_sol": real_pnl_sol,
            "real_pnl_usd": real_pnl_usd,
            "real_pnl_pct": real_pnl_pct,
            # Overwrite the displayed PnL with reality
            "pnl_sol": real_pnl_sol,
            "pnl_usd": real_pnl_usd,
            "pnl_pct": real_pnl_pct,
            "pnl_reconciled": True,
            "pnl_reconciled_at": datetime.now(timezone.utc).isoformat(),
        }
        if real_entry_price is not None:
            update["real_entry_price_sol"] = real_entry_price
            update["entry_price_sol"] = real_entry_price  # update displayed
        if is_ghost_entry:
            update["ghost_entry"] = True
            update["exit_reason"] = "ghost: buy tx landed but failed on-chain"
            # Override the misleading negative PnL pct (was -300% etc.)
            # with a flag that downstream UIs / dashboards can show distinctly.
            update["pnl_pct"] = 0.0
            update["real_pnl_pct"] = 0.0
        await self.state.db.trades.update_one(
            {"id": t["id"]}, {"$set": update}
        )
