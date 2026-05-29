"""
Pump.fun token discovery — bring already-existing tokens into the scanner.

The mempool listener only sees tokens created AFTER the bot started.
This module polls Pump.fun's public coins API every ~2min, finds tokens
in the [now - max_age, now - min_age] window, and seeds them into
BotState.tracking. The existing scanner gates then evaluate them
just like organically-observed launches.

Once seeded, live trades for those mints flow through the existing
Helius logsSubscribe listener automatically (it subscribes to the whole
Pump program, not per-mint).
"""
from __future__ import annotations

import asyncio
import logging
import time
from collections import deque
from typing import TYPE_CHECKING

import httpx

from models import Launch
from solana_client import LAMPORTS_PER_SOL
from pumpfun import LAUNCH_BASELINE_PRICE_SOL
import pumpswap
from ws_hub import hub

if TYPE_CHECKING:
    from bot import BotState

logger = logging.getLogger("discovery")

PUMPFUN_API = "https://frontend-api-v3.pump.fun"
DISCOVERY_INTERVAL_S = 120
REFRESH_INTERVAL_S = 60      # how often to re-poll MC for already-tracked discovered tokens
MC_SAMPLE_KEEP = 12          # 12 × 60s = 12min of MC samples for velocity calc
COINS_PER_CYCLE = 50
HTTP_TIMEOUT = 12.0


class PumpfunDiscovery:
    def __init__(self, state: "BotState"):
        self.state = state
        self._task: asyncio.Task | None = None
        self._refresh_task: asyncio.Task | None = None

    def start(self):
        if self._task is None or self._task.done():
            self._task = asyncio.create_task(self._loop())
        if self._refresh_task is None or self._refresh_task.done():
            self._refresh_task = asyncio.create_task(self._refresh_loop())

    async def _loop(self):
        # Stagger first run by 5s so listener has time to settle
        await asyncio.sleep(5.0)
        while True:
            try:
                await self.run_once()
            except asyncio.CancelledError:
                raise
            except Exception as e:
                logger.warning(f"discovery loop error: {e}")
            await asyncio.sleep(DISCOVERY_INTERVAL_S)

    async def _refresh_loop(self):
        """Re-poll Pump.fun's /coins API every REFRESH_INTERVAL_S to update
        live signals for already-tracked discovered tokens — MC, last_trade,
        and price (via virtual reserves for bonding curve, pool reserves for
        PumpSwap). Appends to rolling `mc_samples` and `price_samples` deques
        so the seasoned-band velocity gates have real data to work with.

        Without this loop, `mc_samples` was always empty → MC velocity always
        computed as 0% → seasoned tokens never passed the velocity gate.
        """
        await asyncio.sleep(REFRESH_INTERVAL_S)
        while True:
            try:
                await self._refresh_once()
            except asyncio.CancelledError:
                raise
            except Exception as e:
                logger.warning(f"discovery refresh loop error: {e}")
            await asyncio.sleep(REFRESH_INTERVAL_S)

    async def _refresh_once(self):
        """Iterate tracked discovered mints, pull fresh MC + price, append
        samples. Batches Pump.fun API calls (per-mint endpoint) up to 25 at a
        time with throttling to stay polite."""
        st = self.state
        # Snapshot mint list — discovered tokens only (mempool-tracked mints
        # already get price samples via on_trade, no refresh needed)
        targets = [
            (mint, b) for mint, b in st.tracking.items()
            if b.get("discovered") and (mint not in st.active_trades)
        ]
        if not targets:
            return
        now = time.time()
        url = f"{PUMPFUN_API}/coins"
        async with httpx.AsyncClient(timeout=HTTP_TIMEOUT) as client:
            for mint, bucket in targets:
                try:
                    r = await client.get(f"{url}/{mint}", headers={"accept": "application/json"})
                    if r.status_code != 200:
                        continue
                    c = r.json() or {}
                except Exception as e:
                    logger.debug(f"refresh fetch failed for {mint}: {e}")
                    continue
                usd_mc = float(c.get("usd_market_cap") or 0.0)
                last_trade_ms = int(c.get("last_trade_timestamp") or 0)
                is_graduated = bool(c.get("complete"))
                # Resolve current price per protocol
                cur_price = 0.0
                if is_graduated:
                    pool = bucket.get("pumpswap_pool") or c.get("pump_swap_pool") or ""
                    if pool:
                        try:
                            ps_state = await pumpswap.fetch_pool_state(pool)
                            if ps_state:
                                cur_price = pumpswap.price_sol_per_raw_token(ps_state)
                                # PumpSwap quote_reserves IS the actual WSOL
                                # in the pool — no virtual offset like Pump.fun.
                                # Store as real_sol (not virtual) so the scanner
                                # band gate sees the correct liquidity.
                                bucket["last_real_sol_lamports"] = ps_state["quote_reserves"]
                                bucket["last_vsr_lamports"] = ps_state["quote_reserves"]  # legacy compat
                        except Exception as e:
                            logger.debug(f"refresh pool fetch failed for {mint}: {e}")
                else:
                    vsr = int(c.get("virtual_sol_reserves") or 0)
                    vtr = int(c.get("virtual_token_reserves") or 0)
                    real_sol = int(c.get("real_sol_reserves") or 0)
                    if vsr and vtr:
                        cur_price = vsr / vtr / LAMPORTS_PER_SOL
                        bucket["last_vsr_lamports"] = vsr
                        # Pump.fun returns real_sol_reserves directly in the
                        # coin doc — prefer it over the legacy vsr-30 estimate.
                        if real_sol > 0:
                            bucket["last_real_sol_lamports"] = real_sol
                # Update bucket
                bucket["usd_market_cap"] = usd_mc
                bucket["last_trade_ms"] = last_trade_ms
                # Refresh social proof fields (creator can add twitter/telegram
                # later, reply_count climbs over time)
                bucket["reply_count"] = int(c.get("reply_count") or 0)
                bucket["twitter"] = (c.get("twitter") or "").strip()
                bucket["telegram"] = (c.get("telegram") or "").strip()
                bucket["website"] = (c.get("website") or "").strip()
                # Cumulative buyer count from the Pump.fun coin endpoint.
                # PumpSwap pools don't generate Helius mempool events, so the
                # in-memory `buyers` set stays empty — `buy_count` is the only
                # signal we have for "how many people are buying this thing"
                # on seasoned/graduated tokens. Used by min_buyers_for_entry.
                bucket["buy_count"] = int(c.get("buy_count") or 0)
                if cur_price > 0:
                    bucket["last_price_sol"] = cur_price
                # Append rolling MC sample
                mc_samples = bucket.setdefault("mc_samples", deque(maxlen=MC_SAMPLE_KEEP))
                mc_samples.append((now, usd_mc))
                # Append rolling price sample (used by the entry-velocity gate)
                if cur_price > 0:
                    price_samples = bucket.setdefault("price_samples", deque(maxlen=120))
                    price_samples.append((now, cur_price))
                # Be polite to Pump.fun's per-mint endpoint
                await asyncio.sleep(0.15)
        logger.debug(f"discovery refresh: updated {len(targets)} tokens")

    async def run_once(self) -> int:
        """Returns the number of newly seeded tokens."""
        st = self.state
        cfg = st.config
        max_age_s = cfg.scanner_window_hours * 3600
        min_age_s = cfg.scanner_min_age_minutes * 60
        if max_age_s <= min_age_s:
            return 0
        now = time.time()
        lo_ts = now - max_age_s   # earliest creation time we care about
        hi_ts = now - min_age_s   # latest creation time (must be at least min_age old)

        coins = await self._fetch_aged_coins(lo_ts, hi_ts)
        if not coins:
            logger.info(f"discovery: 0 coins in band [{min_age_s/60:.0f}m, {max_age_s/60:.0f}m]")
            return 0

        seeded = 0
        skipped_idle = 0
        max_idle_ms = cfg.scanner_discovery_max_idle_minutes * 60 * 1000
        now_ms = now * 1000
        for c in coins:
            if seeded >= COINS_PER_CYCLE:
                break
            mint = c.get("mint")
            if not mint:
                continue
            if mint in st.tracking or mint in st.active_trades or mint in st.entered_mints:
                continue
            created_ms = c.get("created_timestamp") or 0
            created_s = created_ms / 1000.0
            # _fetch_aged_coins already filtered to the band, but double-check
            if not (lo_ts <= created_s <= hi_ts):
                continue
            # Graduated tokens trade on PumpSwap AMM. We still want them — they're
            # often the biggest movers — so just tag the protocol.
            is_pumpswap = bool(c.get("complete"))
            # Freshness gate: skip tokens whose last trade is too stale.
            # IMPORTANT: graduated tokens trade on PumpSwap AMM, and Pump.fun's
            # `last_trade_timestamp` only tracks bonding-curve trades — it goes
            # stale the moment the token graduates. Skip the gate for those so
            # we don't systematically exclude all high-MC graduated movers.
            if not is_pumpswap and max_idle_ms > 0:
                last_trade_ms = c.get("last_trade_timestamp") or 0
                if not last_trade_ms or now_ms - last_trade_ms > max_idle_ms:
                    skipped_idle += 1
                    continue
            try:
                await self._seed_token(c, created_s, is_pumpswap)
                seeded += 1
            except Exception as e:
                logger.debug(f"seed failed for {mint}: {e}")

        logger.info(f"discovery: {len(coins)} in band, seeded {seeded}, skipped_idle {skipped_idle}")
        if seeded:
            await hub.broadcast("discovery", {"seeded": seeded, "ts": now})
        return seeded

    async def _fetch_aged_coins(self, lo_ts: float, hi_ts: float) -> list[dict]:
        """Pull tokens via TWO sort orders and merge:
          1. `last_trade_timestamp DESC` — surfaces actively-traded tokens
             (covers most NEW band candidates).
          2. `market_cap DESC`           — surfaces high-MC and graduated
             tokens whose bonding-curve `last_trade_timestamp` has gone stale
             since they moved to the PumpSwap AMM.
        Filtered to the [lo_ts, hi_ts] creation-time band. Sorting via two
        orders bypasses Pump.fun's ~1000-offset creation-pagination cap and
        ensures the seasoned band sees both active movers AND big-cap names."""
        url = f"{PUMPFUN_API}/coins"
        PAGE_SIZE = 240
        MAX_PAGES_PER_SORT = 5  # 5×240 = ~1200 tokens per sort order
        out: list[dict] = []
        seen: set[str] = set()
        sort_orders = [
            ("last_trade_timestamp", "DESC"),
            ("market_cap", "DESC"),
        ]
        async with httpx.AsyncClient(timeout=HTTP_TIMEOUT) as client:
            for sort_field, order in sort_orders:
                offset = 0
                for _ in range(MAX_PAGES_PER_SORT):
                    params = {
                        "offset": offset,
                        "limit": PAGE_SIZE,
                        "sort": sort_field,
                        "order": order,
                        "includeNsfw": "true",
                    }
                    try:
                        r = await client.get(url, params=params, headers={"accept": "application/json"})
                        r.raise_for_status()
                        page = r.json()
                        if isinstance(page, dict):
                            page = page.get("data") or page.get("coins") or []
                        if not isinstance(page, list) or not page:
                            break
                    except Exception as e:
                        logger.warning(f"pumpfun coins API page sort={sort_field} offset={offset} failed: {e}")
                        break
                    for c in page:
                        mint = c.get("mint")
                        if not mint or mint in seen:
                            continue
                        seen.add(mint)
                        ts_s = (c.get("created_timestamp") or 0) / 1000.0
                        if lo_ts <= ts_s <= hi_ts:
                            out.append(c)
                    offset += PAGE_SIZE
                    # Be a polite client between pages
                    await asyncio.sleep(0.25)
        return out

    async def _seed_token(self, coin: dict, created_s: float, is_pumpswap: bool = False):
        st = self.state
        mint = coin["mint"]
        vsr = int(coin.get("virtual_sol_reserves") or 0)
        vtr = int(coin.get("virtual_token_reserves") or 0)
        usd_mc = float(coin.get("usd_market_cap") or 0.0)
        last_trade_ms = int(coin.get("last_trade_timestamp") or 0)
        pool_address = coin.get("pump_swap_pool") or coin.get("pool_address") or ""

        # Resolve current price per protocol
        if is_pumpswap:
            # Pump.fun API may carry the pool address directly; fall back to
            # finding it via on-chain lookup.
            if not pool_address:
                try:
                    found = await pumpswap.find_pool_for_mint(mint)
                    if found:
                        pool_address = found
                except Exception:
                    pool_address = ""
            cur_price = 0.0
            real_sol_lamports = 0
            if pool_address:
                try:
                    ps_state = await pumpswap.fetch_pool_state(pool_address)
                    if ps_state:
                        cur_price = pumpswap.price_sol_per_raw_token(ps_state)
                        real_sol_lamports = ps_state["quote_reserves"]
                except Exception as e:
                    logger.debug(f"pumpswap pool fetch failed for {mint}: {e}")
        else:
            cur_price = (vsr / vtr / LAMPORTS_PER_SOL) if (vsr and vtr) else 0.0
            # Pump.fun API exposes real_sol_reserves directly; prefer it over
            # the vsr-based estimate so the band gate sees the true SOL pool.
            real_sol_lamports = int(coin.get("real_sol_reserves") or 0) or vsr

        bucket = {
            "launch_id": f"disc-{mint[:8]}",
            "creator": coin.get("creator") or "",
            "start": created_s,
            "buyers": set(),
            "buy_events": deque(maxlen=500),
            "sol_inflow_lamports": 0,
            # Initial cumulative buy count from Pump.fun API. Refreshed every
            # discovery poll. Used by `min_buyers_for_entry` seasoned-band gate.
            "buy_count": int(coin.get("buy_count") or 0),
            "curve_fill_pct": (100.0 if is_pumpswap else
                               (min(100.0, max(0.0, (vsr - 30_000_000_000) / 85_000_000_000 * 100)) if vsr else 0.0)),
            "social_score": 0,
            "social_sources": {},
            "last_persist": 0.0,
            "name": coin.get("name"),
            "symbol": coin.get("symbol"),
            "creator_rugs": 0,
            # Anchor "first seen" at the universal Pump launch baseline so that
            # growth_pct reflects true chart growth from launch.
            "first_seen_price_sol": LAUNCH_BASELINE_PRICE_SOL,
            "last_price_sol": cur_price,
            # Authoritative real-SOL liquidity (no virtual offset). The
            # scanner band gate reads this directly; falls back to
            # last_vsr_lamports-30 only if missing.
            "last_real_sol_lamports": real_sol_lamports,
            "last_vsr_lamports": real_sol_lamports,
            # Throttled price samples for entry-velocity gate. Discovered
            # tokens populate this via the discovery refresh loop (not the
            # mempool listener, which doesn't reach PumpSwap pools).
            "price_samples": deque(maxlen=120),
            "last_price_sample_ts": 0.0,
            "scanner_eligible": True,
            "scanner_last_attempt": 0.0,
            "discovered": True,
            "usd_market_cap": usd_mc,
            "last_trade_ms": last_trade_ms,
            "protocol": "pumpswap" if is_pumpswap else "pumpfun",
            "pumpswap_pool": pool_address if is_pumpswap else "",
            # Social proof fields (used by gate_socials_required entry gate)
            "reply_count": int(coin.get("reply_count") or 0),
            "twitter": (coin.get("twitter") or "").strip(),
            "telegram": (coin.get("telegram") or "").strip(),
            "website": (coin.get("website") or "").strip(),
        }
        st.tracking[mint] = bucket
        # Also push a synthetic launch into the recent feed so the UI shows it
        synthetic = Launch(
            mint=mint,
            creator=bucket["creator"],
            bonding_curve=coin.get("bonding_curve") or "",
            name=bucket["name"],
            symbol=bucket["symbol"],
        )
        synthetic.id = bucket["launch_id"]
        synthetic.classifier_action = "discovered"
        doc = synthetic.model_dump()
        doc["detected_at"] = doc["detected_at"].isoformat()
        doc["discovered"] = True
        st.recent_launches.insert(0, doc)
        st.recent_launches = st.recent_launches[:50]
        await hub.broadcast("launch", doc)
