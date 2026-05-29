"""
Pool Discovery Service

Proactively discovers and caches pool addresses for tokens.
Avoids expensive lookups at exit time.

Key features:
  - Background polling for token state changes
  - Caches bonding curves AND pumpswap pools
  - Detects graduation and updates cache
  - Batch discovery for efficiency
"""
import asyncio
import logging
from datetime import datetime, timezone
from typing import Dict, Any, Optional, List

logger = logging.getLogger("pool_discovery")


class PoolDiscoveryService:
    """Background service for pool discovery and caching"""

    def __init__(self, supabase_client, state_machine):
        self.supabase = supabase
        self.state_machine = state_machine
        self._running = False
        self._task: Optional[asyncio.Task] = None

    def start(self):
        """Start background polling"""
        if self._running:
            return

        self._running = True
        self._task = asyncio.create_task(self._poll_loop())
        logger.info("Pool discovery service started")

    def stop(self):
        """Stop background polling"""
        self._running = False
        if self._task:
            self._task.cancel()
        logger.info("Pool discovery service stopped")

    async def _poll_loop(self):
        """Main polling loop"""

        # Stagger start
        await asyncio.sleep(10)

        while self._running:
            try:
                # Discover pools for tracked positions
                await self._discover_active_position_pools()

                # Discover pools for recently traded tokens
                await self._discover_recent_mint_pools()

                # Clean up stale cache entries
                await self._cleanup_stale_cache()

            except asyncio.CancelledError:
                raise
            except Exception as e:
                logger.exception(f"pool discovery error: {e}")

            # Poll every 30 seconds
            await asyncio.sleep(30)

    async def _discover_active_position_pools(self):
        """Ensure active positions have cached pools"""

        # Get active positions
        result = await self.supabase.table("positions").select(
            "id,mint,current_protocol,pumpfun_bonding_curve,pumpswap_pool"
        ).in_("status", ["active", "graduating"]).execute()

        positions = result.data
        if not positions:
            return

        logger.info(f"Checking {len(positions)} active positions for pool cache")

        for pos in positions:
            mint = pos["mint"]
            current_protocol = pos["current_protocol"]

            # Check if we have the pool address cached
            if current_protocol == "pumpfun" and not pos.get("pumpfun_bonding_curve"):
                # Need to discover bonding curve
                await self._discover_bonding_curve(mint)

            elif current_protocol == "pumpswap" and not pos.get("pumpswap_pool"):
                # Need to discover pool
                await self._discover_pumpswap_pool(mint)

    async def _discover_recent_mint_pools(self):
        """Cache pools for recently traded tokens"""

        # Get mints from recent positions (last 24h)
        cutoff = datetime.now(timezone.utc).replace(
            hour=0, minute=0, second=0, microsecond=0
        ).isoformat()

        result = await self.supabase.table("positions").select(
            "mint"
        ).gte("created_at", cutoff).execute()

        if not result.data:
            return

        mints = [p["mint"] for p in result.data]
        unique_mints = list(set(mints))

        logger.info(f"Caching pools for {len(unique_mints)} recent mints")

        for mint in unique_mints:
            # Check if already cached
            cached = await self.state_machine.get_pool_address(mint)
            if cached:
                continue

            # Discover
            await self._discover_full_pool_info(mint)

    async def _discover_bonding_curve(self, mint: str) -> Optional[str]:
        """Discover bonding curve address for a mint"""

        try:
            # Import here to avoid circular dependency
            import sys
            import os
            sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))
            import pumpfun

            # Derive bonding curve PDA
            from solders.pubkey import Pubkey
            mint_pk = Pubkey.from_string(mint)
            bonding_curve = str(pumpfun.derive_bonding_curve(mint_pk))

            # Fetch state
            state = await pumpfun.fetch_bonding_curve_state(mint)

            if state:
                # Check if already graduated
                is_graduated = state.get("complete", False)

                await self.state_machine.cache_pool_info(
                    mint=mint,
                    bonding_curve=bonding_curve,
                    is_graduated=is_graduated,
                    virtual_sol_reserves=state.get("virtual_sol_reserves"),
                    discovered_via="proactive_discovery",
                )

                logger.info(
                    f"Cached bonding curve: mint={mint[:8]}... | "
                    f"curve={bonding_curve[:8]}... | graduated={is_graduated}"
                )

                # If graduated, also discover pumpswap pool
                if is_graduated:
                    await self._discover_pumpswap_pool(mint)

                return bonding_curve

        except Exception as e:
            logger.warning(f"Failed to discover bonding curve for {mint[:8]}...: {e}")

        return None

    async def _discover_pumpswap_pool(self, mint: str) -> Optional[str]:
        """Discover PumpSwap pool for a mint"""

        try:
            import sys
            import os
            sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))
            import pumpswap

            pool = await pumpswap.find_pool_for_mint(mint)

            if pool:
                # Fetch pool state
                pool_state = await pumpswap.fetch_pool_state(pool)

                if pool_state:
                    liquidity_sol = pool_state.get("pool_quote_reserves", 0) / 1e9
                    pool_base_reserves = pool_state.get("base_reserves")
                    pool_quote_reserves = pool_state.get("quote_reserves")

                    await self.state_machine.cache_pool_info(
                        mint=mint,
                        pumpswap_pool=pool,
                        is_graduated=True,
                        pool_liquidity_sol=liquidity_sol,
                        discovered_via="proactive_discovery",
                    )

                    logger.info(
                        f"Cached PumpSwap pool: mint={mint[:8]}... | "
                        f"pool={pool[:8]}... | liquidity={liquidity_sol:.2f} SOL"
                    )

                    return pool

        except Exception as e:
            logger.warning(f"Failed to discover PumpSwap pool for {mint[:8]}...: {e}")

        return None

    async def _discover_full_pool_info(self, mint: str):
        """Discover both bonding curve and pool for a mint"""

        # Try bonding curve first
        bonding_curve = await self._discover_bonding_curve(mint)

        # If no bonding curve, try pool directly
        if not bonding_curve:
            await self._discover_pumpswap_pool(mint)

    async def _cleanup_stale_cache(self):
        """Remove cache entries older than 7 days with no references"""

        cutoff = (datetime.now(timezone.utc).replace(
            hour=0, minute=0, second=0, microsecond=0
        ) - timedelta(days=7)).isoformat()

        # Get old cache entries
        result = await self.supabase.table("token_pools").select(
            "mint"
        ).lt("last_discovery_at", cutoff).execute()

        if not result.data:
            return

        stale_mints = [p["mint"] for p in result.data]

        # Check if any positions reference these mints
        for mint in stale_mints:
            positions_result = await self.supabase.table("positions").select(
                "id"
            ).eq("mint", mint).limit(1).execute()

            if not positions_result.data:
                # No active positions, can delete
                await self.supabase.table("token_pools").delete().eq(
                    "mint", mint
                ).execute()

                logger.info(f"Removed stale cache for mint {mint[:8]}...")

    async def get_pool_for_exit(
        self,
        mint: str,
        desired_protocol: str
    ) -> Optional[Dict[str, Any]]:
        """
        Get pool info for exiting a position.

        Returns cached pool info, or attempts discovery if missing.
        """
        cached = await self.state_machine.get_pool_address(mint)

        if not cached:
            # Try to discover now
            logger.warning(
                f"Pool cache miss for {mint[:8]}..., attempting discovery"
            )

            if desired_protocol == "pumpfun":
                bonding_curve = await self._discover_bonding_curve(mint)
                if bonding_curve:
                    cached = await self.state_machine.get_pool_address(mint)
            else:
                pool = await self._discover_pumpswap_pool(mint)
                if pool:
                    cached = await self.state_machine.get_pool_address(mint)

        if cached:
            # Check graduation status
            is_graduated = cached.get("is_graduated", False)

            # If graduated but want pumpfun, that's a problem
            if is_graduated and desired_protocol == "pumpfun":
                logger.warning(
                    f"Attempted pumpfun exit on graduated token {mint[:8]}... "
                    f"— should use pumpswap pool instead"
                )
                # Auto-switch to pumpswap if we have the pool
                if cached.get("pumpswap_pool"):
                    return cached

            return cached

        # Final fallback: attempt full discovery
        await self._discover_full_pool_info(mint)
        return await self.state_machine.get_pool_address(mint)


# Import required at module level for timedelta
from datetime import timedelta
