"""
Position State Machine for Solana Trading Bot

Manages position lifecycle with explicit states for graduation transitions.
Integrates with Supabase for persistent state tracking.

Key Design:
  - Proactive protocol migration when graduation detected
  - Pool addresses cached at discovery time
  - Event sourcing for all state transitions
  - Automatic retry with escalation for failed transitions
"""
import asyncio
import logging
from datetime import datetime, timezone
from enum import Enum
from typing import Optional, Dict, Any

logger = logging.getLogger("position_state_machine")


class PositionStatus(str, Enum):
    """Position lifecycle states"""
    PENDING_ENTRY = "pending_entry"
    ACTIVE = "active"
    GRADUATING = "graduating"  # NEW: Transitioning pumpfun -> pumpswap
    EXIT_PENDING = "exit_pending"
    CLOSED = "closed"
    FAILED_ENTRY = "failed_entry"
    EXIT_FAILED = "exit_failed"
    TERMINAL = "terminal"
    ZOMBIE = "zombie"


class ProtocolType(str, Enum):
    """Trading protocols"""
    PUMPFUN = "pumpfun"
    PUMPSWAP = "pumpswap"


class ExitTrigger(str, Enum):
    """Reason for position exit"""
    TAKE_PROFIT = "take_profit"
    STOP_LOSS = "stop_loss"
    TRAILING_STOP = "trailing_stop"
    TIMEOUT = "timeout"
    CLASSIFIER_ABORT = "classifier_abort"
    GRADUATION_EXIT = "graduation_exit"
    HARD_STOP = "hard_stop"
    MANUAL = "manual"


class GraduationStatus(str, Enum):
    """Graduation transition states"""
    DETECTED = "detected"
    DISCOVERING_POOL = "discovering_pool"
    MIGRATING = "migrating"
    COMPLETED = "completed"
    FAILED = "failed"


class PositionStateMachine:
    """Manages position lifecycle with graduation awareness"""

    def __init__(self, supabase_client):
        self.supabase = supabase_client

    async def create_position(
        self,
        user_id: str,
        mint: str,
        entry_protocol: ProtocolType,
        entry_price_sol: float,
        entry_tokens: int,
        entry_sol: float,
        entry_usd: float,
        bonding_curve: Optional[str] = None,
        pumpswap_pool: Optional[str] = None,
        symbol: Optional[str] = None,
        name: Optional[str] = None,
        creator: Optional[str] = None,
    ) -> str:
        """
        Create a new position in pending_entry state.

        Returns position ID.
        """
        position_data = {
            "user_id": user_id,
            "mint": mint,
            "status": PositionStatus.PENDING_ENTRY.value,
            "entry_protocol": entry_protocol.value,
            "current_protocol": entry_protocol.value,
            "entry_price_sol": entry_price_sol,
            "entry_tokens": entry_tokens,
            "entry_sol": entry_sol,
            "entry_usd": entry_usd,
            "pumpfun_bonding_curve": bonding_curve if entry_protocol == ProtocolType.PUMPFUN else None,
            "pumpswap_pool": pumpswap_pool if entry_protocol == ProtocolType.PUMPSWAP else None,
            "symbol": symbol,
            "name": name,
            "creator": creator,
        }

        result = await self.supabase.table("positions").insert(position_data).execute()
        position_id = result.data[0]["id"]

        await self._emit_event(
            position_id=position_id,
            user_id=user_id,
            event_type="position_created",
            to_status=PositionStatus.PENDING_ENTRY,
            protocol_to=entry_protocol,
            notes=f"Created position for {mint[:8]}... on {entry_protocol.value}",
        )

        logger.info(
            f"Position created: {position_id[:8]}... | "
            f"mint={mint[:8]}... | protocol={entry_protocol.value} | "
            f"entry_sol={entry_sol:.4f}"
        )

        return position_id

    async def activate_position(
        self,
        position_id: str,
        user_id: str,
        tx_hash: str,
    ):
        """Transition from pending_entry to active after successful entry tx"""

        update_data = {
            "status": PositionStatus.ACTIVE.value,
            "entry_tx_hash": tx_hash,
        }

        await self.supabase.table("positions").update(update_data).eq("id", position_id).execute()

        position = await self._get_position(position_id)

        await self._emit_event(
            position_id=position_id,
            user_id=user_id,
            event_type="entry_confirmed",
            from_status=PositionStatus.PENDING_ENTRY,
            to_status=PositionStatus.ACTIVE,
            notes=f"Entry tx confirmed: {tx_hash[:12]}...",
            data={"tx_hash": tx_hash},
        )

        logger.info(
            f"Position activated: {position_id[:8]}... | "
            f"mint={position['mint'][:8]}... | tx={tx_hash[:12]}..."
        )

    async def detect_graduation(
        self,
        position_id: str,
        user_id: str,
        bonding_curve_complete: bool,
        graduating_liquidity_sol: Optional[float] = None,
    ) -> bool:
        """
        Detect and initiate graduation transition.

        Returns True if graduation transition started.
        """
        position = await self._get_position(position_id)
        if not position:
            return False

        # Only pumpfun positions can graduate
        if position["current_protocol"] != ProtocolType.PUMPFUN.value:
            return False

        # Already graduated?
        if position.get("graduated_at"):
            return False

        if not bonding_curve_complete:
            return False

        logger.info(
            f"GRADUATION DETECTED: position={position_id[:8]}... | "
            f"mint={position['mint'][:8]}... | old_liquidity={graduating_liquidity_sol:.2f} SOL"
        )

        # Create graduation transition record
        transition_data = {
            "position_id": position_id,
            "user_id": user_id,
            "mint": position["mint"],
            "detected_at": datetime.now(timezone.utc).isoformat(),
            "old_liquidity_sol": graduating_liquidity_sol,
            "transition_status": GraduationStatus.DETECTED.value,
        }

        transition_result = await self.supabase.table("graduation_transitions").insert(
            transition_data
        ).execute()
        transition_id = transition_result.data[0]["id"]

        # Update position
        await self.supabase.table("positions").update({
            "status": PositionStatus.GRADUATING.value,
            "graduation_detected_at": datetime.now(timezone.utc).isoformat(),
        }).eq("id", position_id).execute()

        await self._emit_event(
            position_id=position_id,
            user_id=user_id,
            event_type="graduation_detected",
            from_status=PositionStatus.ACTIVE,
            to_status=PositionStatus.GRADUATING,
            notes=f"Graduation detected, transitioning to PumpSwap",
            data={"transition_id": transition_id, "old_liquidity_sol": graduating_liquidity_sol},
        )

        return True

    async def begin_pool_discovery(
        self,
        position_id: str,
        user_id: str,
        transition_id: str,
    ):
        """Mark graduation as discovering pool"""

        await self.supabase.table("graduation_transitions").update({
            "transition_status": GraduationStatus.DISCOVERING_POOL.value,
            "transition_started_at": datetime.now(timezone.utc).isoformat(),
        }).eq("id", transition_id).execute()

        await self.supabase.table("graduation_transitions").update({
            "pool_discovery_attempts": 1,
        }).eq("id", transition_id).execute()

        logger.info(
            f"Pool discovery started: transition={transition_id[:8]}... | "
            f"position={position_id[:8]}..."
        )

    async def complete_migration(
        self,
        position_id: str,
        user_id: str,
        pool_address: str,
        new_liquidity_sol: float,
        discovery_method: str = "standard_lookup",
    ):
        """Complete the graduation transition"""

        position = await self._get_position(position_id)
        if not position:
            raise ValueError(f"Position {position_id} not found")

        logger.info(
            f"GRADUATION MIGRATION COMPLETE: position={position_id[:8]}... | "
            f"pool={pool_address[:8]}... | new_liquidity={new_liquidity_sol:.2f} SOL"
        )

        # Get active transition
        transitions = await self.supabase.table("graduation_transitions").select(
            "*"  no
        ).eq("position_id", position_id).eq(
            "transition_status", GraduationStatus.DISCOVERING_POOL.value
        ).execute()

        if not transitions.data:
            raise ValueError(f"No active graduation transition for position {position_id}")

        transition_id = transitions.data[0]["id"]

        # Update transition
        now = datetime.now(timezone.utc).isoformat()
        await self.supabase.table("graduation_transitions").update({
            "transition_status": GraduationStatus.COMPLETED.value,
            "transition_completed_at": now,
            "pool_discovery_success": True,
            "pool_discovery_method": discovery_method,
            "new_liquidity_sol": new_liquidity_sol,
        }).eq("id", transition_id).execute()

        # Update position
        await self.supabase.table("positions").update({
            "status": PositionStatus.ACTIVE.value,  # Back to active, now on pumpswap
            "current_protocol": ProtocolType.PUMPSWAP.value,
            "pumpswap_pool": pool_address,
            "graduated_at": now,
            "graduation_tx_hash": transition_id,  # Track which transition did this
        }).eq("id", position_id).execute()

        # Update or create pool cache
        await self._upsert_pool_cache(
            mint=position["mint"],
            pumpswap_pool=pool_address,
            is_graduated=True,
            graduated_at=now,
            pool_liquidity_sol=new_liquidity_sol,
        )

        await self._emit_event(
            position_id=position_id,
            user_id=user_id,
            event_type="graduation_completed",
            from_status=PositionStatus.GRADUATING,
            to_status=PositionStatus.ACTIVE,
            protocol_from=ProtocolType.PUMPFUN,
            protocol_to=ProtocolType.PUMPSWAP,
            notes=f"Migrated to PumpSwap pool {pool_address[:8]}...",
            data={
                "pool_address": pool_address,
                "new_liquidity_sol": new_liquidity_sol,
                "transition_id": transition_id,
            },
        )

    async def fail_graduation(
        self,
        position_id: str,
        user_id: str,
        error_message: str,
    ):
        """Mark graduation as failed, escalate to terminal"""

        # Get active transition
        transitions = await self.supabase.table("graduation_transitions").select(
            "*"
        ).eq("position_id", position_id).in_(
            "transition_status",
            [GraduationStatus.DETECTED.value, GraduationStatus.DISCOVERING_POOL.value]
        ).order("created_at", desc=True).limit(1).execute()

        if transitions.data:
            transition_id = transitions.data[0]["id"]
            await self.supabase.table("graduation_transitions").update({
                "transition_status": GraduationStatus.FAILED.value,
                "error_message": error_message,
            }).eq("id", transition_id).execute()

        # Escalate position to exit_failed with special note
        await self.supabase.table("positions").update({
            "status": PositionStatus.EXIT_FAILED.value,
            "exit_error": f"Graduation transition failed: {error_message}",
        }).eq("id", position_id).execute()

        await self._emit_event(
            position_id=position_id,
            user_id=user_id,
            event_type="graduation_failed",
            from_status=PositionStatus.GRADUATING,
            to_status=PositionStatus.EXIT_FAILED,
            notes=f"Graduation migration failed: {error_message}",
            data={"error": error_message},
        )

        logger.error(
            f"GRADUATION FAILED: position={position_id[:8]}... | error={error_message}"
        )

    async def initiate_exit(
        self,
        position_id: str,
        user_id: str,
        exit_trigger: ExitTrigger,
        exit_protocol: Optional[ProtocolType] = None,
    ):
        """Begin exit process"""

        position = await self._get_position(position_id)
        if not position:
            raise ValueError(f"Position {position_id} not found")

        # Use current protocol if not specified
        if exit_protocol is None:
            exit_protocol = ProtocolType(position["current_protocol"])

        await self.supabase.table("positions").update({
            "status": PositionStatus.EXIT_PENDING.value,
            "exit_trigger": exit_trigger.value,
            "exit_protocol": exit_protocol.value,
            "last_exit_attempt_at": datetime.now(timezone.utc).isoformat(),
        }).eq("id", position_id).execute()

        await self._emit_event(
            position_id=position_id,
            user_id=user_id,
            event_type="exit_initiated",
            from_status=PositionStatus(position["status"]),
            to_status=PositionStatus.EXIT_PENDING,
            notes=f"Exit triggered: {exit_trigger.value}",
            data={"exit_trigger": exit_trigger.value, "exit_protocol": exit_protocol.value},
        )

        logger.info(
            f"Exit initiated: position={position_id[:8]}... | "
            f"trigger={exit_trigger.value} | protocol={exit_protocol.value}"
        )

    async def complete_exit(
        self,
        position_id: str,
        user_id: str,
        exit_tx_hash: str,
        exit_price_sol: float,
        exit_sol: float,
        exit_usd: float,
        pnl_sol: float,
        pnl_usd: float,
        pnl_pct: float,
        fees_sol: float,
    ):
        """Mark position as successfully exited"""

        now = datetime.now(timezone.utc).isoformat()

        await self.supabase.table("positions").update({
            "status": PositionStatus.CLOSED.value,
            "exit_tx_hash": exit_tx_hash,
            "exit_price_sol": exit_price_sol,
            "exit_sol": exit_sol,
            "exit_usd": exit_usd,
            "exit_at": now,
            "pnl_sol": pnl_sol,
            "pnl_usd": pnl_usd,
            "pnl_pct": pnl_pct,
            "fees_sol": fees_sol,
        }).eq("id", position_id).execute()

        position = await self._get_position(position_id)

        await self._emit_event(
            position_id=position_id,
            user_id=user_id,
            event_type="exit_confirmed",
            from_status=PositionStatus.EXIT_PENDING,
            to_status=PositionStatus.CLOSED,
            notes=f"Position closed: pnl={pnl_pct:+.2f}%",
            data={
                "exit_tx_hash": exit_tx_hash,
                "pnl_sol": pnl_sol,
                "pnl_pct": pnl_pct,
            },
        )

        logger.info(
            f"EXIT CONFIRMED: position={position_id[:8]}... | "
            f"mint={position['mint'][:8]}... | pnl={pnl_pct:+.2f}% | "
            f"protocol={position['exit_protocol']}"
        )

    async def fail_exit(
        self,
        position_id: str,
        user_id: str,
        error_message: str,
        retry: bool = True,
    ):
        """Handle exit failure"""

        position = await self._get_position(position_id)
        if not position:
            return

        attempts = position.get("exit_attempts", 0) + 1

        if retry and attempts < 3:
            # Will retry
            await self.supabase.table("positions").update({
                "status": PositionStatus.EXIT_FAILED.value,
                "exit_attempts": attempts,
                "exit_error": error_message,
                "last_exit_attempt_at": datetime.now(timezone.utc).isoformat(),
            }).eq("id", position_id).execute()

            await self._emit_event(
                position_id=position_id,
                user_id=user_id,
                event_type="exit_failed_retry",
                from_status=PositionStatus.EXIT_PENDING,
                to_status=PositionStatus.EXIT_FAILED,
                notes=f"Exit failed (attempt {attempts}/3): {error_message}",
                data={"error": error_message, "attempts": attempts},
            )

            logger.warning(
                f"Exit failed (will retry): position={position_id[:8]}... | "
                f"attempt={attempts}/3 | error={error_message[:50]}..."
            )

        else:
            # Terminal failure
            await self.supabase.table("positions").update({
                "status": PositionStatus.TERMINAL.value,
                "exit_attempts": attempts,
                "exit_error": error_message,
            }).eq("id", position_id).execute()

            await self._emit_event(
                position_id=position_id,
                user_id=user_id,
                event_type="exit_terminal",
                from_status=PositionStatus.EXIT_PENDING,
                to_status=PositionStatus.TERMINAL,
                notes=f"Exit failed after {attempts} attempts, manual recovery needed",
                data={"error": error_message, "attempts": attempts},
            )

            logger.error(
                f"EXIT TERMINAL: position={position_id[:8]}... | "
                f"mint={position['mint'][:8]}... | manual recovery required"
            )

    async def get_pool_address(self, mint: str) -> Optional[Dict[str, Any]]:
        """Get cached pool info for a mint"""

        result = await self.supabase.table("token_pools").select(
            "*"
        ).eq("mint", mint).limit(1).execute()

        if result.data:
            return result.data[0]

        return None

    async def cache_pool_info(
        self,
        mint: str,
        bonding_curve: Optional[str] = None,
        pumpswap_pool: Optional[str] = None,
        is_graduated: bool = False,
        virtual_sol_reserves: Optional[int] = None,
        pool_liquidity_sol: Optional[float] = None,
        discovered_via: str = "unknown",
    ):
        """Cache pool information for a mint"""

        pool_data = {
            "mint": mint,
            "bonding_curve": bonding_curve,
            "pumpswap_pool": pumpswap_pool,
            "is_graduated": is_graduated,
            "virtual_sol_reserves": virtual_sol_reserves,
            "pool_liquidity_sol": pool_liquidity_sol,
            "discovered_via": discovered_via,
            "last_discovery_at": datetime.now(timezone.utc).isoformat(),
        }

        await self._upsert_pool_cache(**pool_data)

    # Private helpers

    async def _get_position(self, position_id: str) -> Optional[Dict[str, Any]]:
        """Get position by ID"""
        result = await self.supabase.table("positions").select(
            "*"
        ).eq("id", position_id).limit(1).execute()

        return result.data[0] if result.data else None

    async def _emit_event(
        self,
        position_id: str,
        user_id: str,
        event_type: str,
        from_status: Optional[PositionStatus] = None,
        to_status: Optional[PositionStatus] = None,
        protocol_from: Optional[ProtocolType] = None,
        protocol_to: Optional[ProtocolType] = None,
        notes: Optional[str] = None,
        data: Optional[Dict[str, Any]] = None,
    ):
        """Emit position event for audit trail"""

        event_data = {
            "position_id": position_id,
            "user_id": user_id,
            "event_type": event_type,
            "from_status": from_status.value if from_status else None,
            "to_status": to_status.value if to_status else None,
            "protocol_from": protocol_from.value if protocol_from else None,
            "protocol_to": protocol_to.value if protocol_to else None,
            "notes": notes,
            "data": data or {},
        }

        await self.supabase.table("position_events").insert(event_data).execute()

    async def _upsert_pool_cache(self, mint: str, **kwargs):
        """Upsert pool cache entry"""

        # Try to get existing
        existing = await self.get_pool_address(mint)

        if existing:
            # Update
            update_data = {k: v for k, v in kwargs.items() if v is not None}
            await self.supabase.table("token_pools").update(update_data).eq(
                "mint", mint
            ).execute()
        else:
            # Insert
            insert_data = {"mint": mint, **kwargs}
            insert_data["discovered_via"] = insert_data.get("discovered_via", "unknown")
            await self.supabase.table("token_pools").insert(insert_data).execute()
