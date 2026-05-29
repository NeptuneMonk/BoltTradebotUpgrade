# State Management Architecture: The Complete Fix

## Executive Summary

The bot's state management was broken at a fundamental level. It didn't track position lifecycle through graduation, relied on reactive fallbacks instead of proactive migration, and had no pool address caching. This fix introduces a complete state machine with explicit graduation transitions.

## Problem Overview

### Root Causes Identified

1. **No Graduation State**: Positions only had "active" or "closed" — no "graduating" transition state
2. **Static Protocol Lock**: Protocol set at entry never changed, even when token migrated to AMM
3. **No Pool Caching**: Pool addresses discovered on-demand at exit time (expensive, failure-prone)
4. **Reactive Recovery**: Only attempted PumpSwap fallback AFTER exit failed, not BEFORE
5. **Silent Failures**: State fetch timeouts left positions in infinite sleep loops
6. **Orphaned Positions**: Monitor crashes left active positions with no watchdog

## Solution Architecture

### 1. Explicit State Machine

```
Position Lifecycle States:

PENDING_ENTRY ──(entry tx lands)──► ACTIVE
     │                                 │
     │                                 ├──► GRADUATING (NEW!)
     │                                 │       │
     │                                 │       ├─► ACTIVE (on PumpSwap)
     │                                 │       │
     │                                 │       └─► EXIT_FAILED
     │                                 │
     │                                 ├──► EXIT_PENDING ──► CLOSED
     │                                 │
     │                                 ├──► EXIT_FAILED (retry ≤3×)
     │                                 │
     │                                 └──► TERMINAL (manual recovery)
     │
     └──► FAILED_ENTRY
```

### 2. Database Schema (Supabase)

**Core Tables:**

#### `positions`
```sql
- id (UUID)
- user_id (UUID)
- mint (TEXT)
- status (ENUM: pending_entry, active, graduating, exit_pending, closed, ...)
  
- entry_protocol (ENUM: pumpfun, pumpswap)
- current_protocol (ENUM: pumpfun, pumpswap)  -- CHANGES after graduation!
- entry_tx_hash
- entry_price_sol
- entry_tokens
- entry_sol
- entry_usd
- entry_at
  
- pumpfun_bonding_curve (TEXT)
- pumpswap_pool (TEXT)
  
- graduated_at (TIMESTAMPTZ)
- graduation_detected_at (TIMESTAMPTZ)
  
- exit_trigger (ENUM)
- exit_protocol (ENUM)
- exit_tx_hash
- exit_price_sol
- exit_sol
- exit_usd
- exit_at
  
- pnl_sol
- pnl_usd
- pnl_pct
- fees_sol
  
- exit_attempts (INT)
- exit_error (TEXT)
```

#### `position_events`
```sql
- id (UUID)
- position_id (UUID)
- event_type (TEXT)
- from_status (ENUM)
- to_status (ENUM)
- protocol_from (ENUM)
- protocol_to (ENUM)
- data (JSONB)
- notes (TEXT)
- created_at (TIMESTAMPTZ)
```

#### `token_pools` (NEW!)
```sql
- id (UUID)
- mint (TEXT UNIQUE)
  
- bonding_curve (TEXT)
- virtual_sol_reserves (BIGINT)
  
- pumpswap_pool (TEXT)
- pool_base_reserves (BIGINT)
- pool_quote_reserves (BIGINT)
- pool_liquidity_sol (DECIMAL)
  
- is_graduated (BOOLEAN)
- graduated_at (TIMESTAMPTZ)
  
- discovered_via (TEXT)
- discovery_attempts (INT)
- last_discovery_at (TIMESTAMPTZ)
```

#### `graduation_transitions` (NEW!)
```sql
- id (UUID)
- position_id (UUID)
- mint (TEXT)
  
- detected_at (TIMESTAMPTZ)
- transition_started_at (TIMESTAMPTZ)
- transition_completed_at (TIMESTAMPTZ)
  
- pool_discovery_method (TEXT)
- pool_discovery_attempts (INT)
- pool_discovery_success (BOOLEAN)
  
- old_liquidity_sol (DECIMAL)
- new_liquidity_sol (DECIMAL)
  
- transition_status (ENUM: detected, discovering_pool, migrating, completed, failed)
- error_message (TEXT)
```

### 3. State Machine Operations

#### Entry Flow

```python
# 1. Create position (pending_entry)
position_id = await state_machine.create_position(
    user_id=user_id,
    mint=mint,
    entry_protocol=ProtocolType.PUMPFUN,
    entry_price_sol=price,
    entry_tokens=tokens,
    bonding_curve=bonding_curve_address,  # Cached now!
)

# 2. Entry tx lands -> activate
await state_machine.activate_position(
    position_id=position_id,
    tx_hash=entry_tx_sig,
)
```

#### Graduation Flow (THE KEY FIX)

```python
# 1. Monitor detects graduation
position = await get_position(position_id)
state = await pumpfun.fetch_bonding_curve_state(mint)

if state["complete"] and not position["graduated_at"]:
    # Create graduation transition record
    await state_machine.detect_graduation(
        position_id=position_id,
        bonding_curve_complete=True,
        graduating_liquidity_sol=old_liquidity,
    )
    
    # Position now in GRADUATING state
    
    # 2. Begin pool discovery (proactive, not reactive!)
    await state_machine.begin_pool_discovery(position_id)
    
    pool_address = await pumpswap.find_pool_for_mint(mint)
    pool_state = await pumpswap.fetch_pool_state(pool_address)
    
    if pool_address and pool_state:
        # 3. Complete migration
        await state_machine.complete_migration(
            position_id=position_id,
            pool_address=pool_address,
            new_liquidity_sol=pool_state["quote_reserves"] / 1e9,
        )
        
        # Position back to ACTIVE, but current_protocol = PUMPSWAP now!
        # pumpswap_pool field populated
        # Pool cache updated
        
    else:
        # Fail graduation, escalate to terminal
        await state_machine.fail_graduation(
            position_id=position_id,
            error_message="Pool discovery failed",
        )
```

#### Exit Flow

```python
# Get cached pool info (fast, no on-demand lookup)
pool_info = await state_machine.get_pool_address(mint)

# Check if we need to switch protocols
if position["current_protocol"] == "pumpfun" and pool_info.get("is_graduated"):
    # Token graduated but position doesn't know yet
    # Auto-migrate before exiting!
    pool_address = pool_info.get("pumpswap_pool")
    if pool_address:
        await state_machine.complete_migration(
            position_id=position_id,
            pool_address=pool_address,
            new_liquidity_sol=pool_info.get("pool_liquidity_sol"),
        )

# Initiate exit with correct protocol
await state_machine.initiate_exit(
    position_id=position_id,
    exit_trigger=ExitTrigger.TAKE_PROFIT,
    exit_protocol=ProtocolType(position["current_protocol"]),  # Correct!
)

# Build sell IX with cached pool
if position["current_protocol"] == "pumpswap":
    pool = position["pumpswap_pool"]  # Already cached!
    pool_state = await pumpswap.fetch_pool_state(pool)
    sell_ix = pumpswap.build_sell_ix(pool_state, ...)
else:
    bonding_curve = position["pumpfun_bonding_curve"]
    state = await pumpfun.fetch_bonding_curve_state(mint)
    sell_ix = pumpfun.build_sell_ix(state, ...)

# Submit tx
tx_hash = await send_tx([sell_ix])

# Confirm exit
await state_machine.complete_exit(
    position_id=position_id,
    exit_tx_hash=tx_hash,
    exit_price_sol=price,
    exit_sol=amount,
    pnl_pct=pnl,
    ...
)
```

### 4. Pool Caching Strategy

**Why Cache Pools?**
- `pumpswap.find_pool_for_mint()` scans ALL pools (100-500 RPC calls)
- Exit path needs pool address in <50ms for SL/TP to react fast
- Network issues during discovery = position stuck

**When to Cache:**
1. **Entry Time**: Discover + cache bonding curve when entering position
2. **Proactive Polling**: Background service discovers pools for active positions every 30s
3. **Discovery Phase**: Cache pools when discovery module finds tokens
4. **Graduation Detection**: Immediately cache PumpSwap pool when graduation detected

**Cache Structure:**
```
token_pools table:
  mint: "abc123..."
  bonding_curve: "xyz789..."
  pumpswap_pool: "def456..." (null until graduated)
  is_graduated: false → true
  pool_liquidity_sol: 0 → 125.5
  last_discovery_at: timestamp
```

### 5. Background Services

#### PoolDiscoveryService
- Poll every 30s
- Check active positions for missing pool addresses
- Recent mints (24h) get proactive discovery
- Clean up cache older than 7 days

#### MonitorWatchdogService (NEW)
- Check all active positions every 15s
- If `last_monitor_tick` > 15s ago → respawn monitor
- Detect zombies (positions in DB but not in active_trades)
- Reconciler logic moved here

#### PositionRecoveryService (NEW)
- Scan for TERMINAL positions every hour
- Attempt auto-recovery via emergency PumpSwap sell
- Notify user via WebSocket if recovery fails

### 6. Event Sourcing

Every state transition emits an event:

```python
position_events table:
  position_id: "abc..."
  event_type: "graduation_detected"
  from_status: "active"
  to_status: "graduating"
  protocol_from: "pumpfun"
  protocol_to: null
  data: {"transition_id": "xyz...", "old_liquidity_sol": 45.2}
  notes: "Graduation detected, transitioning to PumpSwap"
  created_at: timestamp
```

**Benefits:**
- Complete audit trail for debugging
- Can replay position lifecycle
- Analytics: time-from-entry-to-graduation, graduation-success-rate, etc.
- UI can show timeline: "Entry → Active (12m) → Graduating (3s) → Active on PumpSwap → Exit"

### 7. Failure Handling

#### Graduation Fails
```
GRADUATING → discovering_pool (pool lookup timeout)
           → FAIL: mark transition failed
           → ESCALATE: position → EXIT_FAILED with error
           → RETRY: 3× pool discovery with backoff
           → FAIL: position → TERMINAL (manual recovery)
```

#### Exit Fails
```
EXIT_PENDING → tx submit fails OR Custom:6005
            → RETRY: exit_attempts++
            → FAIL after 3×: TERMINAL
            → Success: CLOSED
```

#### State Fetch Fails
```
Old: sleep(1.0) and loop forever
New: sleep(5.0) and check last_monitor_tick
   → if stale > 15s: respawn monitor
   → if fetch fails 5×: initiate_emergency_exit
```

## Migration Path

### Phase 1: Schema Setup ✅
- [x] Create Supabase tables
- [x] Add RLS policies
- [x] Create state machine Python module
- [x] Create pool discovery service

### Phase 2: Integration (Next Steps)
- [ ] Update bot.py to use state_machine for all position changes
- [ ] Replace monitor's direct DB writes with state machine calls
- [ ] Add pool caching to discovery.py
- [ ] Wire background services

### Phase 3: Migration
- [ ] Add migration script for existing positions
  ```sql
  -- Populate pool cache from existing trades
  INSERT INTO token_pools (mint, bonding_curve)
  SELECT DISTINCT mint, pumpfun_bonding_curve
  FROM positions
  WHERE pumpfun_bonding_curve IS NOT NULL;
  ```
- [ ] Backfill position_events for recent trades

### Phase 4: Testing
- [ ] Paper trade through graduation
- [ ] Verify state transitions in DB
- [ ] Check event sourcing works
- [ ] Monitor pool cache updates

## Key Differences from Old Code

| Aspect | Old | New |
|--------|-----|-----|
| Protocol | Static (entry-time) | Dynamic (migration-aware) |
| Pool Address | Discovered at exit | Cached proactively |
| Graduation | Reactive (exit fails first) | Proactive (detect + migrate) |
| State Transitions | Implicit (status strings) | Explicit (state machine) |
| Failure Recovery | Retry loop | Escalation to terminal |
| Audit Trail | Status field only | Full event log |
| Monitor Crashes | Orphaned positions | Watchdog respawns |
| Pool Lookup | On-demand (expensive) | Cached (fast) |

## Metrics to Track

```python
# Graduation success rate
SELECT 
  COUNT(*) FILTER (WHERE transition_status = 'completed')::FLOAT / COUNT(*)
FROM graduation_transitions;

# Average graduation transition time
SELECT AVG(
  EXTRACT(EPOCH FROM (transition_completed_at - detected_at))
)
FROM graduation_transitions
WHERE transition_status = 'completed';

# Pool cache hit rate
SELECT 
  COUNT(*) FILTER (WHERE pumpswap_pool IS NOT NULL)::FLOAT / COUNT(*)
FROM token_pools
WHERE is_graduated = TRUE;

# Position lifecycle time
SELECT 
  AVG(EXTRACT(EPOCH FROM (exit_at - entry_at)))
FROM positions
WHERE status = 'closed';
```

## Files Created

```
backend/src/
  ├── position_state_machine.py   (500 lines) — Core state machine
  ├── pool_discovery.py           (300 lines) — Background pool caching
  └── position_recovery.py        (TODO) — Auto-recovery service

migrations/
  └── 001_trading_state_management.sql (400 lines) — Supabase schema
```

## Next Steps

1. Wire state machine into bot.py monitor loop
2. Replace direct DB updates with state machine calls
3. Add pool caching to discovery.py
4. Start background services on bot startup
5. Test graduation flow end-to-end on paper trading

---

**This fix addresses the root cause: the state machine now explicitly transitions through graduation instead of hoping exit fallback works.**
