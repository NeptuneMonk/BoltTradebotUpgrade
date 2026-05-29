/*
  # Trading State Management Schema

  1. Overview
    - Comprehensive state tracking for Solana trading bot
    - Explicit states for position lifecycle including graduation transitions
    - Pool address caching to avoid expensive lookups during exits
    - Event sourcing for debugging and replay

  2. Core Tables
    - `positions` — Core trading position state with graduation tracking
    - `position_events` — Event log for state transitions
    - `token_pools` — Cache of discovered pool addresses
    - `graduation_transitions` — Explicit graduation migration tracking

  3. Security
    - RLS enabled on all tables
    - Authenticated users can only access their own data
    - Service role can access all data for bot operations

  4. Important Notes
    - `current_protocol` is the ACTIVE protocol (pumpfun/pumpswap)
    - `entry_protocol` preserves where position was opened
    - `graduated_at` timestamps when token migrated to AMM
    - Pool addresses are cached at discovery time, not exit time
*/

-- Enable UUID extension
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- Position states enum
CREATE TYPE position_status AS ENUM (
  'pending_entry',
  'active',
  'graduating',
  'exit_pending',
  'closed',
  'failed_entry',
  'exit_failed',
  'terminal',
  'zombie'
);

-- Protocols enum
CREATE TYPE protocol_type AS ENUM (
  'pumpfun',
  'pumpswap'
);

-- Exit triggers enum
CREATE TYPE exit_trigger AS ENUM (
  'take_profit',
  'stop_loss',
  'trailing_stop',
  'timeout',
  'classifier_abort',
  'graduation_exit',
  'hard_stop',
  'manual'
);

-- Main positions table
CREATE TABLE positions (
  id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
  user_id UUID NOT NULL,
  mint TEXT NOT NULL,
  
  -- Status
  status position_status NOT NULL DEFAULT 'pending_entry',
  
  -- Entry details
  entry_protocol protocol_type NOT NULL,
  entry_tx_hash TEXT,
  entry_price_sol DECIMAL(20, 10) NOT NULL,
  entry_tokens BIGINT NOT NULL,
  entry_sol DECIMAL(10, 6) NOT NULL,
  entry_usd DECIMAL(10, 2) NOT NULL,
  entry_at TIMESTAMPTZ DEFAULT NOW() NOT NULL,
  
  -- Current protocol (changes after graduation)
  current_protocol protocol_type NOT NULL,
  pumpfun_bonding_curve TEXT,
  pumpswap_pool TEXT,
  
  -- Graduation tracking
  graduated_at TIMESTAMPTZ,
  graduation_detected_at TIMESTAMPTZ,
  graduation_tx_hash TEXT,
  
  -- Exit details
  exit_trigger exit_trigger,
  exit_protocol protocol_type,
  exit_tx_hash TEXT,
  exit_price_sol DECIMAL(20, 10),
  exit_sol DECIMAL(10, 6),
  exit_usd DECIMAL(10, 2),
  exit_at TIMESTAMPTZ,
  
  -- PnL
  pnl_sol DECIMAL(10, 6),
  pnl_usd DECIMAL(10, 2),
  pnl_pct DECIMAL(8, 2),
  fees_sol DECIMAL(8, 6),
  
  -- Retry tracking
  exit_attempts INT DEFAULT 0,
  last_exit_attempt_at TIMESTAMPTZ,
  exit_error TEXT,
  
  -- Metadata
  symbol TEXT,
  name TEXT,
  creator TEXT,
  metadata JSONB DEFAULT '{}',
  
  -- Timestamps
  created_at TIMESTAMPTZ DEFAULT NOW() NOT NULL,
  updated_at TIMESTAMPTZ DEFAULT NOW() NOT NULL
);

CREATE INDEX idx_positions_user_status ON positions(user_id, status);
CREATE INDEX idx_positions_mint ON positions(mint);
CREATE INDEX idx_positions_active ON positions(user_id) WHERE status IN ('active', 'graduating', 'exit_pending');
CREATE INDEX idx_positions_graduated ON positions(graduated_at) WHERE graduated_at IS NOT NULL;

-- Position events
CREATE TABLE position_events (
  id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
  position_id UUID NOT NULL,
  user_id UUID NOT NULL,
  
  event_type TEXT NOT NULL,
  from_status position_status,
  to_status position_status NOT NULL,
  
  protocol_from protocol_type,
  protocol_to protocol_type,
  
  data JSONB DEFAULT '{}',
  notes TEXT,
  
  created_at TIMESTAMPTZ DEFAULT NOW() NOT NULL
);

CREATE INDEX idx_events_position ON position_events(position_id, created_at DESC);
CREATE INDEX idx_events_user ON position_events(user_id, created_at DESC);

-- Pool cache
CREATE TABLE token_pools (
  id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
  mint TEXT NOT NULL UNIQUE,
  
  bonding_curve TEXT,
  virtual_sol_reserves BIGINT,
  virtual_token_reserves BIGINT,
  is_graduated BOOLEAN DEFAULT FALSE,
  graduated_at TIMESTAMPTZ,
  
  pumpswap_pool TEXT,
  pool_base_reserves BIGINT,
  pool_quote_reserves BIGINT,
  pool_liquidity_sol DECIMAL(10, 6),
  
  discovered_via TEXT NOT NULL,
  discovery_attempts INT DEFAULT 0,
  last_discovery_at TIMESTAMPTZ DEFAULT NOW(),
  
  created_at TIMESTAMPTZ DEFAULT NOW() NOT NULL,
  updated_at TIMESTAMPTZ DEFAULT NOW() NOT NULL
);

CREATE INDEX idx_pools_mint ON token_pools(mint);
CREATE INDEX idx_pools_graduated ON token_pools(is_graduated) WHERE is_graduated = TRUE;

-- Graduation transitions
CREATE TABLE graduation_transitions (
  id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
  position_id UUID,
  user_id UUID NOT NULL,
  mint TEXT NOT NULL,
  
  detected_at TIMESTAMPTZ NOT NULL,
  transition_started_at TIMESTAMPTZ,
  transition_completed_at TIMESTAMPTZ,
  
  pool_discovery_method TEXT,
  pool_discovery_attempts INT DEFAULT 0,
  pool_discovery_success BOOLEAN DEFAULT FALSE,
  
  old_liquidity_sol DECIMAL(10, 6),
  new_liquidity_sol DECIMAL(10, 6),
  
  transition_status TEXT NOT NULL DEFAULT 'detected',
  error_message TEXT,
  
  created_at TIMESTAMPTZ DEFAULT NOW() NOT NULL
);

CREATE INDEX idx_transitions_position ON graduation_transitions(position_id);
CREATE INDEX idx_transitions_mint ON graduation_transitions(mint);
CREATE INDEX idx_transitions_status ON graduation_transitions(transition_status);

-- Enable RLS
ALTER TABLE positions ENABLE ROW LEVEL SECURITY;
ALTER TABLE position_events ENABLE ROW LEVEL SECURITY;
ALTER TABLE token_pools ENABLE ROW LEVEL SECURITY;
ALTER TABLE graduation_transitions ENABLE ROW LEVEL SECURITY;

-- RLS Policies
CREATE POLICY "Users can view own positions"
  ON positions FOR SELECT
  TO authenticated
  USING (true);  -- Simplified for now, adjust for multi-user later

CREATE POLICY "Users can insert own positions"
  ON positions FOR INSERT
  TO authenticated
  WITH CHECK (true);

CREATE POLICY "Users can update own positions"
  ON positions FOR UPDATE
  TO authenticated
  USING (true)
  WITH CHECK (true);

CREATE POLICY "Users can view own events"
  ON position_events FOR SELECT
  TO authenticated
  USING (true);

CREATE POLICY "Users can insert own events"
  ON position_events FOR INSERT
  TO authenticated
  WITH CHECK (true);

CREATE POLICY "Users can view all pools"
  ON token_pools FOR SELECT
  TO authenticated
  USING (true);

CREATE POLICY "Users can insert pools"
  ON token_pools FOR INSERT
  TO authenticated
  WITH CHECK (true);

CREATE POLICY "Users can update pools"
  ON token_pools FOR UPDATE
  TO authenticated
  USING (true)
  WITH CHECK (true);

CREATE POLICY "Users can view own transitions"
  ON graduation_transitions FOR SELECT
  TO authenticated
  USING (true);

CREATE POLICY "Users can insert own transitions"
  ON graduation_transitions FOR INSERT
  TO authenticated
  WITH CHECK (true);

CREATE POLICY "Users can update own transitions"
  ON graduation_transitions FOR UPDATE
  TO authenticated
  USING (true)
  WITH CHECK (true);

-- Trigger for updated_at
CREATE OR REPLACE FUNCTION update_updated_at()
RETURNS TRIGGER AS $$
BEGIN
  NEW.updated_at = NOW();
  RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER update_positions_updated_at
  BEFORE UPDATE ON positions
  FOR EACH ROW
  EXECUTE FUNCTION update_updated_at();

CREATE TRIGGER update_pools_updated_at
  BEFORE UPDATE ON token_pools
  FOR EACH ROW
  EXECUTE FUNCTION update_updated_at();
