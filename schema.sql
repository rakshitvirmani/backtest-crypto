-- =============================================================================
-- Production Backtesting System - Database Schema (DuckDB)
-- =============================================================================
-- IMPORTANT: This schema enforces data integrity at the database level.
-- All CHECK constraints are non-negotiable safety rails.
-- DuckDB is an embedded analytical database — no server required.
-- =============================================================================

-- Sequences for auto-incrementing IDs
CREATE SEQUENCE IF NOT EXISTS klines_id_seq START 1;
CREATE SEQUENCE IF NOT EXISTS backtest_runs_id_seq START 1;
CREATE SEQUENCE IF NOT EXISTS trade_log_id_seq START 1;
CREATE SEQUENCE IF NOT EXISTS fetch_log_id_seq START 1;
CREATE SEQUENCE IF NOT EXISTS equity_curve_id_seq START 1;


-- Core klines table: single source of truth for all OHLCV data
CREATE TABLE IF NOT EXISTS klines (
    id INTEGER PRIMARY KEY DEFAULT nextval('klines_id_seq'),
    symbol VARCHAR NOT NULL,
    timeframe VARCHAR NOT NULL,
    open_time BIGINT NOT NULL,
    close_time BIGINT NOT NULL,
    open DOUBLE NOT NULL,
    high DOUBLE NOT NULL,
    low DOUBLE NOT NULL,
    close DOUBLE NOT NULL,
    volume DOUBLE NOT NULL,
    quote_asset_volume DOUBLE,
    number_of_trades INTEGER,
    taker_buy_base_volume DOUBLE,
    taker_buy_quote_volume DOUBLE,
    fetch_timestamp TIMESTAMP DEFAULT current_timestamp,
    data_checksum VARCHAR,

    -- Uniqueness enforced separately via UNIQUE constraint
    UNIQUE(symbol, timeframe, open_time),

    -- OHLC integrity
    CHECK (high >= low),
    CHECK (high >= open),
    CHECK (high >= close),
    CHECK (low <= open),
    CHECK (low <= close),
    CHECK (volume >= 0),
    CHECK (close_time > open_time),
    CHECK (open > 0 AND high > 0 AND low > 0 AND close > 0)
);

CREATE SEQUENCE IF NOT EXISTS klines_id_seq START 1;


-- =============================================================================
-- Backtest results table: full audit trail of every backtest run
-- =============================================================================
CREATE SEQUENCE IF NOT EXISTS backtest_runs_id_seq START 1;

CREATE TABLE IF NOT EXISTS backtest_runs (
    id INTEGER PRIMARY KEY DEFAULT nextval('backtest_runs_id_seq'),
    run_id VARCHAR UNIQUE NOT NULL,
    symbol VARCHAR NOT NULL,
    timeframe VARCHAR NOT NULL,
    strategy_name VARCHAR NOT NULL,
    strategy_params VARCHAR NOT NULL,  -- JSON stored as text
    backtest_start TIMESTAMP NOT NULL,
    backtest_end TIMESTAMP NOT NULL,
    created_at TIMESTAMP DEFAULT current_timestamp,

    -- Performance metrics
    total_return DOUBLE,
    annualized_return DOUBLE,
    max_drawdown DOUBLE,
    sharpe_ratio DOUBLE,
    sortino_ratio DOUBLE,
    calmar_ratio DOUBLE,
    win_rate DOUBLE,
    profit_factor DOUBLE,
    num_trades INTEGER,
    num_winning_trades INTEGER,
    avg_trade_pnl_pct DOUBLE,
    best_trade_pnl_pct DOUBLE,
    worst_trade_pnl_pct DOUBLE,
    max_consecutive_losses INTEGER,

    -- Configuration snapshot
    initial_capital DOUBLE,
    slippage_pct DOUBLE,
    commission_pct DOUBLE,

    -- Validation & approval workflow
    validation_status VARCHAR NOT NULL DEFAULT 'PENDING',
    validation_notes VARCHAR,
    approved_for_live BOOLEAN DEFAULT FALSE,
    approved_by VARCHAR,
    approved_at TIMESTAMP,

    CHECK (validation_status IN ('PENDING', 'PASSED', 'FAILED', 'FLAGGED'))
);


-- =============================================================================
-- Trade execution log: every simulated (and later live) trade
-- =============================================================================
CREATE SEQUENCE IF NOT EXISTS trade_log_id_seq START 1;

CREATE TABLE IF NOT EXISTS trade_log (
    id INTEGER PRIMARY KEY DEFAULT nextval('trade_log_id_seq'),
    backtest_run_id INTEGER,
    trade_number INTEGER NOT NULL,
    entry_time TIMESTAMP NOT NULL,
    exit_time TIMESTAMP NOT NULL,
    entry_price DOUBLE NOT NULL,
    exit_price DOUBLE NOT NULL,
    position_size DOUBLE NOT NULL,
    direction VARCHAR NOT NULL DEFAULT 'LONG',
    pnl DOUBLE NOT NULL,
    pnl_percent DOUBLE NOT NULL,
    commission_paid DOUBLE DEFAULT 0,
    slippage_cost DOUBLE DEFAULT 0,
    trade_reason VARCHAR,
    is_winning_trade BOOLEAN,
    entry_signal VARCHAR,
    exit_signal VARCHAR,
    equity_at_entry DOUBLE,
    equity_at_exit DOUBLE,
    drawdown_at_entry DOUBLE,

    CHECK (direction IN ('LONG', 'SHORT')),
    CHECK (exit_time >= entry_time),
    CHECK (entry_price > 0 AND exit_price > 0),
    CHECK (position_size > 0)
);


-- =============================================================================
-- Data fetch audit log: track every API call for debugging
-- =============================================================================
CREATE SEQUENCE IF NOT EXISTS fetch_log_id_seq START 1;

CREATE TABLE IF NOT EXISTS fetch_log (
    id INTEGER PRIMARY KEY DEFAULT nextval('fetch_log_id_seq'),
    symbol VARCHAR NOT NULL,
    timeframe VARCHAR NOT NULL,
    fetch_start TIMESTAMP NOT NULL,
    fetch_end TIMESTAMP NOT NULL,
    http_status INTEGER,
    response_time_ms INTEGER,
    records_fetched INTEGER DEFAULT 0,
    records_upserted INTEGER DEFAULT 0,
    errors VARCHAR,
    checksum VARCHAR,
    created_at TIMESTAMP DEFAULT current_timestamp
);


-- =============================================================================
-- Equity curve snapshots
-- =============================================================================
CREATE SEQUENCE IF NOT EXISTS equity_curve_id_seq START 1;

CREATE TABLE IF NOT EXISTS equity_curve (
    id INTEGER PRIMARY KEY DEFAULT nextval('equity_curve_id_seq'),
    backtest_run_id INTEGER,
    timestamp TIMESTAMP NOT NULL,
    equity DOUBLE NOT NULL,
    drawdown_pct DOUBLE,
    position_open BOOLEAN DEFAULT FALSE,

    UNIQUE(backtest_run_id, timestamp)
);
