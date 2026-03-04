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
    id BIGINT DEFAULT nextval('klines_id_seq') PRIMARY KEY,
    symbol VARCHAR(10) NOT NULL,
    timeframe VARCHAR(5) NOT NULL,
    open_time BIGINT NOT NULL,
    close_time BIGINT NOT NULL,
    open DECIMAL(20, 8) NOT NULL,
    high DECIMAL(20, 8) NOT NULL,
    low DECIMAL(20, 8) NOT NULL,
    close DECIMAL(20, 8) NOT NULL,
    volume DECIMAL(20, 8) NOT NULL,
    quote_asset_volume DECIMAL(20, 8),
    number_of_trades INT,
    taker_buy_base_volume DECIMAL(20, 8),
    taker_buy_quote_volume DECIMAL(20, 8),
    fetch_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    data_checksum VARCHAR(64),

    -- Uniqueness: one candle per symbol/timeframe/timestamp
    UNIQUE(symbol, timeframe, open_time),

    -- OHLC integrity: High is highest, Low is lowest
    CHECK (high >= low),
    CHECK (high >= open),
    CHECK (high >= close),
    CHECK (low <= open),
    CHECK (low <= close),

    -- Volume must be non-negative
    CHECK (volume >= 0),

    -- Timestamps must be ordered
    CHECK (close_time > open_time),

    -- Prices must be positive
    CHECK (open > 0 AND high > 0 AND low > 0 AND close > 0)
);

-- Primary query pattern: fetch candles by symbol+timeframe in time order
CREATE INDEX IF NOT EXISTS idx_klines_symbol_tf_time
    ON klines(symbol, timeframe, open_time DESC);

-- For monitoring data freshness
CREATE INDEX IF NOT EXISTS idx_klines_fetch_timestamp
    ON klines(fetch_timestamp DESC);

-- For gap detection queries
CREATE INDEX IF NOT EXISTS idx_klines_symbol_tf_time_asc
    ON klines(symbol, timeframe, open_time ASC);


-- =============================================================================
-- Backtest results table: full audit trail of every backtest run
-- =============================================================================
CREATE TABLE IF NOT EXISTS backtest_runs (
    id BIGINT DEFAULT nextval('backtest_runs_id_seq') PRIMARY KEY,
    run_id VARCHAR(36) UNIQUE NOT NULL,
    symbol VARCHAR(10) NOT NULL,
    timeframe VARCHAR(5) NOT NULL,
    strategy_name VARCHAR(50) NOT NULL,
    strategy_params JSON NOT NULL,
    backtest_start TIMESTAMP NOT NULL,
    backtest_end TIMESTAMP NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    -- Performance metrics
    total_return DECIMAL(10, 4),
    annualized_return DECIMAL(10, 4),
    max_drawdown DECIMAL(10, 4),
    sharpe_ratio DECIMAL(8, 4),
    sortino_ratio DECIMAL(8, 4),
    calmar_ratio DECIMAL(8, 4),
    win_rate DECIMAL(5, 4),
    profit_factor DECIMAL(8, 4),
    num_trades INT,
    num_winning_trades INT,
    avg_trade_duration VARCHAR(50),
    avg_trade_pnl_pct DECIMAL(8, 4),
    best_trade_pnl_pct DECIMAL(8, 4),
    worst_trade_pnl_pct DECIMAL(8, 4),
    max_consecutive_losses INT,

    -- Configuration snapshot (for reproducibility)
    initial_capital DECIMAL(20, 8),
    slippage_pct DECIMAL(8, 4),
    commission_pct DECIMAL(8, 4),

    -- Validation & approval workflow
    validation_status VARCHAR(20) NOT NULL DEFAULT 'PENDING',
    validation_notes TEXT,
    approved_for_live BOOLEAN DEFAULT FALSE,
    approved_by VARCHAR(100),
    approved_at TIMESTAMP,

    CHECK (validation_status IN ('PENDING', 'PASSED', 'FAILED', 'FLAGGED'))
);

CREATE INDEX IF NOT EXISTS idx_backtest_symbol_strategy
    ON backtest_runs(symbol, strategy_name, created_at DESC);

CREATE INDEX IF NOT EXISTS idx_backtest_validation
    ON backtest_runs(validation_status, approved_for_live);


-- =============================================================================
-- Trade execution log: every simulated (and later live) trade
-- =============================================================================
CREATE TABLE IF NOT EXISTS trade_log (
    id BIGINT DEFAULT nextval('trade_log_id_seq') PRIMARY KEY,
    backtest_run_id BIGINT REFERENCES backtest_runs(id),
    trade_number INT NOT NULL,
    entry_time TIMESTAMP NOT NULL,
    exit_time TIMESTAMP NOT NULL,
    entry_price DECIMAL(20, 8) NOT NULL,
    exit_price DECIMAL(20, 8) NOT NULL,
    position_size DECIMAL(20, 8) NOT NULL,
    direction VARCHAR(5) NOT NULL DEFAULT 'LONG',
    pnl DECIMAL(20, 8) NOT NULL,
    pnl_percent DECIMAL(8, 4) NOT NULL,
    commission_paid DECIMAL(20, 8) DEFAULT 0,
    slippage_cost DECIMAL(20, 8) DEFAULT 0,
    trade_reason TEXT,
    is_winning_trade BOOLEAN,
    entry_signal VARCHAR(50),
    exit_signal VARCHAR(50),
    equity_at_entry DECIMAL(20, 8),
    equity_at_exit DECIMAL(20, 8),
    drawdown_at_entry DECIMAL(8, 4),

    CHECK (direction IN ('LONG', 'SHORT')),
    CHECK (exit_time >= entry_time),
    CHECK (entry_price > 0 AND exit_price > 0),
    CHECK (position_size > 0)
);

CREATE INDEX IF NOT EXISTS idx_trade_log_run
    ON trade_log(backtest_run_id, trade_number);


-- =============================================================================
-- Data fetch audit log: track every API call for debugging
-- =============================================================================
CREATE TABLE IF NOT EXISTS fetch_log (
    id BIGINT DEFAULT nextval('fetch_log_id_seq') PRIMARY KEY,
    symbol VARCHAR(10) NOT NULL,
    timeframe VARCHAR(5) NOT NULL,
    fetch_start TIMESTAMP NOT NULL,
    fetch_end TIMESTAMP NOT NULL,
    http_status INT,
    response_time_ms INT,
    records_fetched INT DEFAULT 0,
    records_upserted INT DEFAULT 0,
    errors TEXT,
    checksum VARCHAR(64),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_fetch_log_time
    ON fetch_log(created_at DESC);


-- =============================================================================
-- Equity curve snapshots: for post-backtest analysis and reporting
-- =============================================================================
CREATE TABLE IF NOT EXISTS equity_curve (
    id BIGINT DEFAULT nextval('equity_curve_id_seq') PRIMARY KEY,
    backtest_run_id BIGINT REFERENCES backtest_runs(id),
    timestamp TIMESTAMP NOT NULL,
    equity DECIMAL(20, 8) NOT NULL,
    drawdown_pct DECIMAL(8, 4),
    position_open BOOLEAN DEFAULT FALSE,

    UNIQUE(backtest_run_id, timestamp)
);

CREATE INDEX IF NOT EXISTS idx_equity_curve_run
    ON equity_curve(backtest_run_id, timestamp);
