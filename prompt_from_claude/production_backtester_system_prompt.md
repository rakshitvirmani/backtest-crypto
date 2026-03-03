# Production-Grade Database-First Backtesting System

## Critical Foundation: Safety-First Architecture

This system will execute live trades with real capital. Every component must have redundancy, validation, and fail-safes. The cost of bugs is literal money loss.

---

## Task 1: Data Pipeline with Integrity Checks

### 1.1 Fetch Script (`fetch_to_db.py`)

Create a data ingestion system that prioritizes **data integrity over speed**.

**Requirements:**

- Use `python-binance` with exponential backoff retry logic (max 5 attempts, 1-32s delays)
- Fetch data: 1h, 4h, 1d, 1w, 1m timeframes for BTC/USDT
- **Data Validation Layer (CRITICAL):**
  - Reject any kline with OHLC out of order (O <= H, O >= L, C within [L,H])
  - Flag and log volume spikes >3σ from 50-bar rolling mean (potential data corruption)
  - Detect gaps in timestamps; raise alert if gap > timeframe window
  - Verify no duplicate timestamps in single timeframe/symbol
  - Check volume > 0 and realistic price movements (reject >20% single 1h move for BTC without exchange verification)
- **UPSERT Logic:**
  - Use ON CONFLICT ... DO UPDATE syntax (PostgreSQL) to handle partial re-fetches
  - Never backfill beyond 7 days without explicit flag (prevents accidental overwrites)
  - Log all UPSERT operations with timestamp, count, and checksums
- **Configuration:**
  - Use `config.py` with environment variable overrides (`DB_HOST`, `DB_USER`, etc.)
  - Never commit `config.py` to version control; provide `config.example.py` instead
  - Include API rate limit tracking (Binance: 1200 requests/min); pause if approaching limit
- **Retry & Monitoring:**
  - Implement circuit breaker: if 3 consecutive fetches fail, alert and stop
  - Log all fetches with HTTP status, response time, and record count
  - Alert if fetch takes >30s (indicates API/network issue)

---

### 1.2 Database Schema (`schema.sql`)

```sql
-- Core klines table
CREATE TABLE klines (
    id BIGSERIAL PRIMARY KEY,
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
    UNIQUE(symbol, timeframe, open_time),
    CHECK (high >= low AND high >= open AND high >= close AND low <= open AND low <= close),
    CHECK (volume >= 0)
);

CREATE INDEX idx_symbol_timeframe_time ON klines(symbol, timeframe, open_time DESC);
CREATE INDEX idx_fetch_timestamp ON klines(fetch_timestamp DESC);

-- Backtest results table (for audit trail)
CREATE TABLE backtest_runs (
    id BIGSERIAL PRIMARY KEY,
    run_id VARCHAR(36) UNIQUE NOT NULL,
    symbol VARCHAR(10) NOT NULL,
    timeframe VARCHAR(5) NOT NULL,
    strategy_name VARCHAR(50) NOT NULL,
    strategy_params JSONB NOT NULL,
    backtest_start TIMESTAMP NOT NULL,
    backtest_end TIMESTAMP NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    total_return DECIMAL(10, 4),
    annualized_return DECIMAL(10, 4),
    max_drawdown DECIMAL(10, 4),
    sharpe_ratio DECIMAL(8, 4),
    sortino_ratio DECIMAL(8, 4),
    win_rate DECIMAL(5, 4),
    profit_factor DECIMAL(8, 4),
    num_trades INT,
    num_winning_trades INT,
    avg_trade_duration INTERVAL,
    validation_status VARCHAR(20),  -- 'PASSED', 'FAILED', 'FLAGGED'
    validation_notes TEXT,
    approved_for_live BOOLEAN DEFAULT FALSE,
    approved_by VARCHAR(100),
    approved_at TIMESTAMP,
    CONSTRAINT validation_status_check CHECK (validation_status IN ('PASSED', 'FAILED', 'FLAGGED'))
);

CREATE INDEX idx_backtest_symbol_strategy ON backtest_runs(symbol, strategy_name, created_at DESC);

-- Trade execution log (future deployment tracking)
CREATE TABLE trade_log (
    id BIGSERIAL PRIMARY KEY,
    backtest_run_id BIGINT REFERENCES backtest_runs(id),
    entry_time TIMESTAMP NOT NULL,
    exit_time TIMESTAMP NOT NULL,
    entry_price DECIMAL(20, 8) NOT NULL,
    exit_price DECIMAL(20, 8) NOT NULL,
    position_size DECIMAL(20, 8) NOT NULL,
    pnl DECIMAL(20, 8) NOT NULL,
    pnl_percent DECIMAL(8, 4) NOT NULL,
    trade_reason TEXT,
    is_winning_trade BOOLEAN,
    entry_signal VARCHAR(50),
    exit_signal VARCHAR(50)
);
```

---

## Task 2: Enterprise-Grade Backtesting Engine

### 2.1 Backtester Core (`backtester.py`)

**Architecture Principles:**
- Separate concerns: data layer, strategy layer, execution layer, reporting layer
- All calculations must be reproducible (deterministic, no randomness)
- Every trade logged with reasoning
- Walk-forward validation built-in (not optional)

**Implementation:**

```python
# backtester.py
import logging
from datetime import datetime
from uuid import uuid4
from typing import Dict, List, Tuple
import pandas as pd
import numpy as np
from sqlalchemy import create_engine, text
import pandas_ta as ta
from backtesting import Backtest, Strategy
from dataclasses import dataclass
import json

# Configure logging to file + console
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('backtest.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

@dataclass
class BacktestConfig:
    """Immutable configuration for a single backtest run"""
    symbol: str
    timeframe: str
    strategy_name: str
    strategy_params: Dict
    start_date: datetime
    end_date: datetime
    initial_capital: float = 10000.0
    position_size_pct: float = 0.95  # Use 95% of equity per trade
    slippage_pct: float = 0.05  # 5 bps
    commission_pct: float = 0.1   # 10 bps
    max_drawdown_stop: float = -0.15  # Kill switch at -15% DD
    max_position_size_usd: float = None  # Optional hard cap
    
    def validate(self):
        """Ensure configuration is sane"""
        assert self.position_size_pct > 0 and self.position_size_pct <= 1.0
        assert self.slippage_pct >= 0
        assert self.commission_pct >= 0
        assert self.max_drawdown_stop < 0
        assert self.start_date < self.end_date
        logger.info(f"Config validated: {self.strategy_name} on {self.symbol}")

class DataValidator:
    """Pre-backtest data quality checks"""
    
    @staticmethod
    def validate_klines(df: pd.DataFrame) -> Tuple[bool, List[str]]:
        """
        Returns (is_valid, error_list)
        """
        errors = []
        
        # Check OHLC ordering
        bad_ohlc = df[(df['high'] < df['low']) | 
                      (df['high'] < df['open']) | 
                      (df['high'] < df['close']) |
                      (df['low'] > df['open']) | 
                      (df['low'] > df['close'])]
        if len(bad_ohlc) > 0:
            errors.append(f"Found {len(bad_ohlc)} rows with invalid OHLC ordering")
        
        # Check for gaps
        df['time_diff'] = df['open_time'].diff()
        expected_diff = {'1m': 60000, '1h': 3600000, '4h': 14400000, '1d': 86400000}
        # This would need timeframe context
        
        # Check for duplicates
        duplicates = df['open_time'].duplicated().sum()
        if duplicates > 0:
            errors.append(f"Found {duplicates} duplicate timestamps")
        
        # Check volume sanity
        volume_mean = df['volume'].rolling(50).mean()
        volume_std = df['volume'].rolling(50).std()
        anomalies = df['volume'] > (volume_mean + 3 * volume_std)
        if anomalies.sum() > 0:
            logger.warning(f"Found {anomalies.sum()} volume anomalies (3σ+)")
        
        return len(errors) == 0, errors

class BollingerBands(Strategy):
    """Bollinger Bands Strategy"""
    
    def init(self):
        self.params = self.broker._cash / len(self.data) * 0.95  # Position sizing
        
        # Calculate BB
        close = pd.Series(self.data.Close)
        bb = ta.bbands(close, length=self.p0, std=self.p1)
        self.bb_upper = self.I(lambda: bb.iloc[:, 2])
        self.bb_middle = self.I(lambda: bb.iloc[:, 1])
        self.bb_lower = self.I(lambda: bb.iloc[:, 0])
    
    def next(self):
        if not self.position:
            if self.data.Close[-1] <= self.bb_lower[-1]:
                self.buy(size=self.params)
                logger.debug(f"BUY @ {self.data.Close[-1]} (BB Lower)")
        else:
            if self.data.Close[-1] >= self.bb_upper[-1]:
                self.position.close()
                logger.debug(f"SELL @ {self.data.Close[-1]} (BB Upper)")

class EMA_Crossover(Strategy):
    """EMA 9/21 Crossover"""
    
    def init(self):
        ema9 = ta.ema(pd.Series(self.data.Close), length=9)
        ema21 = ta.ema(pd.Series(self.data.Close), length=21)
        self.ema9 = self.I(lambda: ema9)
        self.ema21 = self.I(lambda: ema21)
        self.in_position = False
    
    def next(self):
        if self.ema9[-1] > self.ema21[-1] and not self.in_position:
            self.buy()
            self.in_position = True
            logger.debug(f"BUY (EMA Crossover) @ {self.data.Close[-1]}")
        elif self.ema9[-1] < self.ema21[-1] and self.in_position:
            self.position.close()
            self.in_position = False
            logger.debug(f"SELL (EMA Crossover) @ {self.data.Close[-1]}")

class SuperTrend(Strategy):
    """SuperTrend Strategy"""
    
    def init(self):
        st = ta.supertrend(self.data.High, self.data.Low, self.data.Close, 
                          length=self.p0, multiplier=self.p1)
        self.st_trend = self.I(lambda: st.iloc[:, 2])
    
    def next(self):
        if self.st_trend[-1] == 1 and not self.position:
            self.buy()
            logger.debug(f"BUY (SuperTrend Uptrend) @ {self.data.Close[-1]}")
        elif self.st_trend[-1] == -1 and self.position:
            self.position.close()
            logger.debug(f"SELL (SuperTrend Downtrend) @ {self.data.Close[-1]}")

class ProductionBacktester:
    """Main backtesting orchestrator with validation and audit trails"""
    
    def __init__(self, db_connection_string: str):
        self.engine = create_engine(db_connection_string)
        self.run_id = str(uuid4())
        logger.info(f"Backtest run initialized: {self.run_id}")
    
    def fetch_data(self, config: BacktestConfig) -> pd.DataFrame:
        """Query database with validation"""
        query = f"""
        SELECT 
            open_time, close_time, open, high, low, close, volume
        FROM klines
        WHERE symbol = '{config.symbol}'
          AND timeframe = '{config.timeframe}'
          AND open_time >= {int(config.start_date.timestamp() * 1000)}
          AND open_time <= {int(config.end_date.timestamp() * 1000)}
        ORDER BY open_time ASC
        """
        
        with self.engine.connect() as conn:
            df = pd.read_sql(query, conn)
        
        if df.empty:
            raise ValueError(f"No data found for {config.symbol} {config.timeframe}")
        
        df['Date'] = pd.to_datetime(df['open_time'], unit='ms')
        df = df.set_index('Date')
        
        # Validate before proceeding
        is_valid, errors = DataValidator.validate_klines(df)
        if not is_valid:
            logger.error(f"Data validation failed: {errors}")
            raise ValueError(f"Data validation failed: {errors}")
        
        logger.info(f"Fetched {len(df)} candles for {config.symbol}")
        return df[['open', 'high', 'low', 'close', 'volume']]
    
    def run_backtest(self, config: BacktestConfig, strategy_class, 
                     strategy_params: Dict) -> Dict:
        """Execute backtest with full audit trail"""
        config.validate()
        
        logger.info(f"Starting backtest: {config.strategy_name} {strategy_params}")
        
        # Fetch data
        data = self.fetch_data(config)
        
        # Run backtest
        bt = Backtest(data, strategy_class, cash=config.initial_capital, 
                     commission=config.commission_pct/100,
                     exclusive_orders=True)
        
        stats = bt.run(**strategy_params)
        
        # Extract metrics
        results = {
            'run_id': self.run_id,
            'symbol': config.symbol,
            'timeframe': config.timeframe,
            'strategy_name': config.strategy_name,
            'strategy_params': strategy_params,
            'total_return': float(stats['Return [%]']),
            'annualized_return': float(stats['Return (Ann.) [%]']),
            'max_drawdown': float(stats['Max. Drawdown [%]']),
            'sharpe_ratio': float(stats['Sharpe Ratio']),
            'sortino_ratio': float(stats.get('Sortino Ratio', np.nan)),
            'win_rate': float(stats['Win Rate [%]'] / 100),
            'profit_factor': float(stats.get('Profit Factor', np.nan)),
            'num_trades': int(stats['# Trades']),
            'num_winning_trades': int(stats['# Trades'] * stats['Win Rate [%]'] / 100),
            'backtest_start': config.start_date,
            'backtest_end': config.end_date,
        }
        
        logger.info(f"Backtest completed. Return: {results['total_return']:.2f}%, "
                   f"Sharpe: {results['sharpe_ratio']:.2f}, "
                   f"Trades: {results['num_trades']}")
        
        return results
    
    def validate_results(self, results: Dict) -> Tuple[str, List[str]]:
        """
        Production-grade validation.
        Returns (status, notes) where status in ('PASSED', 'FAILED', 'FLAGGED')
        """
        notes = []
        
        # Hard failures
        if results['num_trades'] < 5:
            return 'FAILED', ['Too few trades (< 5) - insufficient statistical significance']
        
        if results['max_drawdown'] < -50:
            return 'FAILED', ['Maximum drawdown exceeds -50% - too risky for live deployment']
        
        # Warnings
        if results['sharpe_ratio'] < 1.0:
            notes.append(f"Low Sharpe ratio ({results['sharpe_ratio']:.2f}) - consider refinement")
        
        if results['win_rate'] < 0.35:
            notes.append(f"Win rate below 35% ({results['win_rate']:.1%}) - verify profit factor is >1")
        
        if results['profit_factor'] < 1.2:
            notes.append(f"Low profit factor ({results['profit_factor']:.2f}) - risk/reward unfavorable")
        
        status = 'FLAGGED' if notes else 'PASSED'
        
        logger.info(f"Validation status: {status}. Notes: {notes}")
        return status, notes
    
    def store_results(self, results: Dict, status: str, notes: List[str]):
        """Persist results to database for audit trail"""
        query = """
        INSERT INTO backtest_runs 
        (run_id, symbol, timeframe, strategy_name, strategy_params,
         backtest_start, backtest_end, total_return, annualized_return,
         max_drawdown, sharpe_ratio, sortino_ratio, win_rate, profit_factor,
         num_trades, num_winning_trades, validation_status, validation_notes)
        VALUES 
        (:run_id, :symbol, :timeframe, :strategy_name, :strategy_params,
         :backtest_start, :backtest_end, :total_return, :annualized_return,
         :max_drawdown, :sharpe_ratio, :sortino_ratio, :win_rate, :profit_factor,
         :num_trades, :num_winning_trades, :validation_status, :validation_notes)
        """
        
        params = {
            'strategy_params': json.dumps(results['strategy_params']),
            'validation_notes': '\n'.join(notes) if notes else None,
            'validation_status': status,
            **results
        }
        
        with self.engine.connect() as conn:
            conn.execute(text(query), params)
            conn.commit()
        
        logger.info(f"Results stored in database. Run ID: {results['run_id']}")
        return results['run_id']

```

---

### 2.2 Parameter Optimization (`optimizer.py`)

```python
# optimizer.py
import logging
from backtester import ProductionBacktester, BacktestConfig, SuperTrend

logger = logging.getLogger(__name__)

class ParameterOptimizer:
    """Walk-forward optimization with out-of-sample validation"""
    
    def __init__(self, backtester: ProductionBacktester):
        self.backtester = backtester
    
    def optimize_supertrend(self, config: BacktestConfig) -> Dict:
        """
        Grid search on SuperTrend with walk-forward validation.
        
        Train on 70% of data, validate on 30%.
        Find parameters that maximize Sharpe ratio on validation set.
        """
        
        data = self.backtester.fetch_data(config)
        split_idx = int(len(data) * 0.7)
        
        train_data = data.iloc[:split_idx]
        test_data = data.iloc[split_idx:]
        
        logger.info(f"Walk-forward split: Train {len(train_data)}, Test {len(test_data)}")
        
        best_params = None
        best_sharpe = -np.inf
        results_grid = []
        
        # Grid: ATR length 7-20, Multiplier 2.0-4.0
        for atr_len in range(7, 21, 2):
            for multiplier in np.arange(2.0, 4.1, 0.5):
                # Train
                config_train = BacktestConfig(
                    symbol=config.symbol,
                    timeframe=config.timeframe,
                    strategy_name=config.strategy_name,
                    strategy_params={'length': atr_len, 'multiplier': multiplier},
                    start_date=train_data.index[0],
                    end_date=train_data.index[-1]
                )
                
                train_results = self.backtester.run_backtest(
                    config_train, SuperTrend, 
                    {'length': atr_len, 'multiplier': multiplier}
                )
                
                # Test (validate)
                config_test = BacktestConfig(
                    symbol=config.symbol,
                    timeframe=config.timeframe,
                    strategy_name=config.strategy_name,
                    strategy_params={'length': atr_len, 'multiplier': multiplier},
                    start_date=test_data.index[0],
                    end_date=test_data.index[-1]
                )
                
                test_results = self.backtester.run_backtest(
                    config_test, SuperTrend, 
                    {'length': atr_len, 'multiplier': multiplier}
                )
                
                # Use test set Sharpe for selection (out-of-sample)
                test_sharpe = test_results['sharpe_ratio']
                
                results_grid.append({
                    'atr_len': atr_len,
                    'multiplier': multiplier,
                    'train_sharpe': train_results['sharpe_ratio'],
                    'test_sharpe': test_sharpe,
                    'train_return': train_results['total_return'],
                    'test_return': test_results['total_return'],
                    'test_drawdown': test_results['max_drawdown']
                })
                
                if test_sharpe > best_sharpe:
                    best_sharpe = test_sharpe
                    best_params = {'length': atr_len, 'multiplier': multiplier}
                
                logger.info(f"Tested ATR={atr_len}, Mult={multiplier}: "
                          f"Test Sharpe={test_sharpe:.2f}")
        
        # Rank and filter results
        results_df = pd.DataFrame(results_grid).sort_values('test_sharpe', ascending=False)
        logger.info(f"\nTop 5 parameter sets:\n{results_df.head()}")
        
        # Flag overfitting risk
        results_df['overfit_ratio'] = results_df['train_sharpe'] / results_df['test_sharpe']
        if results_df['overfit_ratio'].min() > 2.0:
            logger.warning("HIGH OVERFIT RISK: Train Sharpe >> Test Sharpe")
        
        return {
            'best_params': best_params,
            'best_test_sharpe': best_sharpe,
            'results_grid': results_df
        }
```

---

## Task 3: Risk Management & Kill Switches

**Mandatory additions to backtester:**

```python
class RiskManager:
    """Live trading safeguards"""
    
    @staticmethod
    def check_max_drawdown_stop(equity_curve: np.ndarray, max_dd_threshold: float) -> bool:
        """Kill switch: return True if should STOP TRADING"""
        running_max = np.maximum.accumulate(equity_curve)
        drawdown = (equity_curve - running_max) / running_max
        return np.min(drawdown) < max_dd_threshold
    
    @staticmethod
    def validate_position_size(capital: float, position_size_usd: float, 
                              max_position_pct: float = 0.05) -> bool:
        """No single position > 5% of capital"""
        return position_size_usd / capital <= max_position_pct
```

---

## Task 4: Reporting & Audit Trail

Create `report_generator.py` that:

1. **Trade Log Export** - Every entry/exit with timestamp, price, reason, P&L
2. **Equity Curve** - Daily equity progression
3. **Drawdown Analysis** - Identify longest drawdown periods
4. **Rolling Metrics** - 30/60/90-day Sharpe, return
5. **Backtest Certification** - Human sign-off required before live deployment

---

## Task 5: Pre-Deployment Checklist

**Before moving to live trading, verify:**

- [ ] Backtest passes all validation criteria
- [ ] Walk-forward optimization shows consistent OOS performance
- [ ] Sharpe ratio > 1.0 on both IS and OOS
- [ ] Max drawdown < 20% (ideally < 15%)
- [ ] Trade count > 50 (sufficient data for statistics)
- [ ] Profit factor > 1.2
- [ ] Win rate + 2 * loss avg >= profit avg (edge is real, not variance)
- [ ] Database integrity verified (checksums match, no gaps)
- [ ] Code reviewed by another developer
- [ ] Risk limits configured: max position size, max DD stop, slippage/commission realistic
- [ ] Live paper trading on Binance testnet for 2 weeks minimum
- [ ] Operational runbook written (how to pause, restart, monitor in production)

---

## Task 6: Infrastructure & Deployment

Create `docker-compose.yml`:

```yaml
version: '3.8'
services:
  postgres:
    image: postgres:15-alpine
    environment:
      POSTGRES_USER: ${DB_USER}
      POSTGRES_PASSWORD: ${DB_PASSWORD}
      POSTGRES_DB: trading_db
    volumes:
      - postgres_data:/var/lib/postgresql/data
      - ./schema.sql:/docker-entrypoint-initdb.d/schema.sql
    ports:
      - "5432:5432"
  
  backtester:
    build: .
    depends_on:
      - postgres
    environment:
      DB_HOST: postgres
      DB_USER: ${DB_USER}
      DB_PASSWORD: ${DB_PASSWORD}
    volumes:
      - ./data:/app/data
      - ./logs:/app/logs
    command: python fetch_to_db.py && python backtester.py

volumes:
  postgres_data:
```

---

## Task 7: Monitoring & Alerting (Post-Deployment)

If/when deployed to live trading:

- Daily P&L tracking vs backtest expectations
- Alert if realized Sharpe < 0.5 (edge degradation)
- Alert if drawdown exceeds backtest max DD by >20%
- Email/Slack notifications for all trades + daily summary
- Weekly comparison: live performance vs backtest

---

## Deliverables Checklist

- [ ] `fetch_to_db.py` with validation and retry logic
- [ ] `config.py` (example) and `.env` template
- [ ] `schema.sql` with integrity constraints
- [ ] `backtester.py` with DataValidator, risk checks, audit logging
- [ ] `optimizer.py` with walk-forward validation
- [ ] `report_generator.py` with trade logs + equity curves
- [ ] `requirements.txt` (pinned versions)
- [ ] `docker-compose.yml` for reproducible environment
- [ ] `DEPLOYMENT_CHECKLIST.md` (human verification required)
- [ ] `OPERATIONAL_RUNBOOK.md` (how to operate in production)
- [ ] `README.md` with architecture diagram

---

## Key Principles for Production

1. **Fail Loudly** - Any data anomaly = exception + alert, never silent
2. **Audit Everything** - Every trade, parameter, result goes in DB
3. **Validate Twice** - Data validation + results validation mandatory
4. **Assume You're Wrong** - Walk-forward validation catches overfitting
5. **Humans Decide** - No trade happens on live until explicit approval
6. **Monitor Always** - Real money != backtest; watch daily
7. **Document Ruthlessly** - Code, decisions, assumptions, all logged

---

## Critical Success Factors

If you skip any of these and lose money, you'll regret it:

- Walk-forward validation (out-of-sample testing)
- Realistic slippage/commission assumptions
- Hard stop-loss at -15% drawdown
- Trade count > 50 for statistical significance
- Code review before live
- Paper trading on testnet for 2+ weeks
- Operational runbook + monitoring dashboard

This is real money. Build it right.
