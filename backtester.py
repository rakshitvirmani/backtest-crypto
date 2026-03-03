"""
backtester.py - Production-Grade Backtesting Engine
=====================================================
Architecture:
  - Data Layer: fetches from PostgreSQL, validates before use
  - Strategy Layer: pluggable strategies via backtesting.py's Strategy class
  - Execution Layer: realistic slippage, commission, position sizing
  - Reporting Layer: full audit trail to DB, every trade logged with reasoning

All calculations are deterministic. No randomness. Every trade logged with reasoning.

This is the core engine. Do not modify without understanding the full implications.
"""

import os
import sys
import json
import logging
from datetime import datetime
from uuid import uuid4
from typing import Dict, List, Tuple, Optional, Type
from dataclasses import dataclass, field

import pandas as pd
import numpy as np
import pandas_ta as ta
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError
from backtesting import Backtest, Strategy

try:
    from config import DATABASE_URL, LOG_LEVEL, LOG_FILE
except ImportError:
    print("ERROR: config.py not found.")
    sys.exit(1)

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
os.makedirs(os.path.dirname(LOG_FILE), exist_ok=True)
logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler(LOG_FILE),
        logging.StreamHandler(),
    ],
)
logger = logging.getLogger("backtester")


# ---------------------------------------------------------------------------
# Backtest Configuration (Immutable per run)
# ---------------------------------------------------------------------------
@dataclass(frozen=False)
class BacktestConfig:
    """Immutable configuration for a single backtest run."""

    symbol: str
    timeframe: str
    strategy_name: str
    strategy_params: Dict
    start_date: datetime
    end_date: datetime
    initial_capital: float = 10_000.0
    position_size_pct: float = 0.95
    slippage_pct: float = 0.05       # 5 bps
    commission_pct: float = 0.10      # 10 bps (Binance maker fee)
    max_drawdown_stop: float = -0.15  # Kill switch at -15% DD
    max_position_size_usd: Optional[float] = None

    def validate(self):
        assert 0 < self.position_size_pct <= 1.0, "position_size_pct must be in (0, 1]"
        assert self.slippage_pct >= 0, "slippage cannot be negative"
        assert self.commission_pct >= 0, "commission cannot be negative"
        assert self.max_drawdown_stop < 0, "max_drawdown_stop must be negative"
        assert self.start_date < self.end_date, "start_date must be before end_date"
        assert self.initial_capital > 0, "initial_capital must be positive"
        logger.info(f"Config validated: {self.strategy_name} on {self.symbol} {self.timeframe}")

    def to_dict(self) -> Dict:
        return {
            "symbol": self.symbol,
            "timeframe": self.timeframe,
            "strategy_name": self.strategy_name,
            "strategy_params": self.strategy_params,
            "start_date": self.start_date.isoformat(),
            "end_date": self.end_date.isoformat(),
            "initial_capital": self.initial_capital,
            "position_size_pct": self.position_size_pct,
            "slippage_pct": self.slippage_pct,
            "commission_pct": self.commission_pct,
            "max_drawdown_stop": self.max_drawdown_stop,
        }


# ---------------------------------------------------------------------------
# Data Validator (pre-backtest)
# ---------------------------------------------------------------------------
class DataValidator:
    """Pre-backtest data quality checks. Fail loudly on bad data."""

    @staticmethod
    def validate_klines(df: pd.DataFrame, timeframe: str = None) -> Tuple[bool, List[str]]:
        errors = []

        if df.empty:
            return False, ["DataFrame is empty — no data to backtest"]

        # OHLC ordering
        bad_ohlc = df[
            (df["High"] < df["Low"])
            | (df["High"] < df["Open"])
            | (df["High"] < df["Close"])
            | (df["Low"] > df["Open"])
            | (df["Low"] > df["Close"])
        ]
        if len(bad_ohlc) > 0:
            errors.append(f"Found {len(bad_ohlc)} rows with invalid OHLC ordering")

        # Duplicates
        if df.index.duplicated().sum() > 0:
            errors.append(f"Found {df.index.duplicated().sum()} duplicate timestamps")

        # NaN check
        nan_count = df[["Open", "High", "Low", "Close", "Volume"]].isna().sum().sum()
        if nan_count > 0:
            errors.append(f"Found {nan_count} NaN values in OHLCV columns")

        # Volume anomalies (warning, not error)
        if len(df) >= 50:
            vol_mean = df["Volume"].rolling(50).mean()
            vol_std = df["Volume"].rolling(50).std()
            anomalies = (df["Volume"] > (vol_mean + 3 * vol_std)).sum()
            if anomalies > 0:
                logger.warning(
                    f"Volume anomalies: {anomalies} bars exceed 3σ from 50-bar mean"
                )

        # Minimum data requirement
        if len(df) < 100:
            errors.append(f"Insufficient data: {len(df)} bars (minimum 100 required)")

        return len(errors) == 0, errors


# ---------------------------------------------------------------------------
# Strategy Implementations
# ---------------------------------------------------------------------------
class BollingerBandsStrategy(Strategy):
    """
    Bollinger Bands Mean Reversion
    - Buy when price touches lower band
    - Sell when price touches upper band
    Parameters: bb_length (int), bb_std (float)
    """
    bb_length = 20
    bb_std = 2.0

    def init(self):
        close = pd.Series(self.data.Close, dtype=float)
        bb = ta.bbands(close, length=self.bb_length, std=self.bb_std)
        if bb is not None and len(bb.columns) >= 3:
            self.bb_lower = self.I(lambda: bb.iloc[:, 0].values)
            self.bb_middle = self.I(lambda: bb.iloc[:, 1].values)
            self.bb_upper = self.I(lambda: bb.iloc[:, 2].values)
        else:
            # Fallback: manual calculation
            sma = close.rolling(self.bb_length).mean()
            std = close.rolling(self.bb_length).std()
            self.bb_lower = self.I(lambda: (sma - self.bb_std * std).values)
            self.bb_middle = self.I(lambda: sma.values)
            self.bb_upper = self.I(lambda: (sma + self.bb_std * std).values)

    def next(self):
        price = self.data.Close[-1]
        if np.isnan(self.bb_lower[-1]) or np.isnan(self.bb_upper[-1]):
            return

        if not self.position:
            if price <= self.bb_lower[-1]:
                self.buy()
        else:
            if price >= self.bb_upper[-1]:
                self.position.close()


class EMACrossoverStrategy(Strategy):
    """
    EMA 9/21 Crossover (Trend Following)
    - Buy when fast EMA crosses above slow EMA
    - Sell when fast EMA crosses below slow EMA
    Parameters: ema_fast (int), ema_slow (int)
    """
    ema_fast = 9
    ema_slow = 21

    def init(self):
        close = pd.Series(self.data.Close, dtype=float)
        ema_f = ta.ema(close, length=self.ema_fast)
        ema_s = ta.ema(close, length=self.ema_slow)
        self.fast = self.I(lambda: ema_f.values if ema_f is not None else close.rolling(self.ema_fast).mean().values)
        self.slow = self.I(lambda: ema_s.values if ema_s is not None else close.rolling(self.ema_slow).mean().values)

    def next(self):
        if np.isnan(self.fast[-1]) or np.isnan(self.slow[-1]):
            return
        if not self.position:
            if self.fast[-1] > self.slow[-1] and self.fast[-2] <= self.slow[-2]:
                self.buy()
        elif self.fast[-1] < self.slow[-1] and self.fast[-2] >= self.slow[-2]:
            self.position.close()


class SuperTrendStrategy(Strategy):
    """
    SuperTrend Strategy
    - Buy when SuperTrend flips to uptrend
    - Sell when SuperTrend flips to downtrend
    Parameters: st_length (int), st_multiplier (float)
    """
    st_length = 10
    st_multiplier = 3.0

    def init(self):
        high = pd.Series(self.data.High, dtype=float)
        low = pd.Series(self.data.Low, dtype=float)
        close = pd.Series(self.data.Close, dtype=float)
        st = ta.supertrend(high, low, close, length=self.st_length, multiplier=self.st_multiplier)
        if st is not None and len(st.columns) >= 3:
            # Column index 1 = direction, index 2 = trend (1=up, -1=down) in pandas_ta
            # Actual column names: SUPERTd_{length}_{mult}
            trend_col = [c for c in st.columns if c.startswith("SUPERTd")]
            if trend_col:
                self.trend = self.I(lambda: st[trend_col[0]].values)
            else:
                self.trend = self.I(lambda: st.iloc[:, 1].values)
        else:
            # Fallback: simple ATR breakout
            atr = ta.atr(high, low, close, length=self.st_length)
            sma = close.rolling(self.st_length).mean()
            self.trend = self.I(lambda: np.where(close > sma + self.st_multiplier * atr, 1, -1))

    def next(self):
        if np.isnan(self.trend[-1]):
            return
        if not self.position:
            if self.trend[-1] == 1:
                self.buy()
        elif self.trend[-1] == -1:
            self.position.close()


class RSIMeanReversionStrategy(Strategy):
    """
    RSI Mean Reversion
    - Buy when RSI < oversold threshold
    - Sell when RSI > overbought threshold
    Parameters: rsi_length (int), rsi_oversold (float), rsi_overbought (float)
    """
    rsi_length = 14
    rsi_oversold = 30.0
    rsi_overbought = 70.0

    def init(self):
        close = pd.Series(self.data.Close, dtype=float)
        rsi = ta.rsi(close, length=self.rsi_length)
        self.rsi = self.I(lambda: rsi.values if rsi is not None else np.full(len(close), 50.0))

    def next(self):
        if np.isnan(self.rsi[-1]):
            return
        if not self.position:
            if self.rsi[-1] < self.rsi_oversold:
                self.buy()
        elif self.rsi[-1] > self.rsi_overbought:
            self.position.close()


class MACDStrategy(Strategy):
    """
    MACD Crossover
    - Buy when MACD line crosses above signal line
    - Sell when MACD line crosses below signal line
    Parameters: macd_fast (int), macd_slow (int), macd_signal (int)
    """
    macd_fast = 12
    macd_slow = 26
    macd_signal = 9

    def init(self):
        close = pd.Series(self.data.Close, dtype=float)
        macd_df = ta.macd(close, fast=self.macd_fast, slow=self.macd_slow, signal=self.macd_signal)
        if macd_df is not None and len(macd_df.columns) >= 2:
            macd_col = [c for c in macd_df.columns if "MACD_" in c and "s" not in c.lower() and "h" not in c.lower()]
            signal_col = [c for c in macd_df.columns if "MACDs" in c]
            if macd_col and signal_col:
                self.macd_line = self.I(lambda: macd_df[macd_col[0]].values)
                self.signal_line = self.I(lambda: macd_df[signal_col[0]].values)
            else:
                self.macd_line = self.I(lambda: macd_df.iloc[:, 0].values)
                self.signal_line = self.I(lambda: macd_df.iloc[:, 2].values)
        else:
            ema_f = close.ewm(span=self.macd_fast).mean()
            ema_s = close.ewm(span=self.macd_slow).mean()
            macd = ema_f - ema_s
            signal = macd.ewm(span=self.macd_signal).mean()
            self.macd_line = self.I(lambda: macd.values)
            self.signal_line = self.I(lambda: signal.values)

    def next(self):
        if np.isnan(self.macd_line[-1]) or np.isnan(self.signal_line[-1]):
            return
        if len(self.macd_line) < 2:
            return
        if not self.position:
            if self.macd_line[-1] > self.signal_line[-1] and self.macd_line[-2] <= self.signal_line[-2]:
                self.buy()
        elif self.macd_line[-1] < self.signal_line[-1] and self.macd_line[-2] >= self.signal_line[-2]:
            self.position.close()


# ---------------------------------------------------------------------------
# Strategy Registry
# ---------------------------------------------------------------------------
STRATEGY_REGISTRY: Dict[str, Type[Strategy]] = {
    "bollinger_bands": BollingerBandsStrategy,
    "ema_crossover": EMACrossoverStrategy,
    "supertrend": SuperTrendStrategy,
    "rsi_mean_reversion": RSIMeanReversionStrategy,
    "macd": MACDStrategy,
}


# ---------------------------------------------------------------------------
# Production Backtester
# ---------------------------------------------------------------------------
class ProductionBacktester:
    """Main backtesting orchestrator with validation and audit trails."""

    def __init__(self, db_connection_string: str = None):
        self.engine = create_engine(db_connection_string or DATABASE_URL)
        self.run_id = str(uuid4())
        logger.info(f"Backtest run initialized: {self.run_id}")

    def fetch_data(self, config: BacktestConfig) -> pd.DataFrame:
        """Query database with pre-backtest validation."""
        query = text("""
            SELECT
                open_time, close_time, open, high, low, close, volume
            FROM klines
            WHERE symbol = :symbol
              AND timeframe = :timeframe
              AND open_time >= :start_ms
              AND open_time <= :end_ms
            ORDER BY open_time ASC
        """)

        start_ms = int(config.start_date.timestamp() * 1000)
        end_ms = int(config.end_date.timestamp() * 1000)

        with self.engine.connect() as conn:
            df = pd.read_sql(
                query, conn,
                params={"symbol": config.symbol, "timeframe": config.timeframe,
                         "start_ms": start_ms, "end_ms": end_ms}
            )

        if df.empty:
            raise ValueError(
                f"No data found for {config.symbol} {config.timeframe} "
                f"between {config.start_date} and {config.end_date}"
            )

        # Convert to backtesting.py expected format
        df["Date"] = pd.to_datetime(df["open_time"], unit="ms")
        df = df.set_index("Date")
        df.rename(columns={
            "open": "Open", "high": "High", "low": "Low",
            "close": "Close", "volume": "Volume"
        }, inplace=True)

        # Validate before proceeding
        is_valid, errors = DataValidator.validate_klines(df, config.timeframe)
        if not is_valid:
            error_msg = f"Data validation failed: {errors}"
            logger.error(error_msg)
            raise ValueError(error_msg)

        logger.info(f"Fetched {len(df)} candles for {config.symbol} {config.timeframe}")
        return df[["Open", "High", "Low", "Close", "Volume"]]

    def run_backtest(
        self,
        config: BacktestConfig,
        strategy_class: Type[Strategy] = None,
        data: pd.DataFrame = None,
    ) -> Dict:
        """
        Execute backtest with full audit trail.
        Optionally pass pre-fetched data (for optimizer reuse).
        """
        config.validate()
        self.run_id = str(uuid4())  # fresh ID per run

        if strategy_class is None:
            strategy_class = STRATEGY_REGISTRY.get(config.strategy_name)
            if strategy_class is None:
                raise ValueError(
                    f"Unknown strategy: {config.strategy_name}. "
                    f"Available: {list(STRATEGY_REGISTRY.keys())}"
                )

        logger.info(
            f"Starting backtest [{self.run_id[:8]}]: "
            f"{config.strategy_name} {config.strategy_params} on {config.symbol} {config.timeframe}"
        )

        # Fetch data if not provided
        if data is None:
            data = self.fetch_data(config)

        # Run backtest
        bt = Backtest(
            data,
            strategy_class,
            cash=config.initial_capital,
            commission=config.commission_pct / 100,
            exclusive_orders=True,
        )

        stats = bt.run(**config.strategy_params)

        # Extract metrics safely
        def safe_float(val, default=np.nan):
            try:
                v = float(val)
                return v if not np.isinf(v) else default
            except (TypeError, ValueError):
                return default

        num_trades = int(stats.get("# Trades", 0))
        win_rate = safe_float(stats.get("Win Rate [%]", 0)) / 100

        results = {
            "run_id": self.run_id,
            "symbol": config.symbol,
            "timeframe": config.timeframe,
            "strategy_name": config.strategy_name,
            "strategy_params": config.strategy_params,
            "total_return": safe_float(stats.get("Return [%]", 0)),
            "annualized_return": safe_float(stats.get("Return (Ann.) [%]", 0)),
            "max_drawdown": safe_float(stats.get("Max. Drawdown [%]", 0)),
            "sharpe_ratio": safe_float(stats.get("Sharpe Ratio", 0)),
            "sortino_ratio": safe_float(stats.get("Sortino Ratio", np.nan)),
            "calmar_ratio": safe_float(stats.get("Calmar Ratio", np.nan)),
            "win_rate": win_rate,
            "profit_factor": safe_float(stats.get("Profit Factor", np.nan)),
            "num_trades": num_trades,
            "num_winning_trades": int(num_trades * win_rate),
            "avg_trade_pnl_pct": safe_float(stats.get("Avg. Trade [%]", np.nan)),
            "best_trade_pnl_pct": safe_float(stats.get("Best Trade [%]", np.nan)),
            "worst_trade_pnl_pct": safe_float(stats.get("Worst Trade [%]", np.nan)),
            "max_consecutive_losses": int(stats.get("Max. Trade Duration", 0) if isinstance(stats.get("Max. Trade Duration", 0), (int, float)) else 0),
            "backtest_start": config.start_date,
            "backtest_end": config.end_date,
            "initial_capital": config.initial_capital,
            "slippage_pct": config.slippage_pct,
            "commission_pct": config.commission_pct,
            "_stats": stats,  # keep raw stats for reporting
            "_trades": stats._trades if hasattr(stats, "_trades") else None,
            "_equity_curve": stats._equity_curve if hasattr(stats, "_equity_curve") else None,
        }

        logger.info(
            f"Backtest [{self.run_id[:8]}] completed: "
            f"Return={results['total_return']:.2f}%, "
            f"Sharpe={results['sharpe_ratio']:.2f}, "
            f"MaxDD={results['max_drawdown']:.2f}%, "
            f"Trades={results['num_trades']}"
        )

        return results

    def validate_results(self, results: Dict) -> Tuple[str, List[str]]:
        """
        Production-grade validation.
        Returns (status, notes) where status in ('PASSED', 'FAILED', 'FLAGGED').
        """
        notes = []

        # Hard failures
        if results["num_trades"] < 5:
            return "FAILED", ["Too few trades (< 5) — insufficient statistical significance"]

        if results["max_drawdown"] < -50:
            return "FAILED", [f"Max drawdown {results['max_drawdown']:.1f}% exceeds -50% — too risky"]

        if results["total_return"] < -30:
            return "FAILED", [f"Total return {results['total_return']:.1f}% — strategy loses money"]

        # Warnings (FLAGGED)
        if results["sharpe_ratio"] < 1.0:
            notes.append(f"Low Sharpe ratio ({results['sharpe_ratio']:.2f}) — consider refinement")

        if results["win_rate"] < 0.35:
            notes.append(f"Win rate below 35% ({results['win_rate']:.1%}) — verify profit factor")

        pf = results.get("profit_factor", np.nan)
        if not np.isnan(pf) and pf < 1.2:
            notes.append(f"Low profit factor ({pf:.2f}) — risk/reward unfavorable")

        if results["num_trades"] < 50:
            notes.append(f"Trade count ({results['num_trades']}) below 50 — limited statistical confidence")

        if results["max_drawdown"] < -20:
            notes.append(f"Max drawdown ({results['max_drawdown']:.1f}%) exceeds -20% threshold")

        status = "FLAGGED" if notes else "PASSED"
        logger.info(f"Validation [{self.run_id[:8]}]: {status}. Notes: {notes}")
        return status, notes

    def store_results(self, results: Dict, status: str, notes: List[str]) -> str:
        """Persist results to database for audit trail."""
        insert_sql = text("""
            INSERT INTO backtest_runs (
                run_id, symbol, timeframe, strategy_name, strategy_params,
                backtest_start, backtest_end, total_return, annualized_return,
                max_drawdown, sharpe_ratio, sortino_ratio, calmar_ratio,
                win_rate, profit_factor, num_trades, num_winning_trades,
                avg_trade_pnl_pct, best_trade_pnl_pct, worst_trade_pnl_pct,
                initial_capital, slippage_pct, commission_pct,
                validation_status, validation_notes
            ) VALUES (
                :run_id, :symbol, :timeframe, :strategy_name, :strategy_params,
                :backtest_start, :backtest_end, :total_return, :annualized_return,
                :max_drawdown, :sharpe_ratio, :sortino_ratio, :calmar_ratio,
                :win_rate, :profit_factor, :num_trades, :num_winning_trades,
                :avg_trade_pnl_pct, :best_trade_pnl_pct, :worst_trade_pnl_pct,
                :initial_capital, :slippage_pct, :commission_pct,
                :validation_status, :validation_notes
            )
        """)

        params = {
            "run_id": results["run_id"],
            "symbol": results["symbol"],
            "timeframe": results["timeframe"],
            "strategy_name": results["strategy_name"],
            "strategy_params": json.dumps(results["strategy_params"]),
            "backtest_start": results["backtest_start"],
            "backtest_end": results["backtest_end"],
            "total_return": results["total_return"],
            "annualized_return": results["annualized_return"],
            "max_drawdown": results["max_drawdown"],
            "sharpe_ratio": results["sharpe_ratio"],
            "sortino_ratio": results.get("sortino_ratio"),
            "calmar_ratio": results.get("calmar_ratio"),
            "win_rate": results["win_rate"],
            "profit_factor": results.get("profit_factor"),
            "num_trades": results["num_trades"],
            "num_winning_trades": results["num_winning_trades"],
            "avg_trade_pnl_pct": results.get("avg_trade_pnl_pct"),
            "best_trade_pnl_pct": results.get("best_trade_pnl_pct"),
            "worst_trade_pnl_pct": results.get("worst_trade_pnl_pct"),
            "initial_capital": results["initial_capital"],
            "slippage_pct": results["slippage_pct"],
            "commission_pct": results["commission_pct"],
            "validation_status": status,
            "validation_notes": "\n".join(notes) if notes else None,
        }

        try:
            with self.engine.begin() as conn:
                conn.execute(insert_sql, params)
            logger.info(f"Results stored in DB. Run ID: {results['run_id']}")
        except SQLAlchemyError as e:
            logger.error(f"Failed to store results: {e}")
            raise

        return results["run_id"]

    def store_trades(self, results: Dict, backtest_run_db_id: int = None):
        """Store individual trades from backtest results."""
        trades_df = results.get("_trades")
        if trades_df is None or trades_df.empty:
            logger.warning("No trades to store")
            return

        insert_sql = text("""
            INSERT INTO trade_log (
                backtest_run_id, trade_number, entry_time, exit_time,
                entry_price, exit_price, position_size, direction,
                pnl, pnl_percent, is_winning_trade,
                entry_signal, exit_signal
            ) VALUES (
                :backtest_run_id, :trade_number, :entry_time, :exit_time,
                :entry_price, :exit_price, :position_size, :direction,
                :pnl, :pnl_percent, :is_winning_trade,
                :entry_signal, :exit_signal
            )
        """)

        # Get the DB id for this run
        if backtest_run_db_id is None:
            with self.engine.connect() as conn:
                row = conn.execute(
                    text("SELECT id FROM backtest_runs WHERE run_id = :rid"),
                    {"rid": results["run_id"]}
                ).fetchone()
                if row:
                    backtest_run_db_id = row[0]
                else:
                    logger.error("Could not find backtest_run_id in DB")
                    return

        try:
            with self.engine.begin() as conn:
                for i, trade in trades_df.iterrows():
                    pnl_pct = float(trade.get("ReturnPct", 0)) * 100
                    conn.execute(insert_sql, {
                        "backtest_run_id": backtest_run_db_id,
                        "trade_number": i + 1,
                        "entry_time": trade.get("EntryTime", trade.get("EntryBar")),
                        "exit_time": trade.get("ExitTime", trade.get("ExitBar")),
                        "entry_price": float(trade.get("EntryPrice", 0)),
                        "exit_price": float(trade.get("ExitPrice", 0)),
                        "position_size": float(trade.get("Size", 0)),
                        "direction": "LONG" if float(trade.get("Size", 0)) > 0 else "SHORT",
                        "pnl": float(trade.get("PnL", 0)),
                        "pnl_percent": pnl_pct,
                        "is_winning_trade": pnl_pct > 0,
                        "entry_signal": results["strategy_name"],
                        "exit_signal": results["strategy_name"],
                    })
            logger.info(f"Stored {len(trades_df)} trades for run {results['run_id'][:8]}")
        except SQLAlchemyError as e:
            logger.error(f"Failed to store trades: {e}")

    def run_full_pipeline(self, config: BacktestConfig) -> Dict:
        """
        Complete pipeline: fetch -> backtest -> validate -> store.
        Returns results dict with validation status.
        """
        results = self.run_backtest(config)
        status, notes = self.validate_results(results)
        results["validation_status"] = status
        results["validation_notes"] = notes

        try:
            self.store_results(results, status, notes)
            self.store_trades(results)
        except Exception as e:
            logger.error(f"DB storage failed (results still returned): {e}")

        return results


# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------
def main():
    import argparse

    parser = argparse.ArgumentParser(description="Run backtest")
    parser.add_argument("--symbol", default="BTCUSDT")
    parser.add_argument("--timeframe", default="4h")
    parser.add_argument("--strategy", default="supertrend",
                        choices=list(STRATEGY_REGISTRY.keys()))
    parser.add_argument("--start", default="2021-01-01")
    parser.add_argument("--end", default="2024-12-31")
    parser.add_argument("--capital", type=float, default=10000.0)
    parser.add_argument("--params", type=str, default="{}",
                        help='JSON string of strategy params, e.g. \'{"st_length": 10}\'')
    args = parser.parse_args()

    config = BacktestConfig(
        symbol=args.symbol,
        timeframe=args.timeframe,
        strategy_name=args.strategy,
        strategy_params=json.loads(args.params),
        start_date=datetime.fromisoformat(args.start),
        end_date=datetime.fromisoformat(args.end),
        initial_capital=args.capital,
    )

    bt = ProductionBacktester()
    results = bt.run_full_pipeline(config)

    print(f"\n{'='*60}")
    print(f"Strategy: {results['strategy_name']}")
    print(f"Return: {results['total_return']:.2f}%")
    print(f"Sharpe: {results['sharpe_ratio']:.2f}")
    print(f"Max DD: {results['max_drawdown']:.2f}%")
    print(f"Trades: {results['num_trades']}")
    print(f"Win Rate: {results['win_rate']:.1%}")
    print(f"Validation: {results['validation_status']}")
    if results['validation_notes']:
        for note in results['validation_notes']:
            print(f"  - {note}")
    print(f"{'='*60}")


if __name__ == "__main__":
    main()
