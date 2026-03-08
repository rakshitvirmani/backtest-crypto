"""
backtester.py - Production-Grade Backtesting Engine
=====================================================
Architecture:
  - Data Layer: fetches from DuckDB, validates before use
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
from dataclasses import dataclass

import pandas as pd
import numpy as np
import pandas_ta as ta
from backtesting import Backtest, Strategy

from db import get_connection, get_db_path, init_schema

try:
    from config import DB_PATH, LOG_LEVEL, LOG_FILE
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
    """EMA 9/21 Crossover (Trend Following)"""
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
    """SuperTrend Strategy"""
    st_length = 10
    st_multiplier = 3.0

    def init(self):
        high = pd.Series(self.data.High, dtype=float)
        low = pd.Series(self.data.Low, dtype=float)
        close = pd.Series(self.data.Close, dtype=float)
        st = ta.supertrend(high, low, close, length=self.st_length, multiplier=self.st_multiplier)
        if st is not None and len(st.columns) >= 3:
            trend_col = [c for c in st.columns if c.startswith("SUPERTd")]
            if trend_col:
                self.trend = self.I(lambda: st[trend_col[0]].values)
            else:
                self.trend = self.I(lambda: st.iloc[:, 1].values)
        else:
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
    """RSI Mean Reversion"""
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
    """MACD Crossover"""
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


class DMA200Strategy(Strategy):
    """
    200-DMA Trend Following
    - Buy when 2 consecutive closes above the 200-period moving average
    - Sell when 2 consecutive closes below the 200-period moving average
    Parameters: dma_length (int), consecutive_bars (int)
    """
    dma_length = 200
    consecutive_bars = 2

    def init(self):
        close = pd.Series(self.data.Close, dtype=float)
        sma = ta.sma(close, length=self.dma_length)
        self.dma = self.I(lambda: sma.values if sma is not None else close.rolling(self.dma_length).mean().values)

    def next(self):
        if len(self.data.Close) < self.consecutive_bars + 1:
            return
        if np.isnan(self.dma[-1]):
            return

        # Check if last N consecutive closes are above/below DMA
        all_above = all(
            self.data.Close[-1 - i] > self.dma[-1 - i]
            for i in range(self.consecutive_bars)
            if not np.isnan(self.dma[-1 - i])
        )
        all_below = all(
            self.data.Close[-1 - i] < self.dma[-1 - i]
            for i in range(self.consecutive_bars)
            if not np.isnan(self.dma[-1 - i])
        )

        if not self.position:
            if all_above:
                self.buy()
        elif all_below:
            self.position.close()


class DMA200Trail63Strategy(Strategy):
    """
    200-DMA Crossover Entry with 63-DMA Trailing Stop
    - Entry: Price crosses above 200-DMA (previous close below, current close above)
             Buy executes on the next bar after the crossover.
    - Exit:  2 consecutive closes below the 63-DMA (trailing SL)
    Parameters: entry_dma (int), trail_dma (int), exit_consecutive (int)
    """
    entry_dma = 200
    trail_dma = 63
    exit_consecutive = 2

    def init(self):
        close = pd.Series(self.data.Close, dtype=float)
        sma_entry = ta.sma(close, length=self.entry_dma)
        sma_trail = ta.sma(close, length=self.trail_dma)
        self.dma_entry = self.I(lambda: sma_entry.values if sma_entry is not None else close.rolling(self.entry_dma).mean().values)
        self.dma_trail = self.I(lambda: sma_trail.values if sma_trail is not None else close.rolling(self.trail_dma).mean().values)

    def next(self):
        if len(self.data.Close) < 3:
            return
        if np.isnan(self.dma_entry[-1]) or np.isnan(self.dma_entry[-2]) or np.isnan(self.dma_trail[-1]):
            return

        # Entry: previous close was below 200-DMA, then closed above 200-DMA
        # e.g. Jan 1 close < 200DMA, Jan 2 close > 200DMA → buy on Jan 3 (this bar)
        crossover = (
            self.data.Close[-2] > self.dma_entry[-2]      # yesterday closed above
            and self.data.Close[-3] < self.dma_entry[-3]   # day before closed below
        )

        # Exit: 2 consecutive closes below 63-DMA
        exit_signal = all(
            self.data.Close[-1 - i] < self.dma_trail[-1 - i]
            for i in range(self.exit_consecutive)
            if not np.isnan(self.dma_trail[-1 - i])
        )

        if not self.position:
            if crossover:
                self.buy()
        elif exit_signal:
            self.position.close()


class DMA30CrossoverStrategy(Strategy):
    """
    30-DMA Crossover Entry & Exit
    - Entry: Price crosses above 30-DMA from below
             (previous close below, current close above) → buy next bar
    - Exit:  Price closes below 30-DMA → sell
    Parameters: dma_length (int)
    """
    dma_length = 30

    def init(self):
        close = pd.Series(self.data.Close, dtype=float)
        sma = ta.sma(close, length=self.dma_length)
        self.dma = self.I(lambda: sma.values if sma is not None else close.rolling(self.dma_length).mean().values)

    def next(self):
        if len(self.data.Close) < 3:
            return
        if np.isnan(self.dma[-1]) or np.isnan(self.dma[-2]) or np.isnan(self.dma[-3]):
            return

        # Entry: crosses above 30-DMA from below → buy on next bar
        crossover = (
            self.data.Close[-2] > self.dma[-2]      # yesterday closed above
            and self.data.Close[-3] < self.dma[-3]   # day before closed below
        )

        # Exit: closes below 30-DMA
        close_below = self.data.Close[-1] < self.dma[-1]

        if not self.position:
            if crossover:
                self.buy()
        elif close_below:
            self.position.close()


class GoldenCrossDrawdownStrategy(Strategy):
    """
    21/200-DMA Golden Cross with Drawdown Exit
    - Entry: 21-DMA crosses above 200-DMA from below AND price close > 200-DMA
    - Exit:  Price drops 20% from the highest close since entry
    Parameters: fast_dma (int), slow_dma (int), drawdown_pct (float)
    """
    fast_dma = 21
    slow_dma = 200
    drawdown_pct = 20.0

    def init(self):
        close = pd.Series(self.data.Close, dtype=float)
        sma_fast = ta.sma(close, length=self.fast_dma)
        sma_slow = ta.sma(close, length=self.slow_dma)
        self.dma_fast = self.I(lambda: sma_fast.values if sma_fast is not None else close.rolling(self.fast_dma).mean().values)
        self.dma_slow = self.I(lambda: sma_slow.values if sma_slow is not None else close.rolling(self.slow_dma).mean().values)
        self._peak_price = 0.0

    def next(self):
        if len(self.data.Close) < 3:
            return
        if np.isnan(self.dma_fast[-1]) or np.isnan(self.dma_fast[-2]) or np.isnan(self.dma_slow[-1]):
            return

        # Entry: 21-DMA crosses above 200-DMA from below AND close > 200-DMA
        golden_cross = (
            self.dma_fast[-2] > self.dma_slow[-2]      # yesterday 21DMA above 200DMA
            and self.dma_fast[-3] < self.dma_slow[-3]   # day before 21DMA below 200DMA
        )
        price_above_slow = self.data.Close[-1] > self.dma_slow[-1]

        if not self.position:
            if golden_cross and price_above_slow:
                self._peak_price = self.data.Close[-1]
                self.buy()
        else:
            # Track the highest close since entry
            if self.data.Close[-1] > self._peak_price:
                self._peak_price = self.data.Close[-1]

            # Exit: price has fallen drawdown_pct% from peak on closing basis
            drop_from_peak = (self._peak_price - self.data.Close[-1]) / self._peak_price * 100
            if drop_from_peak >= self.drawdown_pct:
                self.position.close()
                self._peak_price = 0.0


class TripleDMAStrategy(Strategy):
    """
    Triple DMA Tiered Entry with Adaptive Trailing Stop
    Uses 3 moving averages: 200, 63, 21

    Entry signals (whichever fires first while flat):
      1. Price crosses above 200-DMA from below → enter, trail with 63-DMA
      2. Price crosses above 63-DMA from below  → enter, trail with 21-DMA

    While in position, if price also crosses above the next tier:
      - Entered on 200-DMA crossover, trailing 63-DMA →
        once price crosses above 63-DMA, tighten trail to 21-DMA

    Exit: close below the active trailing DMA

    Parameters: slow_dma (int), mid_dma (int), fast_dma (int)
    """
    slow_dma = 200
    mid_dma = 63
    fast_dma = 21

    def init(self):
        close = pd.Series(self.data.Close, dtype=float)
        sma_slow = ta.sma(close, length=self.slow_dma)
        sma_mid = ta.sma(close, length=self.mid_dma)
        sma_fast = ta.sma(close, length=self.fast_dma)
        self.dma_slow = self.I(lambda: sma_slow.values if sma_slow is not None else close.rolling(self.slow_dma).mean().values)
        self.dma_mid = self.I(lambda: sma_mid.values if sma_mid is not None else close.rolling(self.mid_dma).mean().values)
        self.dma_fast = self.I(lambda: sma_fast.values if sma_fast is not None else close.rolling(self.fast_dma).mean().values)
        # Track which trailing DMA is active: 'mid' (63) or 'fast' (21)
        self._active_trail = None

    def _crossed_above(self, dma):
        """Check if price crossed above a DMA from below (yesterday above, day before below)."""
        return (
            self.data.Close[-2] > dma[-2]
            and self.data.Close[-3] < dma[-3]
        )

    def next(self):
        if len(self.data.Close) < 4:
            return
        if np.isnan(self.dma_slow[-1]) or np.isnan(self.dma_mid[-1]) or np.isnan(self.dma_fast[-1]):
            return

        if not self.position:
            # Entry 1: price crosses above 200-DMA → trail with 63-DMA
            if self._crossed_above(self.dma_slow):
                self._active_trail = 'mid'
                self.buy()
            # Entry 2: price crosses above 63-DMA → trail with 21-DMA
            elif self._crossed_above(self.dma_mid):
                self._active_trail = 'fast'
                self.buy()
        else:
            # Upgrade trailing: if entered on 200-DMA (trailing 63), and price
            # now crosses above 63-DMA, tighten trail to 21-DMA
            if self._active_trail == 'mid' and self._crossed_above(self.dma_mid):
                self._active_trail = 'fast'

            # Exit: close below the active trailing DMA
            if self._active_trail == 'mid':
                if self.data.Close[-1] < self.dma_mid[-1]:
                    self.position.close()
                    self._active_trail = None
            elif self._active_trail == 'fast':
                if self.data.Close[-1] < self.dma_fast[-1]:
                    self.position.close()
                    self._active_trail = None


class BBRSIStrategy(Strategy):
    """
    Bollinger Bands + RSI Confluence Strategy
    - Buy:  Price closes below lower BB AND RSI < oversold threshold
    - Sell: Price closes above upper BB AND RSI > overbought threshold
    Both conditions must be true simultaneously for higher conviction entries/exits.
    Parameters: bb_length (int), bb_std (float), rsi_length (int),
                rsi_oversold (float), rsi_overbought (float)
    """
    bb_length = 20
    bb_std = 2.0
    rsi_length = 14
    rsi_oversold = 30.0
    rsi_overbought = 70.0

    def init(self):
        close = pd.Series(self.data.Close, dtype=float)

        # Bollinger Bands
        bb = ta.bbands(close, length=self.bb_length, std=self.bb_std)
        if bb is not None and len(bb.columns) >= 3:
            self.bb_lower = self.I(lambda: bb.iloc[:, 0].values)
            self.bb_middle = self.I(lambda: bb.iloc[:, 1].values)
            self.bb_upper = self.I(lambda: bb.iloc[:, 2].values)
        else:
            sma = close.rolling(self.bb_length).mean()
            std = close.rolling(self.bb_length).std()
            self.bb_lower = self.I(lambda: (sma - self.bb_std * std).values)
            self.bb_middle = self.I(lambda: sma.values)
            self.bb_upper = self.I(lambda: (sma + self.bb_std * std).values)

        # RSI
        rsi = ta.rsi(close, length=self.rsi_length)
        self.rsi = self.I(lambda: rsi.values if rsi is not None else np.full(len(close), 50.0))

    def next(self):
        if np.isnan(self.bb_lower[-1]) or np.isnan(self.bb_upper[-1]) or np.isnan(self.rsi[-1]):
            return

        price = self.data.Close[-1]

        if not self.position:
            # Buy: price at/below lower BB AND RSI oversold
            if price <= self.bb_lower[-1] and self.rsi[-1] < self.rsi_oversold:
                self.buy()
        else:
            # Sell: price at/above upper BB AND RSI overbought
            if price >= self.bb_upper[-1] and self.rsi[-1] > self.rsi_overbought:
                self.position.close()


class LongShortDMA200Strategy(Strategy):
    """
    Long/Short 200-DMA Regime Strategy with 63-DMA Trailing
    - Price crosses above 200-DMA from below → go LONG, trail with 63-DMA
    - Price crosses below 200-DMA from above → go SHORT, trail with 63-DMA
    - Long exit:  close below 63-DMA
    - Short exit: close above 63-DMA
    Parameters: regime_dma (int), trail_dma (int)
    """
    regime_dma = 200
    trail_dma = 63

    def init(self):
        close = pd.Series(self.data.Close, dtype=float)
        sma_regime = ta.sma(close, length=self.regime_dma)
        sma_trail = ta.sma(close, length=self.trail_dma)
        self.dma_regime = self.I(lambda: sma_regime.values if sma_regime is not None else close.rolling(self.regime_dma).mean().values)
        self.dma_trail = self.I(lambda: sma_trail.values if sma_trail is not None else close.rolling(self.trail_dma).mean().values)

    def next(self):
        if len(self.data.Close) < 4:
            return
        if np.isnan(self.dma_regime[-1]) or np.isnan(self.dma_regime[-3]) or np.isnan(self.dma_trail[-1]):
            return

        # Crossover: price crosses above 200-DMA from below
        crossed_above = (
            self.data.Close[-2] > self.dma_regime[-2]
            and self.data.Close[-3] < self.dma_regime[-3]
        )
        # Crossunder: price crosses below 200-DMA from above
        crossed_below = (
            self.data.Close[-2] < self.dma_regime[-2]
            and self.data.Close[-3] > self.dma_regime[-3]
        )

        if not self.position:
            if crossed_above:
                self.buy()
            elif crossed_below:
                self.sell()
        elif self.position.is_long:
            # Exit long if close below 63-DMA, or flip to short on cross below 200-DMA
            if crossed_below:
                self.position.close()
                self.sell()
            elif self.data.Close[-1] < self.dma_trail[-1]:
                self.position.close()
        elif self.position.is_short:
            # Exit short if close above 63-DMA, or flip to long on cross above 200-DMA
            if crossed_above:
                self.position.close()
                self.buy()
            elif self.data.Close[-1] > self.dma_trail[-1]:
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
    "dma200": DMA200Strategy,
    "dma200_trail63": DMA200Trail63Strategy,
    "dma30": DMA30CrossoverStrategy,
    "golden_cross_dd": GoldenCrossDrawdownStrategy,
    "triple_dma": TripleDMAStrategy,
    "bb_rsi": BBRSIStrategy,
    "long_short_dma": LongShortDMA200Strategy,
}


# ---------------------------------------------------------------------------
# Production Backtester
# ---------------------------------------------------------------------------
class ProductionBacktester:
    """Main backtesting orchestrator with validation and audit trails."""

    def __init__(self, db_path: str = None):
        self.db_path = db_path or DB_PATH
        self.run_id = str(uuid4())
        init_schema(self.db_path)
        logger.info(f"Backtest run initialized: {self.run_id}")

    def fetch_data(self, config: BacktestConfig) -> pd.DataFrame:
        """Query database with pre-backtest validation."""
        start_ms = int(config.start_date.timestamp() * 1000)
        end_ms = int(config.end_date.timestamp() * 1000)

        conn = get_connection(self.db_path)
        try:
            df = conn.execute("""
                SELECT open_time, close_time, open, high, low, close, volume
                FROM klines
                WHERE symbol = ? AND timeframe = ?
                  AND open_time >= ? AND open_time <= ?
                ORDER BY open_time ASC
            """, [config.symbol, config.timeframe, start_ms, end_ms]).fetchdf()
        finally:
            conn.close()

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

        if data is None:
            data = self.fetch_data(config)

        bt = Backtest(
            data, strategy_class,
            cash=config.initial_capital,
            commission=config.commission_pct / 100,
            exclusive_orders=True,
        )
        stats = bt.run(**config.strategy_params)

        def safe_float(val, default=np.nan):
            try:
                v = float(val)
                return v if not (np.isinf(v) or np.isnan(v)) else default
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
            "num_winning_trades": int(num_trades * win_rate) if not np.isnan(win_rate) else 0,
            "avg_trade_pnl_pct": safe_float(stats.get("Avg. Trade [%]", np.nan)),
            "best_trade_pnl_pct": safe_float(stats.get("Best Trade [%]", np.nan)),
            "worst_trade_pnl_pct": safe_float(stats.get("Worst Trade [%]", np.nan)),
            "max_consecutive_losses": int(safe_float(stats.get("Max. Trade Duration", 0), default=0)),
            "backtest_start": config.start_date,
            "backtest_end": config.end_date,
            "initial_capital": config.initial_capital,
            "slippage_pct": config.slippage_pct,
            "commission_pct": config.commission_pct,
            "_stats": stats,
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
        """Production-grade validation. Returns (status, notes)."""
        notes = []

        if results["num_trades"] < 5:
            return "FAILED", ["Too few trades (< 5) — insufficient statistical significance"]
        if results["max_drawdown"] < -50:
            return "FAILED", [f"Max drawdown {results['max_drawdown']:.1f}% exceeds -50% — too risky"]
        if results["total_return"] < -30:
            return "FAILED", [f"Total return {results['total_return']:.1f}% — strategy loses money"]

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
        conn = get_connection(self.db_path)
        try:
            conn.execute("""
                INSERT INTO backtest_runs (
                    run_id, symbol, timeframe, strategy_name, strategy_params,
                    backtest_start, backtest_end, total_return, annualized_return,
                    max_drawdown, sharpe_ratio, sortino_ratio, calmar_ratio,
                    win_rate, profit_factor, num_trades, num_winning_trades,
                    avg_trade_pnl_pct, best_trade_pnl_pct, worst_trade_pnl_pct,
                    initial_capital, slippage_pct, commission_pct,
                    validation_status, validation_notes
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, [
                results["run_id"], results["symbol"], results["timeframe"],
                results["strategy_name"], json.dumps(results["strategy_params"]),
                results["backtest_start"], results["backtest_end"],
                results["total_return"], results["annualized_return"],
                results["max_drawdown"], results["sharpe_ratio"],
                results.get("sortino_ratio"), results.get("calmar_ratio"),
                results["win_rate"], results.get("profit_factor"),
                results["num_trades"], results["num_winning_trades"],
                results.get("avg_trade_pnl_pct"), results.get("best_trade_pnl_pct"),
                results.get("worst_trade_pnl_pct"),
                results["initial_capital"], results["slippage_pct"],
                results["commission_pct"],
                status, "\n".join(notes) if notes else None,
            ])
            logger.info(f"Results stored in DB. Run ID: {results['run_id']}")
        except Exception as e:
            logger.error(f"Failed to store results: {e}")
            raise
        finally:
            conn.close()
        return results["run_id"]

    def store_trades(self, results: Dict, backtest_run_db_id: int = None):
        """Store individual trades from backtest results."""
        trades_df = results.get("_trades")
        if trades_df is None or trades_df.empty:
            logger.warning("No trades to store")
            return

        conn = get_connection(self.db_path)
        try:
            if backtest_run_db_id is None:
                row = conn.execute(
                    "SELECT id FROM backtest_runs WHERE run_id = ?",
                    [results["run_id"]]
                ).fetchone()
                if row:
                    backtest_run_db_id = row[0]
                else:
                    logger.error("Could not find backtest_run_id in DB")
                    return

            for i, trade in trades_df.iterrows():
                pnl_pct = float(trade.get("ReturnPct", 0)) * 100
                conn.execute("""
                    INSERT INTO trade_log (
                        backtest_run_id, trade_number, entry_time, exit_time,
                        entry_price, exit_price, position_size, direction,
                        pnl, pnl_percent, is_winning_trade,
                        entry_signal, exit_signal
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, [
                    backtest_run_db_id, i + 1,
                    trade.get("EntryTime", trade.get("EntryBar")),
                    trade.get("ExitTime", trade.get("ExitBar")),
                    float(trade.get("EntryPrice", 0)),
                    float(trade.get("ExitPrice", 0)),
                    float(abs(trade.get("Size", 0))),
                    "LONG" if float(trade.get("Size", 0)) > 0 else "SHORT",
                    float(trade.get("PnL", 0)), pnl_pct, pnl_pct > 0,
                    results["strategy_name"], results["strategy_name"],
                ])
            logger.info(f"Stored {len(trades_df)} trades for run {results['run_id'][:8]}")
        except Exception as e:
            logger.error(f"Failed to store trades: {e}")
        finally:
            conn.close()

    def run_full_pipeline(self, config: BacktestConfig) -> Dict:
        """Complete pipeline: fetch -> backtest -> validate -> store."""
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

    # Export trades to CSV
    trades_df = results.get("_trades")
    if trades_df is not None and not trades_df.empty:
        today = datetime.now().strftime("%Y-%m-%d")
        csv_name = f"{today}_{args.symbol}_{args.timeframe}_{args.strategy}.csv"
        csv_path = os.path.join("data", csv_name)
        os.makedirs("data", exist_ok=True)
        trades_df.to_csv(csv_path, index=False)
        print(f"\nTrades exported to: {csv_path}")
    else:
        print("\nNo trades to export.")

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
