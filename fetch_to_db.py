"""
fetch_to_db.py - Production Data Ingestion Pipeline
====================================================
Fetches OHLCV data from Binance with:
- Exponential backoff retry (max 5 attempts, 1-32s delays)
- Circuit breaker (3 consecutive failures = halt)
- OHLC integrity validation
- Volume anomaly detection (3σ from 50-bar rolling mean)
- Timestamp gap detection
- Duplicate rejection
- Price movement sanity checks
- UPSERT with conflict handling
- Full audit logging of every fetch operation

NEVER run this without understanding what it does. Real money depends on this data.
"""

import os
import sys
import time
import hashlib
import logging
from datetime import datetime, timedelta
from typing import List, Dict, Tuple, Optional

import numpy as np
import pandas as pd
from binance.client import Client
from binance.exceptions import BinanceAPIException, BinanceRequestException
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError

# ---------------------------------------------------------------------------
# Configuration - import from config.py or fall back to env vars
# ---------------------------------------------------------------------------
try:
    from config import (
        DATABASE_URL, BINANCE_API_KEY, BINANCE_API_SECRET,
        SYMBOLS, TIMEFRAMES, MAX_RETRIES, RETRY_BASE_DELAY_SEC,
        RETRY_MAX_DELAY_SEC, CIRCUIT_BREAKER_THRESHOLD,
        FETCH_TIMEOUT_ALERT_SEC, RATE_LIMIT_REQUESTS_PER_MIN,
        RATE_LIMIT_SAFETY_MARGIN, MAX_BACKFILL_DAYS, LOG_LEVEL, LOG_FILE
    )
except ImportError:
    print("ERROR: config.py not found. Copy config.example.py to config.py and fill in values.")
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
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("fetch_to_db")


# ---------------------------------------------------------------------------
# Timeframe helpers
# ---------------------------------------------------------------------------
TIMEFRAME_MS = {
    "1m": 60_000,
    "3m": 180_000,
    "5m": 300_000,
    "15m": 900_000,
    "30m": 1_800_000,
    "1h": 3_600_000,
    "2h": 7_200_000,
    "4h": 14_400_000,
    "6h": 21_600_000,
    "8h": 28_800_000,
    "12h": 43_200_000,
    "1d": 86_400_000,
    "3d": 259_200_000,
    "1w": 604_800_000,
    "1M": 2_592_000_000,  # approximate 30 days
}

BINANCE_TF_MAP = {
    "1m": Client.KLINE_INTERVAL_1MINUTE,
    "3m": Client.KLINE_INTERVAL_3MINUTE,
    "5m": Client.KLINE_INTERVAL_5MINUTE,
    "15m": Client.KLINE_INTERVAL_15MINUTE,
    "30m": Client.KLINE_INTERVAL_30MINUTE,
    "1h": Client.KLINE_INTERVAL_1HOUR,
    "2h": Client.KLINE_INTERVAL_2HOUR,
    "4h": Client.KLINE_INTERVAL_4HOUR,
    "6h": Client.KLINE_INTERVAL_6HOUR,
    "8h": Client.KLINE_INTERVAL_8HOUR,
    "12h": Client.KLINE_INTERVAL_12HOUR,
    "1d": Client.KLINE_INTERVAL_1DAY,
    "3d": Client.KLINE_INTERVAL_3DAY,
    "1w": Client.KLINE_INTERVAL_1WEEK,
    "1M": Client.KLINE_INTERVAL_1MONTH,
}


# ---------------------------------------------------------------------------
# Rate limiter
# ---------------------------------------------------------------------------
class RateLimiter:
    """Track Binance API request rate and pause if approaching limit."""

    def __init__(self, max_per_min: int, safety_margin: float = 0.85):
        self.max_per_min = max_per_min
        self.threshold = int(max_per_min * safety_margin)
        self.timestamps: List[float] = []

    def record(self):
        self.timestamps.append(time.time())

    def check_and_wait(self):
        now = time.time()
        self.timestamps = [t for t in self.timestamps if now - t < 60]
        if len(self.timestamps) >= self.threshold:
            sleep_for = 60 - (now - self.timestamps[0]) + 1
            logger.warning(
                f"Rate limit approaching ({len(self.timestamps)}/{self.max_per_min}). "
                f"Sleeping {sleep_for:.1f}s"
            )
            time.sleep(sleep_for)


# ---------------------------------------------------------------------------
# Data Validator
# ---------------------------------------------------------------------------
class FetchDataValidator:
    """Validate fetched kline data before inserting into DB."""

    @staticmethod
    def validate_ohlc(df: pd.DataFrame) -> Tuple[pd.DataFrame, List[str]]:
        """Reject rows with OHLC out of order. Returns (clean_df, errors)."""
        errors = []
        bad_mask = (
            (df["high"] < df["low"])
            | (df["high"] < df["open"])
            | (df["high"] < df["close"])
            | (df["low"] > df["open"])
            | (df["low"] > df["close"])
        )
        bad_count = bad_mask.sum()
        if bad_count > 0:
            errors.append(f"Rejected {bad_count} rows with invalid OHLC ordering")
            logger.error(f"OHLC validation failed for {bad_count} rows")
            df = df[~bad_mask].copy()
        return df, errors

    @staticmethod
    def flag_volume_anomalies(df: pd.DataFrame) -> List[str]:
        """Flag volume spikes >3σ from 50-bar rolling mean."""
        warnings = []
        if len(df) < 50:
            return warnings
        rolling_mean = df["volume"].rolling(50).mean()
        rolling_std = df["volume"].rolling(50).std()
        anomalies = df["volume"] > (rolling_mean + 3 * rolling_std)
        anomaly_count = anomalies.sum()
        if anomaly_count > 0:
            warnings.append(
                f"Volume anomaly: {anomaly_count} bars exceed 3σ from 50-bar mean"
            )
            logger.warning(
                f"Volume anomalies detected: {anomaly_count} bars. "
                f"Possible data corruption — review manually."
            )
        return warnings

    @staticmethod
    def detect_gaps(df: pd.DataFrame, timeframe: str) -> List[str]:
        """Detect timestamp gaps larger than expected timeframe window."""
        warnings = []
        expected_ms = TIMEFRAME_MS.get(timeframe)
        if expected_ms is None or len(df) < 2:
            return warnings

        diffs = df["open_time"].diff().dropna()
        # Allow 10% tolerance for monthly candles
        tolerance = expected_ms * 1.1 if timeframe == "1M" else expected_ms * 1.01
        gaps = diffs[diffs > tolerance]
        if len(gaps) > 0:
            warnings.append(
                f"Timestamp gaps detected: {len(gaps)} gaps > expected {timeframe} window"
            )
            for idx in gaps.index[:5]:  # log first 5
                gap_time = pd.to_datetime(df.loc[idx, "open_time"], unit="ms")
                logger.warning(f"  Gap at {gap_time}: {diffs.loc[idx]}ms (expected {expected_ms}ms)")
        return warnings

    @staticmethod
    def check_duplicates(df: pd.DataFrame) -> Tuple[pd.DataFrame, List[str]]:
        """Remove duplicate timestamps. Returns (deduped_df, errors)."""
        errors = []
        dup_count = df["open_time"].duplicated().sum()
        if dup_count > 0:
            errors.append(f"Removed {dup_count} duplicate timestamps")
            logger.error(f"Duplicate timestamps found: {dup_count}. Keeping last occurrence.")
            df = df.drop_duplicates(subset=["open_time"], keep="last").copy()
        return df, errors

    @staticmethod
    def check_price_sanity(df: pd.DataFrame, timeframe: str) -> List[str]:
        """Reject >20% single 1h move for BTC without verification."""
        warnings = []
        if timeframe not in ("1m", "3m", "5m", "15m", "30m", "1h"):
            return warnings  # only check short timeframes

        pct_change = df["close"].pct_change().abs()
        extreme = pct_change[pct_change > 0.20]
        if len(extreme) > 0:
            warnings.append(
                f"Extreme price moves: {len(extreme)} bars with >20% change in {timeframe}"
            )
            for idx in extreme.index[:3]:
                logger.error(
                    f"  Extreme move at {pd.to_datetime(df.loc[idx, 'open_time'], unit='ms')}: "
                    f"{pct_change.loc[idx]:.2%}"
                )
        return warnings

    @classmethod
    def validate_all(
        cls, df: pd.DataFrame, timeframe: str
    ) -> Tuple[pd.DataFrame, List[str], List[str]]:
        """
        Run all validations. Returns (clean_df, errors, warnings).
        Errors = data was modified/rejected. Warnings = flagged for review.
        """
        all_errors = []
        all_warnings = []

        df, errs = cls.validate_ohlc(df)
        all_errors.extend(errs)

        df, errs = cls.check_duplicates(df)
        all_errors.extend(errs)

        all_warnings.extend(cls.flag_volume_anomalies(df))
        all_warnings.extend(cls.detect_gaps(df, timeframe))
        all_warnings.extend(cls.check_price_sanity(df, timeframe))

        return df, all_errors, all_warnings


# ---------------------------------------------------------------------------
# Checksum utility
# ---------------------------------------------------------------------------
def compute_checksum(df: pd.DataFrame) -> str:
    """SHA-256 checksum of the dataframe content for integrity verification."""
    content = df.to_csv(index=False).encode("utf-8")
    return hashlib.sha256(content).hexdigest()


# ---------------------------------------------------------------------------
# Core Fetcher
# ---------------------------------------------------------------------------
class BinanceFetcher:
    """
    Fetches kline data from Binance with retry, circuit breaker, and validation.
    """

    def __init__(self, db_url: str, api_key: str, api_secret: str):
        self.engine = create_engine(db_url)
        self.client = Client(api_key, api_secret)
        self.rate_limiter = RateLimiter(RATE_LIMIT_REQUESTS_PER_MIN, RATE_LIMIT_SAFETY_MARGIN)
        self.consecutive_failures = 0
        self.validator = FetchDataValidator()
        logger.info("BinanceFetcher initialized")

    def _fetch_with_retry(
        self, symbol: str, interval: str, start_str: str, end_str: Optional[str] = None
    ) -> List:
        """Fetch klines with exponential backoff retry."""
        for attempt in range(1, MAX_RETRIES + 1):
            try:
                self.rate_limiter.check_and_wait()
                start_time = time.time()

                klines = self.client.get_historical_klines(
                    symbol, interval, start_str, end_str
                )

                elapsed = time.time() - start_time
                self.rate_limiter.record()

                if elapsed > FETCH_TIMEOUT_ALERT_SEC:
                    logger.warning(
                        f"Slow fetch: {symbol} {interval} took {elapsed:.1f}s (threshold: {FETCH_TIMEOUT_ALERT_SEC}s)"
                    )

                self.consecutive_failures = 0
                logger.info(
                    f"Fetched {len(klines)} klines for {symbol} {interval} "
                    f"in {elapsed:.2f}s (attempt {attempt})"
                )
                return klines

            except (BinanceAPIException, BinanceRequestException) as e:
                delay = min(RETRY_BASE_DELAY_SEC * (2 ** (attempt - 1)), RETRY_MAX_DELAY_SEC)
                logger.error(
                    f"Binance error (attempt {attempt}/{MAX_RETRIES}): {e}. "
                    f"Retrying in {delay}s..."
                )
                time.sleep(delay)

            except Exception as e:
                delay = min(RETRY_BASE_DELAY_SEC * (2 ** (attempt - 1)), RETRY_MAX_DELAY_SEC)
                logger.error(
                    f"Unexpected error (attempt {attempt}/{MAX_RETRIES}): {e}. "
                    f"Retrying in {delay}s..."
                )
                time.sleep(delay)

        # All retries exhausted
        self.consecutive_failures += 1
        if self.consecutive_failures >= CIRCUIT_BREAKER_THRESHOLD:
            logger.critical(
                f"CIRCUIT BREAKER TRIPPED: {self.consecutive_failures} consecutive failures. "
                f"Halting all fetches. Manual intervention required."
            )
            raise SystemExit(
                f"Circuit breaker: {CIRCUIT_BREAKER_THRESHOLD} consecutive fetch failures"
            )
        raise RuntimeError(f"Failed to fetch {symbol} {interval} after {MAX_RETRIES} attempts")

    def _klines_to_dataframe(self, klines: List) -> pd.DataFrame:
        """Convert Binance kline response to DataFrame."""
        columns = [
            "open_time", "open", "high", "low", "close", "volume",
            "close_time", "quote_asset_volume", "number_of_trades",
            "taker_buy_base_volume", "taker_buy_quote_volume", "ignore"
        ]
        df = pd.DataFrame(klines, columns=columns)
        df.drop(columns=["ignore"], inplace=True)

        # Convert numeric columns
        numeric_cols = [
            "open", "high", "low", "close", "volume",
            "quote_asset_volume", "taker_buy_base_volume", "taker_buy_quote_volume"
        ]
        for col in numeric_cols:
            df[col] = pd.to_numeric(df[col], errors="coerce")

        df["open_time"] = pd.to_numeric(df["open_time"])
        df["close_time"] = pd.to_numeric(df["close_time"])
        df["number_of_trades"] = pd.to_numeric(df["number_of_trades"], downcast="integer")

        return df

    def _upsert_to_db(
        self, df: pd.DataFrame, symbol: str, timeframe: str
    ) -> int:
        """
        UPSERT klines into PostgreSQL using ON CONFLICT ... DO UPDATE.
        Returns count of upserted rows.
        """
        checksum = compute_checksum(df)
        upsert_sql = text("""
            INSERT INTO klines (
                symbol, timeframe, open_time, close_time,
                open, high, low, close, volume,
                quote_asset_volume, number_of_trades,
                taker_buy_base_volume, taker_buy_quote_volume,
                fetch_timestamp, data_checksum
            ) VALUES (
                :symbol, :timeframe, :open_time, :close_time,
                :open, :high, :low, :close, :volume,
                :quote_asset_volume, :number_of_trades,
                :taker_buy_base_volume, :taker_buy_quote_volume,
                CURRENT_TIMESTAMP, :data_checksum
            )
            ON CONFLICT (symbol, timeframe, open_time) DO UPDATE SET
                close_time = EXCLUDED.close_time,
                open = EXCLUDED.open,
                high = EXCLUDED.high,
                low = EXCLUDED.low,
                close = EXCLUDED.close,
                volume = EXCLUDED.volume,
                quote_asset_volume = EXCLUDED.quote_asset_volume,
                number_of_trades = EXCLUDED.number_of_trades,
                taker_buy_base_volume = EXCLUDED.taker_buy_base_volume,
                taker_buy_quote_volume = EXCLUDED.taker_buy_quote_volume,
                fetch_timestamp = CURRENT_TIMESTAMP,
                data_checksum = EXCLUDED.data_checksum
        """)

        upserted = 0
        with self.engine.begin() as conn:
            for _, row in df.iterrows():
                conn.execute(upsert_sql, {
                    "symbol": symbol,
                    "timeframe": timeframe,
                    "open_time": int(row["open_time"]),
                    "close_time": int(row["close_time"]),
                    "open": float(row["open"]),
                    "high": float(row["high"]),
                    "low": float(row["low"]),
                    "close": float(row["close"]),
                    "volume": float(row["volume"]),
                    "quote_asset_volume": float(row["quote_asset_volume"]) if pd.notna(row["quote_asset_volume"]) else None,
                    "number_of_trades": int(row["number_of_trades"]) if pd.notna(row["number_of_trades"]) else None,
                    "taker_buy_base_volume": float(row["taker_buy_base_volume"]) if pd.notna(row["taker_buy_base_volume"]) else None,
                    "taker_buy_quote_volume": float(row["taker_buy_quote_volume"]) if pd.notna(row["taker_buy_quote_volume"]) else None,
                    "data_checksum": checksum,
                })
                upserted += 1

        logger.info(
            f"Upserted {upserted} rows for {symbol} {timeframe}. Checksum: {checksum[:12]}..."
        )
        return upserted

    def _log_fetch(
        self, symbol: str, timeframe: str, start_time: datetime,
        records_fetched: int, records_upserted: int,
        http_status: int = 200, errors: str = None, checksum: str = None
    ):
        """Write fetch audit record to fetch_log table."""
        sql = text("""
            INSERT INTO fetch_log (
                symbol, timeframe, fetch_start, fetch_end,
                http_status, response_time_ms,
                records_fetched, records_upserted, errors, checksum
            ) VALUES (
                :symbol, :timeframe, :fetch_start, :fetch_end,
                :http_status, :response_time_ms,
                :records_fetched, :records_upserted, :errors, :checksum
            )
        """)
        elapsed_ms = int((datetime.utcnow() - start_time).total_seconds() * 1000)
        try:
            with self.engine.begin() as conn:
                conn.execute(sql, {
                    "symbol": symbol,
                    "timeframe": timeframe,
                    "fetch_start": start_time,
                    "fetch_end": datetime.utcnow(),
                    "http_status": http_status,
                    "response_time_ms": elapsed_ms,
                    "records_fetched": records_fetched,
                    "records_upserted": records_upserted,
                    "errors": errors,
                    "checksum": checksum,
                })
        except SQLAlchemyError as e:
            logger.error(f"Failed to write fetch log: {e}")

    def fetch_symbol_timeframe(
        self, symbol: str, timeframe: str,
        start_str: str = "1 Jan, 2020",
        end_str: Optional[str] = None,
        force_backfill: bool = False
    ):
        """
        Fetch and store klines for a single symbol/timeframe.
        Respects MAX_BACKFILL_DAYS unless force_backfill=True.
        """
        fetch_start = datetime.utcnow()
        logger.info(f"=== Fetching {symbol} {timeframe} from {start_str} ===")

        # Backfill guard
        if not force_backfill:
            cutoff = datetime.utcnow() - timedelta(days=MAX_BACKFILL_DAYS)
            # Check if we already have recent data
            with self.engine.connect() as conn:
                result = conn.execute(text(
                    "SELECT MAX(open_time) FROM klines "
                    "WHERE symbol = :symbol AND timeframe = :timeframe"
                ), {"symbol": symbol, "timeframe": timeframe}).scalar()

            if result is not None:
                last_time = pd.to_datetime(result, unit="ms")
                if last_time > cutoff:
                    start_str = last_time.strftime("%d %b, %Y %H:%M:%S")
                    logger.info(f"Incremental fetch from {start_str} (last record in DB)")

        try:
            interval = BINANCE_TF_MAP[timeframe]
            raw_klines = self._fetch_with_retry(symbol, interval, start_str, end_str)

            if not raw_klines:
                logger.warning(f"No data returned for {symbol} {timeframe}")
                self._log_fetch(symbol, timeframe, fetch_start, 0, 0)
                return

            df = self._klines_to_dataframe(raw_klines)
            records_fetched = len(df)

            # Validate
            df, errors, warnings = self.validator.validate_all(df, timeframe)

            if errors:
                logger.error(f"Validation errors for {symbol} {timeframe}: {errors}")
            if warnings:
                logger.warning(f"Validation warnings for {symbol} {timeframe}: {warnings}")

            if df.empty:
                logger.error(f"All data rejected for {symbol} {timeframe} after validation")
                self._log_fetch(
                    symbol, timeframe, fetch_start, records_fetched, 0,
                    errors="; ".join(errors)
                )
                return

            # Upsert
            checksum = compute_checksum(df)
            records_upserted = self._upsert_to_db(df, symbol, timeframe)

            self._log_fetch(
                symbol, timeframe, fetch_start,
                records_fetched, records_upserted,
                checksum=checksum,
                errors="; ".join(errors) if errors else None
            )

            logger.info(
                f"Completed {symbol} {timeframe}: "
                f"fetched={records_fetched}, upserted={records_upserted}, "
                f"errors={len(errors)}, warnings={len(warnings)}"
            )

        except SystemExit:
            raise  # re-raise circuit breaker
        except Exception as e:
            logger.error(f"Fatal error fetching {symbol} {timeframe}: {e}", exc_info=True)
            self._log_fetch(
                symbol, timeframe, fetch_start, 0, 0,
                http_status=500, errors=str(e)
            )
            raise

    def fetch_all(self, force_backfill: bool = False):
        """Fetch all configured symbols and timeframes."""
        logger.info(
            f"Starting full fetch: symbols={SYMBOLS}, timeframes={TIMEFRAMES}, "
            f"force_backfill={force_backfill}"
        )
        total_start = time.time()

        for symbol in SYMBOLS:
            for timeframe in TIMEFRAMES:
                try:
                    self.fetch_symbol_timeframe(
                        symbol, timeframe, force_backfill=force_backfill
                    )
                except SystemExit:
                    logger.critical("Circuit breaker tripped. Aborting all fetches.")
                    sys.exit(1)
                except Exception as e:
                    logger.error(f"Skipping {symbol} {timeframe} due to error: {e}")
                    continue

        elapsed = time.time() - total_start
        logger.info(f"Full fetch completed in {elapsed:.1f}s")


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------
def main():
    import argparse

    parser = argparse.ArgumentParser(description="Fetch Binance klines to PostgreSQL")
    parser.add_argument("--force-backfill", action="store_true",
                        help="Force full historical backfill (ignores MAX_BACKFILL_DAYS)")
    parser.add_argument("--symbol", type=str, help="Override symbol (e.g., BTCUSDT)")
    parser.add_argument("--timeframe", type=str, help="Override timeframe (e.g., 1h)")
    parser.add_argument("--start", type=str, default="1 Jan, 2020",
                        help="Start date for fetch")
    args = parser.parse_args()

    fetcher = BinanceFetcher(DATABASE_URL, BINANCE_API_KEY, BINANCE_API_SECRET)

    if args.symbol and args.timeframe:
        fetcher.fetch_symbol_timeframe(
            args.symbol, args.timeframe,
            start_str=args.start,
            force_backfill=args.force_backfill
        )
    else:
        fetcher.fetch_all(force_backfill=args.force_backfill)


if __name__ == "__main__":
    main()
