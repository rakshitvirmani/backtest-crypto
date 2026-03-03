"""
Configuration template for the backtesting system.
Copy this file to config.py and fill in your values.
NEVER commit config.py to version control.
"""
import os

# Database Configuration (override with env vars)
DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = int(os.getenv("DB_PORT", "5432"))
DB_USER = os.getenv("DB_USER", "trading_user")
DB_PASSWORD = os.getenv("DB_PASSWORD", "CHANGE_ME")
DB_NAME = os.getenv("DB_NAME", "trading_db")

DATABASE_URL = (
    f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
)

# Binance API Configuration
BINANCE_API_KEY = os.getenv("BINANCE_API_KEY", "")
BINANCE_API_SECRET = os.getenv("BINANCE_API_SECRET", "")

# Data Fetch Settings
SYMBOLS = ["BTCUSDT"]
TIMEFRAMES = ["1h", "4h", "1d", "1w", "1M"]
MAX_BACKFILL_DAYS = 7  # Never backfill beyond this without explicit flag
RATE_LIMIT_REQUESTS_PER_MIN = 1200
RATE_LIMIT_SAFETY_MARGIN = 0.85  # Pause at 85% of limit

# Retry Configuration
MAX_RETRIES = 5
RETRY_BASE_DELAY_SEC = 1
RETRY_MAX_DELAY_SEC = 32
CIRCUIT_BREAKER_THRESHOLD = 3  # Consecutive failures before halt
FETCH_TIMEOUT_ALERT_SEC = 30

# Backtest Defaults
DEFAULT_INITIAL_CAPITAL = 10000.0
DEFAULT_POSITION_SIZE_PCT = 0.95
DEFAULT_SLIPPAGE_PCT = 0.05  # 5 bps
DEFAULT_COMMISSION_PCT = 0.1  # 10 bps
DEFAULT_MAX_DRAWDOWN_STOP = -0.15  # -15%

# Validation Thresholds
MIN_TRADES_FOR_SIGNIFICANCE = 50
MAX_ACCEPTABLE_DRAWDOWN = -0.20  # -20%
MIN_SHARPE_FOR_LIVE = 1.0
MIN_PROFIT_FACTOR = 1.2
MIN_WIN_RATE = 0.35

# Alerting (fill in for production)
SLACK_WEBHOOK_URL = os.getenv("SLACK_WEBHOOK_URL", "")
ALERT_EMAIL = os.getenv("ALERT_EMAIL", "")

# Logging
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")
LOG_FILE = "logs/backtest.log"
