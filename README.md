# Production-Grade Crypto Backtesting System

Database-first backtesting system designed for safety-first live trading deployment.

## Architecture

```
┌─────────────┐     ┌──────────────┐     ┌─────────────────┐
│  Binance API │────>│ fetch_to_db  │────>│   PostgreSQL    │
│  (OHLCV)    │     │  + Validation│     │  (klines, runs) │
└─────────────┘     └──────────────┘     └────────┬────────┘
                                                   │
                    ┌──────────────┐               │
                    │  optimizer   │<──────────────┤
                    │  Walk-Fwd    │               │
                    └──────┬───────┘               │
                           │                       │
                    ┌──────v───────┐               │
                    │  backtester  │<──────────────┘
                    │  + Audit Log │
                    └──────┬───────┘
                           │
              ┌────────────┼────────────┐
              v            v            v
        ┌──────────┐ ┌──────────┐ ┌──────────────┐
        │  Reports │ │   Risk   │ │ Certification│
        │  HTML/CSV│ │  Manager │ │   Sign-off   │
        └──────────┘ └──────────┘ └──────────────┘
```

## Quick Start

```bash
# 1. Copy config and fill in values
cp config.example.py config.py
cp .env.example .env
# Edit both files with your credentials

# 2. Start database
docker compose up -d postgres

# 3. Fetch data
docker compose run --rm fetcher

# 4. Run a backtest
python backtester.py --symbol BTCUSDT --timeframe 4h --strategy supertrend

# 5. Optimize parameters
python optimizer.py --symbol BTCUSDT --timeframe 4h --strategy supertrend

# 6. Generate reports
python report_generator.py <RUN_ID> --report all
```

## Components

| File | Purpose |
|------|---------|
| `fetch_to_db.py` | Data ingestion with retry, validation, circuit breaker |
| `backtester.py` | Core backtesting engine with 5 strategies |
| `optimizer.py` | Walk-forward parameter optimization |
| `risk_manager.py` | Kill switches and pre-trade checks |
| `report_generator.py` | Trade logs, equity curves, HTML reports |
| `schema.sql` | PostgreSQL schema with integrity constraints |
| `config.example.py` | Configuration template |

## Strategies

- Bollinger Bands (mean reversion)
- EMA 9/21 Crossover (trend following)
- SuperTrend (trend following)
- RSI Mean Reversion
- MACD Crossover

## Safety Features

- OHLC integrity checks at database level (CHECK constraints)
- Data validation before every backtest
- Walk-forward validation (70/30 train/test split)
- Overfit detection (train vs test Sharpe comparison)
- Max drawdown kill switch (-15% default)
- Daily loss limit
- Consecutive loss pause
- Price sanity checks
- Full audit trail (every trade, every run)
- Human sign-off required before live deployment

## Documentation

- `DEPLOYMENT_CHECKLIST.md` - Must complete before live trading
- `OPERATIONAL_RUNBOOK.md` - How to operate, troubleshoot, and monitor
