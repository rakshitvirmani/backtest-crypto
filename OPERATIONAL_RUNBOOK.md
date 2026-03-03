# Operational Runbook

## System Overview

This system fetches OHLCV data from Binance, stores it in PostgreSQL, runs backtests with walk-forward validation, and (when approved) executes live trades.

## Architecture

```
Binance API -> fetch_to_db.py -> PostgreSQL -> backtester.py -> Reports
                                      |
                                      v
                               optimizer.py -> Best Parameters
                                      |
                                      v
                               risk_manager.py -> Live Trading (future)
```

## Quick Reference

### Start the system
```bash
# Start database
docker compose up -d postgres

# Fetch data
docker compose run --rm fetcher

# Run backtest
docker compose --profile backtest run --rm backtester

# Run optimization
docker compose --profile optimize run --rm optimizer
```

### Stop everything
```bash
docker compose down
```

### Check database
```bash
docker compose exec postgres psql -U trading_user -d trading_db -c "SELECT COUNT(*) FROM klines;"
```

---

## Standard Operating Procedures

### SOP-1: Daily Data Fetch

1. Run the fetcher: `docker compose run --rm fetcher`
2. Check logs: `tail -100 logs/backtest.log`
3. Verify no errors in fetch_log table:
   ```sql
   SELECT * FROM fetch_log WHERE errors IS NOT NULL ORDER BY created_at DESC LIMIT 10;
   ```
4. Verify data freshness:
   ```sql
   SELECT symbol, timeframe, MAX(open_time), COUNT(*)
   FROM klines GROUP BY symbol, timeframe;
   ```

### SOP-2: Running a Backtest

1. Ensure data is up to date (SOP-1)
2. Run backtest:
   ```bash
   python backtester.py --symbol BTCUSDT --timeframe 4h --strategy supertrend \
     --start 2021-01-01 --end 2024-12-31 --params '{"st_length": 10, "st_multiplier": 3.0}'
   ```
3. Review validation status in output
4. Generate reports:
   ```bash
   python report_generator.py <RUN_ID> --report all
   ```
5. Review certification report before any deployment decisions

### SOP-3: Parameter Optimization

1. Run optimizer:
   ```bash
   python optimizer.py --symbol BTCUSDT --timeframe 4h --strategy supertrend
   ```
2. Review top parameters (sorted by OOS Sharpe)
3. Check overfit_ratio (train_sharpe / test_sharpe) — should be < 2.0
4. Run full backtest with best params for complete audit trail
5. If multiple strategies, run `python optimizer.py` without --strategy for full ranking

### SOP-4: Pre-Deployment Review

1. Complete DEPLOYMENT_CHECKLIST.md (every box must be checked)
2. Generate certification report
3. Have second developer review
4. Run paper trading for minimum 2 weeks
5. Get written approval before live deployment

---

## Troubleshooting

### Issue: Fetch fails with rate limit error
**Cause:** Binance API rate limit exceeded
**Fix:** The system has built-in rate limiting. If you see this:
1. Check `RATE_LIMIT_SAFETY_MARGIN` in config.py (default: 0.85)
2. Reduce to 0.7 if still hitting limits
3. Reduce number of timeframes fetched per run

### Issue: Circuit breaker tripped
**Cause:** 3 consecutive fetch failures
**Fix:**
1. Check Binance API status: https://www.binance.com/en/support/announcement
2. Check your API key validity
3. Check network connectivity
4. After resolving, restart the fetcher

### Issue: Backtest returns "No data found"
**Cause:** Data not fetched for that symbol/timeframe/period
**Fix:**
1. Check what data exists:
   ```sql
   SELECT MIN(open_time), MAX(open_time) FROM klines
   WHERE symbol = 'BTCUSDT' AND timeframe = '4h';
   ```
2. Fetch missing data: `python fetch_to_db.py --symbol BTCUSDT --timeframe 4h --start "1 Jan, 2020"`

### Issue: Data validation failures
**Cause:** Bad data from API (OHLC ordering, gaps, etc.)
**Fix:**
1. Check specific errors in logs
2. If OHLC ordering: data corruption at source; re-fetch that period
3. If gaps: exchange downtime; document and accept or interpolate
4. If volume anomalies: review manually; may be legitimate

### Issue: Database connection refused
**Cause:** PostgreSQL not running or wrong credentials
**Fix:**
1. Check container: `docker compose ps`
2. Check logs: `docker compose logs postgres`
3. Verify .env file credentials match docker-compose.yml

---

## Emergency Procedures

### EMERGENCY: Kill switch triggered in live trading
1. System automatically stops trading
2. DO NOT restart without understanding WHY
3. Review equity curve and recent trades
4. Check if market conditions changed (flash crash, black swan)
5. Consult with team before resuming

### EMERGENCY: Suspected data corruption
1. Stop all backtests and live trading IMMEDIATELY
2. Query: `SELECT * FROM klines WHERE data_checksum IS NULL;`
3. Cross-reference with Binance API data
4. If corruption confirmed, re-fetch affected period with `--force-backfill`
5. Re-run all backtests that used corrupted data

### EMERGENCY: Unexpected large loss in live trading
1. Kill switch should activate automatically
2. If not, manually stop: close all positions immediately
3. Document: timestamp, position details, market conditions
4. Compare live execution vs backtest expectations
5. Root cause analysis before resuming

---

## Monitoring Checklist (Daily)

- [ ] Data fetch completed successfully (check logs)
- [ ] No errors in fetch_log table
- [ ] Database size within expected range
- [ ] If live: P&L within expected range
- [ ] If live: No alerts triggered
- [ ] If live: Drawdown within acceptable limits

## Monitoring Checklist (Weekly)

- [ ] Live performance vs backtest comparison
- [ ] Rolling Sharpe ratio trend
- [ ] Slippage analysis (actual vs assumed)
- [ ] Commission costs review
- [ ] Database backup verified
- [ ] System resource usage (disk, memory)

---

## Contact

- On-call: ___________________
- Escalation: ___________________
- Database admin: ___________________
