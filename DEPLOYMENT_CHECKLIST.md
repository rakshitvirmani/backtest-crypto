# Pre-Deployment Checklist

**Strategy:** ___________________
**Run ID:** ___________________
**Date:** ___________________
**Reviewer:** ___________________

---

## Backtest Validation

- [ ] Backtest passes all automated validation criteria (status = PASSED)
- [ ] Walk-forward optimization shows consistent OOS performance
- [ ] Sharpe ratio > 1.0 on BOTH in-sample AND out-of-sample
- [ ] Max drawdown < 20% (ideally < 15%)
- [ ] Trade count > 50 (sufficient statistical significance)
- [ ] Profit factor > 1.2
- [ ] Win rate + 2 * avg_loss >= avg_profit (edge is real, not variance)

## Data Integrity

- [ ] Database checksums verified (no corruption)
- [ ] No timestamp gaps in klines data for the backtest period
- [ ] Volume anomalies reviewed and explained
- [ ] Fetch logs show no errors during data collection

## Code Quality

- [ ] Code reviewed by another developer (name: _______________)
- [ ] All strategies produce deterministic results (same input = same output)
- [ ] No hardcoded credentials or API keys in codebase
- [ ] Error handling covers all failure modes
- [ ] Logging is comprehensive (every trade, every decision)

## Risk Configuration

- [ ] Max position size set: ___% of capital
- [ ] Max drawdown kill switch set: ___%
- [ ] Daily loss limit set: ___%
- [ ] Consecutive loss pause threshold: ___ trades
- [ ] Slippage assumption: ___ bps (verified against real spreads)
- [ ] Commission assumption: ___ bps (verified against exchange fees)

## Paper Trading

- [ ] Paper trading on Binance testnet completed
- [ ] Duration: ___ weeks (minimum 2 required)
- [ ] Paper trading results within 20% of backtest expectations
- [ ] No unexpected errors or crashes during paper trading
- [ ] Latency and fill quality measured and acceptable

## Operational Readiness

- [ ] Operational runbook written and reviewed
- [ ] Monitoring dashboard configured
- [ ] Alert channels set up (Slack/email)
- [ ] Kill switch accessible and tested
- [ ] Rollback procedure documented and tested
- [ ] On-call rotation established

## Final Approval

Approved for live trading: [ ] YES  [ ] NO

Approved by: ___________________
Date: ___________________
Signature: ___________________

---

**If ANY checkbox above is unchecked, DO NOT deploy to live trading.**
