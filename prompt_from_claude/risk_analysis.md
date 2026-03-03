# Production Backtester: Critical Risks & Mitigations

## As a Stock Analyst Evaluating This System

You're building an automated trading system that will execute real trades with real capital. The statistical and operational risks are substantial. Here's what keeps me awake at night:

---

## Market Risk Category: Signal Degradation

**The Problem:** Your backtest shows a 2.5 Sharpe ratio on historical data (2015-2025), but live market conditions in 2026 are fundamentally different.

- **Cryptocurrency market maturity**: BTC has evolved from wild west to institutional adoption. Volatility patterns that worked in 2015 won't work in 2025.
- **Regime changes**: Your Bollinger Bands strategy might work in ranging markets but fail spectacularly in fast-moving volatility shocks (like March 2020 or May 2021 crashes).
- **Crowded trades**: If this strategy is obvious (high volume on Bollinger Bands), institutional front-runners already exploit it.

**Mitigation:**
- Walk-forward validation must span AT LEAST 3 separate 2+ year regimes (bull run, sideways, bear market)
- Check correlation between backtest periods: if 2015-2018 and 2022-2025 have different Sharpe, you have regime risk
- Set live trading capital at 10-25% of what backtest suggests (assume 50-75% edge decay in production)
- Monitor 14-day rolling Sharpe on live trades; if it drops < 0.3, pause and re-optimize

---

## Statistical Risk: Overfitting & Data Mining Bias

**The Problem:** You tested 3 strategies with tunable parameters. That's implicit multiple hypothesis testing.

Using standard rules:
- 3 strategies × 5 parameter combinations each = 15 tests
- At 5% Type I error rate per test, probability that AT LEAST ONE shows false positive = 1 - (0.95)^15 ≈ 54%

**If you tested 10 parameter combos per strategy, that jumps to 85% chance of spurious results.**

**Critical example from your system:**
- SuperTrend (10, 3) showed +18% return in backtest
- But if you actually tested 50 parameter combinations to find this "best" one, you likely overfit
- Expected live performance: -5% to +5% (accounting for slippage, market regime changes)

**Mitigation:**
- Pre-specify your 3 strategies AND their parameters BEFORE backtesting
  - Bollinger Bands (period=20, std=2) ← These are standard, don't tune
  - EMA 9/21 ← Standard setup, no tuning
  - SuperTrend (10,3) ← Industry default from TradingView, don't optimize (or optimization shows minimal improvement)
- If you DO optimize, use *only* out-of-sample (OOS) data to select winner
- Report BOTH in-sample (2015-2022) and OOS (2023-2025) Sharpe; they should be similar
  - If IS Sharpe=2.5 and OOS Sharpe=0.8, massive overfitting detected
- Bonferroni correction: if testing N variations, require OOS Sharpe > 1.5 (not 1.0) to pass

---

## Execution Risk: Slippage & Commission Assumptions

**Current Assumptions in Your Prompt:**
- Slippage: 0.05% per trade
- Commission: 0.1% per trade

**Reality Check (Binance Spot BTC/USDT):**
- Maker fee: 0.1% (you likely get this as a medium-frequency trader)
- Taker fee: 0.1% (when executing market orders)
- Bid-ask spread: 0.01-0.05% during normal hours, 0.1-0.3% during low liquidity
- Price impact on large orders: 0.05-0.5% depending on position size and market conditions

**If your system trades 20 times/month and uses 50% of available liquidity, actual costs are:**
- 20 trades × (0.1% entry + 0.15% spread impact + 0.1% exit) ≈ 0.7% monthly drag
- Over 12 months: nearly -8% just from friction
- **Your backtest assumes this is factored in; live reality may be worse**

**Mitigation:**
- Backtest at 0.25% cost per trade (entry + exit), not 0.15%
- Cap position size at 1-2% of 24h volume (ensure you can exit within 2 minutes)
- Use only maker orders (limit orders, not market orders) to reduce slippage
- If you can't hit these constraints, your edge is too thin for live deployment

---

## Liquidity & Position Sizing Risk

**Scenario:** SuperTrend signals a 10 BTC entry on a 1h candle close.
- Binance 1h BTC volume: typically 800-1200 BTC
- Your 10 BTC order = 1-1.25% of hourly volume
- Execution time: 30-60 seconds
- Slippage on market order: 0.05-0.15%, not 0.05%
- If signal reverses during execution, you're underwater immediately

**Mitigation:**
- Never position size based on percentage of portfolio alone; also check market liquidity
- Max position: min(1% of portfolio, 0.5% of 24h volume)
- Use limit orders only; accept 10-20% of signals being missed (better than overpaying for execution)
- Add 15-minute cooldown after large orders (let order book rebalance)

---

## Data Quality & Survivorship Bias

**Current Plan:** Fetch all klines from Binance API dating back to inception.

**Risks:**
- Binance API limitations: only provides ~1000 candles per request, and data gaps exist for pre-2017
- "Clean" backtest data ≠ real trading data (no flash crashes, no circuit breakers captured)
- You're not accounting for what happens on exchange downtime or forced liquidations

**Mitigation:**
- Validate all data fetches: checksum against multiple exchanges (Kraken, FTX history if available)
- Set data quality threshold: if >0.5% of days are missing, don't backtest that period
- Run sensitivity analysis: How much does performance change if slippage jumps to 0.30%? If returns drop >50%, your edge is fragile

---

## Black Swan / Tail Risk

**Historical precedent in crypto:**
- May 2021: BTC crashed 30% in 2 days
- March 2020: Liquidation cascade (though pre-crypto maturity)
- November 2022: FTX collapse, sentiment shock

**Your backtest assumes:**
- Markets are relatively liquid
- No circuit breakers (halt trading)
- No exchange shutdowns
- No flash crashes

**Reality:**
- Crypto exchanges do go down (remember Kraken outages, Binance API limits)
- During crashes, slippage can hit 5-10% (not 0.05%)
- Stop-loss orders might not execute (slippage fills you far below expected exit)

**Mitigation:**
- Max position size = amount you can exit within 10 minutes even during 50% volatility spike
- If strategy holds avg 24 hours per trade, you're exposed to overnight gap risk (crypto trades 24/7, you can't react)
- Add hard stop-loss at -3% per trade (not -15% portfolio DD, but per-trade protection)
- Simulate 2008-like crash scenario: If BTC crashed 70%, would your positions liquidate? If yes, position size is too large

---

## Monitoring & Live Degradation

**Backtest Result:** Sharpe 1.8, Return 24% annualized, Max DD -12%

**Realistic Outcomes in First 6 Months Live:**
- Sharpe 0.6-1.2 (edge is smaller than expected due to overfitting, market changes, slippage)
- Return 5-15% annualized (60-75% of backtest expectation)
- Max DD -18-25% (higher than backtest, regime changes, stress events)

**Don't panic at these numbers; DO panic at:**
- Sharpe dropping below 0.2 (no edge at all)
- Win rate dropping below 30% (strategy broken)
- Max DD reaching -30%+ early (catastrophic risk)

**Mitigation:**
- First 1 month: paper trading only (no real capital)
- Months 2-3: 5% of target capital, monitor closely
- Months 4-6: 25% of target capital only if first 3 months show consistent performance
- If live Sharpe < 0.5 for 2 consecutive weeks, PAUSE and re-optimize
- Weekly monitoring: equity curve, win rate, avg trade duration should match backtest

---

## Crypto-Specific Risks

**Unlike stock markets:**
1. No SEC oversight (exchanges can shut down, rules change overnight)
2. Regulatory risk (US bans on crypto trading, capital controls, etc.)
3. Fork risk (BTC/BCH split affected trading in 2017)
4. Custody risk (if you withdraw to cold wallet, network delays can cause slippage)

**Mitigation:**
- Keep capital in segregated, regulated exchange (Kraken, Coinbase Institutional, not shady exchanges)
- Never leave >5% of portfolio on any single exchange
- If regulations change, have pause mechanism (not auto-trading, manual override)

---

## Operational Risk

**Your system runs 24/7. What happens when:**

- Your EC2 instance crashes at 3 AM (position is open, no one monitoring)
- Network latency spikes and orders execute at terrible prices
- PostgreSQL database becomes corrupt (backtest results lost, audit trail gone)
- You realize a bug only after losing $5,000

**Mitigation:**
- Automated daily backup of database (S3, not just local)
- Health check: every 1 hour, verify database connection + latest trade log exists
- Alert mechanism: Slack/email for every trade + daily summary + any system errors
- Kill switch: manual override that closes all positions within 5 minutes (in case of bugs)
- Sandbox environment that's identical to production; test all updates there first
- Code review by another Python developer before ANY live deployment

---

## The Real Talk: Kelly Criterion & Position Sizing

Your backtest says: Sharpe 1.8, Win Rate 60%, Avg Win $200, Avg Loss $100

Using Kelly Criterion (f = (p × b - q) / b, where p=win%, q=loss%, b=odds):
- Optimal fractional capital per trade: ~12-15% of portfolio
- BUT this assumes edge is real and won't degrade
- SAFE fractional Kelly: 25% of theoretical Kelly = 3-4% per trade

If you follow backtest position sizing (95% of capital per trade), you're running MAXIMUM leverage. One bad regime shift and you're wiped out.

**Mitigation:**
- Cap single position at 2-3% of portfolio (not 95%)
- This means backtest returns of 24% annualized become 5-7% real world (still good if you nail the edge)
- Better to survive and compound than blow up reaching for max returns

---

## Pre-Deployment Validation Checklist (Beyond Code)

- [ ] Can you articulate WHY each strategy should work in 2026? (Not just "it worked in 2022")
- [ ] What would make you WRONG? (Under what market conditions does this completely fail?)
- [ ] Do you have 2+ years of out-of-sample results showing consistent edge?
- [ ] Is the Sharpe ratio on OOS data > 1.0? (If not, no edge)
- [ ] What's your monthly P&L comfort zone? (If a month of -10% losses breaks you emotionally, position size is too large)
- [ ] Can you articulate slippage + commission assumptions to a professional trader without flinching?
- [ ] Do you have a documented exit plan? (At what performance level do you turn this off?)

---

## Summary: The Risk Hierarchy

**From most to least dangerous:**

1. **Overfitting** (backtest is optimistic) → Use walk-forward validation, OOS testing
2. **Signal degradation** (market changed) → Monitor live Sharpe weekly, pause if < 0.5
3. **Position sizing too aggressive** → Cap at 2-3% per trade, not 95%
4. **Execution slippage underestimated** → Backtest at 0.25% friction, not 0.15%
5. **Operational failures** → Backups, monitoring, kill switches
6. **Tail risk / black swan** → Accept you can't predict; just keep position small enough to survive it

**If you do these 6 things, you have a fighting chance. If you skip any of them, you're gambling with real money.**
