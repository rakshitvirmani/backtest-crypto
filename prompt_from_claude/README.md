# Production Backtester System - Complete Package

This package contains everything you need to build a production-grade crypto trading backtester with real-money deployment safeguards.

## Files Overview

### 1. **assessment_summary.md** (START HERE)
- Compares your original prompt (6/10) to production-grade version
- Lists all critical gaps organized by risk tier
- Quantifies what can go wrong and probability of ruin
- Explains why production-grade matters for real capital

### 2. **production_backtester_system_prompt.md** (MAIN SYSTEM PROMPT)
The complete specification for Claude to build your system. Includes:
- 7 detailed tasks (data pipeline, backtesting, optimization, risk management, reporting, checklist, infrastructure)
- Full Python code examples ready to use
- Database schema with integrity constraints
- Risk management classes and kill-switches
- Monitoring and alerting specifications

### 3. **risk_analysis.md** (PRE-DEPLOYMENT READING)
A stock analyst's perspective on what can break:
- Signal degradation (backtest ≠ live performance)
- Overfitting and multiple hypothesis testing bias
- Slippage and commission underestimation
- Liquidity constraints and position sizing
- Data quality issues
- Black swan / tail risk scenarios
- Monitoring thresholds for live trading
- Pre-deployment validation checklist

### 4. **infrastructure_config.txt** (DEPLOYMENT FILES)
Production-ready templates:
- requirements.txt (pinned Python versions)
- docker-compose.yml (PostgreSQL + backtester services)
- Dockerfile (containerized backtester)
- .env.example (configuration template)
- config.py (configuration management)
- Makefile (convenience commands)
- DEPLOYMENT_CHECKLIST.md (sign-off requirements)

## How to Use This Package

### Option A: Build the Full Production System (Recommended)
1. Read assessment_summary.md (understand the gaps)
2. Review risk_analysis.md (understand what can go wrong)
3. Share production_backtester_system_prompt.md with Claude Code
4. Use infrastructure files to set up Docker environment
5. Follow 8-week implementation plan in assessment_summary.md

### Option B: Start with Prototype (Faster, More Risk)
1. Use your original prompt for initial development
2. Add walk-forward validation from production_backtester_system_prompt.md Task 3
3. Implement risk checks from risk_analysis.md before ANY live deployment
4. Plan to upgrade to full production system within 3 months

## Key Statistics from Assessment

- **Original prompt rating:** 6/10 (prototype-grade)
- **Risk of ruin without production safeguards:** 25-30% within 6 months
- **Expected performance degradation:** 50-75% (backtest to live)
- **Walk-forward testing gap:** Critical vulnerability in original prompt
- **Implementation timeline:** 8 weeks for full production system

## Critical Success Factors

These are non-negotiable if deploying real capital:

1. ✅ Walk-forward validation (OOS testing with Sharpe > 1.0)
2. ✅ Realistic slippage/commission assumptions (0.25%+ per trade)
3. ✅ Hard position sizing limits (2-3% per trade, not 95%)
4. ✅ Code review by external developer
5. ✅ 2+ weeks paper trading on testnet
6. ✅ Daily monitoring + automated alerts
7. ✅ Documented exit criteria (stop deployment if Sharpe < 0.3)
8. ✅ Operational runbook + kill switches

## Questions Before You Start

Ask yourself these before deploying real capital:

- [ ] Can I articulate WHY each strategy should work in 2026? (Not just "it worked in 2022")
- [ ] What market conditions would break this completely?
- [ ] Am I comfortable losing 15% in a single month?
- [ ] Can I stick to the position sizing rules when backtests show higher returns?
- [ ] Do I have 1-2 hours daily to monitor this system?
- [ ] Have I verified my backtest assumptions against real Binance data?

If you answered "no" to any, read risk_analysis.md again and adjust your plan.

## Additional Notes

- All code examples are production-ready but assume Python 3.11+
- Docker setup enables reproducible environments (essential for production)
- Database schema includes audit tables for every backtest run + trade
- Configuration is environment-variable driven (no secrets in code)
- Logging is JSON-formatted for easy parsing/alerting

## Contact / Support

These files were designed to be used with Claude Code or Claude Cowork:
1. Share production_backtester_system_prompt.md with Claude
2. Provide infrastructure_config.txt for Docker/configuration setup
3. Use assessment_summary.md to explain context and constraints

---

**Remember:** The cost of a bug in a trading system is literal money loss. Take the time to do this right.
