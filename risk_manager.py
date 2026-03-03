"""
risk_manager.py - Risk Management & Kill Switches
===================================================
Live trading safeguards. Every check here can STOP trading.
If in doubt, STOP. Money lost to caution < money lost to hubris.
"""

import logging
from datetime import datetime, timedelta
from typing import Dict, Optional, Tuple

import numpy as np
import pandas as pd
from sqlalchemy import create_engine, text

try:
    from config import DATABASE_URL
except ImportError:
    DATABASE_URL = None

logger = logging.getLogger("risk_manager")


class RiskManager:
    """
    Live trading safeguards.
    Every method returns a clear PASS/FAIL with reasoning.
    """

    def __init__(self, db_url: str = None):
        self.engine = create_engine(db_url or DATABASE_URL) if (db_url or DATABASE_URL) else None

    # ------------------------------------------------------------------
    # Kill Switches
    # ------------------------------------------------------------------
    @staticmethod
    def check_max_drawdown(
        equity_curve: np.ndarray, max_dd_threshold: float = -0.15
    ) -> Tuple[bool, float, str]:
        """
        Kill switch: return (should_stop, current_dd, reason).
        should_stop=True means HALT ALL TRADING IMMEDIATELY.
        """
        if len(equity_curve) < 2:
            return False, 0.0, "Insufficient data"

        running_max = np.maximum.accumulate(equity_curve)
        drawdowns = (equity_curve - running_max) / running_max
        current_dd = drawdowns[-1]
        max_dd = np.min(drawdowns)

        if max_dd < max_dd_threshold:
            reason = (
                f"KILL SWITCH: Max drawdown {max_dd:.2%} breached threshold {max_dd_threshold:.2%}. "
                f"Current DD: {current_dd:.2%}. STOP TRADING."
            )
            logger.critical(reason)
            return True, max_dd, reason

        if current_dd < max_dd_threshold * 0.8:
            reason = (
                f"WARNING: Approaching kill switch. Current DD: {current_dd:.2%}, "
                f"threshold: {max_dd_threshold:.2%}"
            )
            logger.warning(reason)

        return False, current_dd, "OK"

    @staticmethod
    def validate_position_size(
        capital: float,
        position_size_usd: float,
        max_position_pct: float = 0.05,
    ) -> Tuple[bool, str]:
        """
        No single position > max_position_pct of capital.
        Returns (is_valid, reason).
        """
        if capital <= 0:
            return False, "Capital is zero or negative"

        ratio = position_size_usd / capital
        if ratio > max_position_pct:
            reason = (
                f"REJECTED: Position ${position_size_usd:.2f} is {ratio:.1%} of "
                f"capital ${capital:.2f} (max: {max_position_pct:.1%})"
            )
            logger.warning(reason)
            return False, reason

        return True, f"OK: {ratio:.1%} of capital"

    @staticmethod
    def check_daily_loss_limit(
        daily_pnl: float, daily_loss_limit: float = -0.03
    ) -> Tuple[bool, str]:
        """
        Stop trading for the day if daily loss exceeds limit.
        Returns (should_stop, reason).
        """
        if daily_pnl < daily_loss_limit:
            reason = (
                f"DAILY STOP: P&L {daily_pnl:.2%} breached daily limit {daily_loss_limit:.2%}"
            )
            logger.critical(reason)
            return True, reason
        return False, f"OK: Daily P&L {daily_pnl:.2%}"

    @staticmethod
    def check_consecutive_losses(
        recent_trades_pnl: list, max_consecutive: int = 5
    ) -> Tuple[bool, str]:
        """
        Pause trading after N consecutive losses.
        Returns (should_pause, reason).
        """
        if not recent_trades_pnl:
            return False, "No recent trades"

        consecutive = 0
        for pnl in reversed(recent_trades_pnl):
            if pnl < 0:
                consecutive += 1
            else:
                break

        if consecutive >= max_consecutive:
            reason = (
                f"PAUSE: {consecutive} consecutive losses (threshold: {max_consecutive}). "
                f"Review strategy before resuming."
            )
            logger.warning(reason)
            return True, reason

        return False, f"OK: {consecutive} consecutive losses"

    @staticmethod
    def check_price_sanity(
        current_price: float,
        last_known_price: float,
        max_deviation_pct: float = 0.10,
    ) -> Tuple[bool, str]:
        """
        Reject trades if current price deviates too much from last known.
        Protects against flash crashes and data feed errors.
        """
        if last_known_price <= 0:
            return False, "Invalid last known price"

        deviation = abs(current_price - last_known_price) / last_known_price
        if deviation > max_deviation_pct:
            reason = (
                f"PRICE ALERT: Current ${current_price:.2f} deviates {deviation:.2%} "
                f"from last known ${last_known_price:.2f} (max: {max_deviation_pct:.2%}). "
                f"Possible data feed error."
            )
            logger.critical(reason)
            return False, reason

        return True, f"OK: {deviation:.2%} deviation"

    @staticmethod
    def check_spread(
        bid: float, ask: float, max_spread_pct: float = 0.005
    ) -> Tuple[bool, str]:
        """
        Reject trades if spread is too wide (illiquid market).
        """
        if bid <= 0 or ask <= 0:
            return False, "Invalid bid/ask prices"

        spread_pct = (ask - bid) / bid
        if spread_pct > max_spread_pct:
            reason = (
                f"SPREAD WARNING: {spread_pct:.3%} exceeds max {max_spread_pct:.3%}. "
                f"Market may be illiquid."
            )
            logger.warning(reason)
            return False, reason

        return True, f"OK: Spread {spread_pct:.3%}"

    # ------------------------------------------------------------------
    # Pre-Trade Validation (all checks combined)
    # ------------------------------------------------------------------
    def pre_trade_check(
        self,
        capital: float,
        position_size_usd: float,
        current_price: float,
        last_known_price: float,
        equity_curve: np.ndarray,
        daily_pnl: float,
        recent_trades_pnl: list,
        max_dd_threshold: float = -0.15,
        max_position_pct: float = 0.05,
    ) -> Tuple[bool, List]:
        """
        Run ALL pre-trade checks. Returns (can_trade, list_of_issues).
        """
        issues = []

        # Kill switch
        should_stop, dd, reason = self.check_max_drawdown(equity_curve, max_dd_threshold)
        if should_stop:
            issues.append(("CRITICAL", reason))

        # Position size
        valid, reason = self.validate_position_size(capital, position_size_usd, max_position_pct)
        if not valid:
            issues.append(("ERROR", reason))

        # Daily loss
        should_stop, reason = self.check_daily_loss_limit(daily_pnl)
        if should_stop:
            issues.append(("CRITICAL", reason))

        # Consecutive losses
        should_pause, reason = self.check_consecutive_losses(recent_trades_pnl)
        if should_pause:
            issues.append(("WARNING", reason))

        # Price sanity
        valid, reason = self.check_price_sanity(current_price, last_known_price)
        if not valid:
            issues.append(("ERROR", reason))

        can_trade = not any(level in ("CRITICAL", "ERROR") for level, _ in issues)

        if not can_trade:
            logger.error(f"PRE-TRADE CHECK FAILED: {len(issues)} issues found")
            for level, issue in issues:
                logger.error(f"  [{level}] {issue}")
        else:
            logger.info("Pre-trade check: PASSED")

        return can_trade, issues

    # ------------------------------------------------------------------
    # Live Performance Monitoring
    # ------------------------------------------------------------------
    def check_live_vs_backtest(
        self,
        live_sharpe: float,
        backtest_sharpe: float,
        live_max_dd: float,
        backtest_max_dd: float,
        sharpe_degradation_threshold: float = 0.5,
        dd_excess_threshold: float = 0.20,
    ) -> Dict:
        """
        Compare live performance vs backtest expectations.
        Returns dict of alerts.
        """
        alerts = {}

        if live_sharpe < sharpe_degradation_threshold:
            alerts["sharpe_degradation"] = (
                f"ALERT: Live Sharpe ({live_sharpe:.2f}) below threshold "
                f"({sharpe_degradation_threshold:.2f}). Edge may be degrading."
            )

        if backtest_max_dd != 0:
            dd_excess = abs(live_max_dd) / abs(backtest_max_dd) - 1
            if dd_excess > dd_excess_threshold:
                alerts["drawdown_excess"] = (
                    f"ALERT: Live max DD ({live_max_dd:.2%}) exceeds backtest DD "
                    f"({backtest_max_dd:.2%}) by {dd_excess:.1%}"
                )

        sharpe_ratio = live_sharpe / backtest_sharpe if backtest_sharpe != 0 else 0
        if sharpe_ratio < 0.5:
            alerts["performance_decay"] = (
                f"ALERT: Live/Backtest Sharpe ratio ({sharpe_ratio:.2f}) indicates "
                f"significant performance decay."
            )

        if alerts:
            for key, msg in alerts.items():
                logger.warning(msg)
        else:
            logger.info("Live vs backtest check: All within tolerance")

        return alerts


# ---------------------------------------------------------------------------
# Pre-Deployment Validator
# ---------------------------------------------------------------------------
class PreDeploymentValidator:
    """
    Validates that a backtest run meets ALL criteria before live deployment.
    This is the final gate. Every check must pass.
    """

    CRITERIA = {
        "min_sharpe": 1.0,
        "max_drawdown": -0.20,        # -20%
        "min_trades": 50,
        "min_profit_factor": 1.2,
        "min_win_rate": 0.35,
        "max_overfit_ratio": 2.0,      # train_sharpe / test_sharpe
    }

    @classmethod
    def validate_for_deployment(
        cls,
        backtest_results: Dict,
        oos_results: Optional[Dict] = None,
    ) -> Tuple[bool, List[str]]:
        """
        Returns (approved, list_of_failures).
        ALL criteria must pass for approval.
        """
        failures = []
        warnings = []

        # Sharpe ratio
        sharpe = backtest_results.get("sharpe_ratio", 0)
        if sharpe < cls.CRITERIA["min_sharpe"]:
            failures.append(
                f"Sharpe {sharpe:.2f} < {cls.CRITERIA['min_sharpe']} (required)"
            )

        # Max drawdown
        max_dd = backtest_results.get("max_drawdown", 0)
        if max_dd < cls.CRITERIA["max_drawdown"] * 100:  # stored as percentage
            failures.append(
                f"Max DD {max_dd:.1f}% exceeds {cls.CRITERIA['max_drawdown']*100:.0f}% limit"
            )

        # Trade count
        trades = backtest_results.get("num_trades", 0)
        if trades < cls.CRITERIA["min_trades"]:
            failures.append(
                f"Trade count {trades} < {cls.CRITERIA['min_trades']} (insufficient data)"
            )

        # Profit factor
        pf = backtest_results.get("profit_factor", 0)
        if not np.isnan(pf) and pf < cls.CRITERIA["min_profit_factor"]:
            failures.append(
                f"Profit factor {pf:.2f} < {cls.CRITERIA['min_profit_factor']}"
            )

        # Win rate
        wr = backtest_results.get("win_rate", 0)
        if wr < cls.CRITERIA["min_win_rate"]:
            failures.append(
                f"Win rate {wr:.1%} < {cls.CRITERIA['min_win_rate']:.0%}"
            )

        # Overfit check (if OOS results provided)
        if oos_results:
            oos_sharpe = oos_results.get("sharpe_ratio", 0)
            if oos_sharpe > 0:
                overfit_ratio = sharpe / oos_sharpe
                if overfit_ratio > cls.CRITERIA["max_overfit_ratio"]:
                    failures.append(
                        f"Overfit risk: IS/OOS Sharpe ratio {overfit_ratio:.2f} > "
                        f"{cls.CRITERIA['max_overfit_ratio']}"
                    )
            if oos_sharpe < cls.CRITERIA["min_sharpe"]:
                failures.append(
                    f"OOS Sharpe {oos_sharpe:.2f} < {cls.CRITERIA['min_sharpe']} (required)"
                )

        approved = len(failures) == 0
        status = "APPROVED" if approved else "REJECTED"
        logger.info(f"Pre-deployment validation: {status}")
        if failures:
            for f in failures:
                logger.error(f"  FAIL: {f}")

        return approved, failures
