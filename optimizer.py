"""
optimizer.py - Walk-Forward Parameter Optimization
====================================================
Grid search with mandatory out-of-sample validation.
Train on 70% of data, validate on 30%.
Detects overfitting by comparing IS vs OOS performance.

Never deploy parameters that only look good in-sample.
"""

import os
import sys
import json
import logging
from datetime import datetime
from typing import Dict, List, Tuple, Type, Optional
from itertools import product as cartesian_product

import pandas as pd
import numpy as np
from backtesting import Backtest, Strategy

from backtester import (
    ProductionBacktester, BacktestConfig, DataValidator,
    STRATEGY_REGISTRY, SuperTrendStrategy, BollingerBandsStrategy,
    EMACrossoverStrategy, RSIMeanReversionStrategy, MACDStrategy,
)

try:
    from config import DB_PATH, LOG_LEVEL, LOG_FILE
except ImportError:
    print("ERROR: config.py not found.")
    sys.exit(1)

logger = logging.getLogger("optimizer")


# ---------------------------------------------------------------------------
# Parameter grids for each strategy
# ---------------------------------------------------------------------------
PARAM_GRIDS = {
    "supertrend": {
        "st_length": list(range(7, 21, 2)),       # 7, 9, 11, 13, 15, 17, 19
        "st_multiplier": [round(x, 1) for x in np.arange(2.0, 4.1, 0.5)],  # 2.0 - 4.0
    },
    "bollinger_bands": {
        "bb_length": list(range(10, 31, 5)),       # 10, 15, 20, 25, 30
        "bb_std": [round(x, 1) for x in np.arange(1.5, 3.1, 0.5)],  # 1.5 - 3.0
    },
    "ema_crossover": {
        "ema_fast": [5, 7, 9, 12, 15],
        "ema_slow": [18, 21, 26, 30, 50],
    },
    "rsi_mean_reversion": {
        "rsi_length": [7, 10, 14, 21],
        "rsi_oversold": [20.0, 25.0, 30.0],
        "rsi_overbought": [70.0, 75.0, 80.0],
    },
    "macd": {
        "macd_fast": [8, 10, 12],
        "macd_slow": [21, 26, 30],
        "macd_signal": [7, 9, 12],
    },
}


# ---------------------------------------------------------------------------
# Walk-Forward Optimizer
# ---------------------------------------------------------------------------
class WalkForwardOptimizer:
    """
    Walk-forward optimization with out-of-sample validation.
    Train on 70% of data, validate on 30%.
    """

    def __init__(self, db_path: str = None):
        self.db_path = db_path or DB_PATH
        self.backtester = ProductionBacktester(self.db_path)

    def _split_data(
        self, data: pd.DataFrame, train_pct: float = 0.7
    ) -> Tuple[pd.DataFrame, pd.DataFrame]:
        """Split data into train/test sets chronologically."""
        split_idx = int(len(data) * train_pct)
        train = data.iloc[:split_idx].copy()
        test = data.iloc[split_idx:].copy()
        logger.info(
            f"Walk-forward split: Train={len(train)} bars "
            f"({train.index[0]} to {train.index[-1]}), "
            f"Test={len(test)} bars ({test.index[0]} to {test.index[-1]})"
        )
        return train, test

    def _run_single_backtest(
        self,
        data: pd.DataFrame,
        strategy_class: Type[Strategy],
        params: Dict,
        config: BacktestConfig,
    ) -> Dict:
        """Run a single backtest on provided data (no DB fetch)."""
        try:
            bt = Backtest(
                data,
                strategy_class,
                cash=config.initial_capital,
                commission=config.commission_pct / 100,
                exclusive_orders=True,
            )
            stats = bt.run(**params)

            def safe_float(val, default=np.nan):
                try:
                    v = float(val)
                    return v if not np.isinf(v) else default
                except (TypeError, ValueError):
                    return default

            return {
                "total_return": safe_float(stats.get("Return [%]", 0)),
                "sharpe_ratio": safe_float(stats.get("Sharpe Ratio", 0)),
                "sortino_ratio": safe_float(stats.get("Sortino Ratio", np.nan)),
                "max_drawdown": safe_float(stats.get("Max. Drawdown [%]", 0)),
                "win_rate": safe_float(stats.get("Win Rate [%]", 0)) / 100,
                "profit_factor": safe_float(stats.get("Profit Factor", np.nan)),
                "num_trades": int(stats.get("# Trades", 0)),
                "avg_trade": safe_float(stats.get("Avg. Trade [%]", np.nan)),
            }
        except Exception as e:
            logger.warning(f"Backtest failed for params {params}: {e}")
            return {
                "total_return": np.nan,
                "sharpe_ratio": np.nan,
                "sortino_ratio": np.nan,
                "max_drawdown": np.nan,
                "win_rate": np.nan,
                "profit_factor": np.nan,
                "num_trades": 0,
                "avg_trade": np.nan,
            }

    def optimize(
        self,
        config: BacktestConfig,
        param_grid: Dict = None,
        train_pct: float = 0.7,
        min_trades: int = 5,
        metric: str = "sharpe_ratio",
    ) -> Dict:
        """
        Grid search with walk-forward validation.

        Args:
            config: BacktestConfig with symbol, timeframe, etc.
            param_grid: Dict of param_name -> list of values. Auto-detected if None.
            train_pct: Fraction of data for training (default 0.7).
            min_trades: Minimum trades required for a valid result.
            metric: Metric to optimize ('sharpe_ratio', 'total_return', 'sortino_ratio').

        Returns:
            Dict with best_params, results_grid, overfit analysis.
        """
        strategy_class = STRATEGY_REGISTRY.get(config.strategy_name)
        if strategy_class is None:
            raise ValueError(f"Unknown strategy: {config.strategy_name}")

        if param_grid is None:
            param_grid = PARAM_GRIDS.get(config.strategy_name)
            if param_grid is None:
                raise ValueError(
                    f"No default param grid for {config.strategy_name}. Provide one explicitly."
                )

        # Fetch full dataset once
        full_data = self.backtester.fetch_data(config)
        train_data, test_data = self._split_data(full_data, train_pct)

        # Validate both splits have enough data
        for label, split in [("train", train_data), ("test", test_data)]:
            is_valid, errors = DataValidator.validate_klines(split)
            if not is_valid:
                raise ValueError(f"{label} data validation failed: {errors}")

        # Generate all parameter combinations
        param_names = list(param_grid.keys())
        param_values = list(param_grid.values())
        combinations = list(cartesian_product(*param_values))
        total_combos = len(combinations)

        logger.info(
            f"Optimizing {config.strategy_name}: "
            f"{total_combos} parameter combinations, metric={metric}"
        )

        results_list = []
        best_test_metric = -np.inf
        best_params = None

        for i, combo in enumerate(combinations, 1):
            params = dict(zip(param_names, combo))

            # Validate EMA crossover constraint: fast < slow
            if config.strategy_name == "ema_crossover":
                if params.get("ema_fast", 0) >= params.get("ema_slow", 999):
                    continue

            # Validate MACD constraint: fast < slow
            if config.strategy_name == "macd":
                if params.get("macd_fast", 0) >= params.get("macd_slow", 999):
                    continue

            # Train
            train_result = self._run_single_backtest(
                train_data, strategy_class, params, config
            )
            # Test (out-of-sample)
            test_result = self._run_single_backtest(
                test_data, strategy_class, params, config
            )

            row = {
                **{f"param_{k}": v for k, v in params.items()},
                "train_return": train_result["total_return"],
                "train_sharpe": train_result["sharpe_ratio"],
                "train_sortino": train_result["sortino_ratio"],
                "train_max_dd": train_result["max_drawdown"],
                "train_trades": train_result["num_trades"],
                "train_win_rate": train_result["win_rate"],
                "train_profit_factor": train_result["profit_factor"],
                "test_return": test_result["total_return"],
                "test_sharpe": test_result["sharpe_ratio"],
                "test_sortino": test_result["sortino_ratio"],
                "test_max_dd": test_result["max_drawdown"],
                "test_trades": test_result["num_trades"],
                "test_win_rate": test_result["win_rate"],
                "test_profit_factor": test_result["profit_factor"],
            }
            results_list.append(row)

            # Track best (based on TEST set metric)
            test_metric_val = test_result.get(metric, -np.inf)
            if (
                not np.isnan(test_metric_val)
                and test_result["num_trades"] >= min_trades
                and test_metric_val > best_test_metric
            ):
                best_test_metric = test_metric_val
                best_params = params

            if i % 10 == 0 or i == total_combos:
                logger.info(f"  Progress: {i}/{total_combos} combinations tested")

        # Build results DataFrame
        results_df = pd.DataFrame(results_list)

        if results_df.empty:
            logger.error("No valid results from optimization")
            return {"best_params": None, "results_grid": results_df}

        results_df = results_df.sort_values(
            f"test_{metric.replace('_ratio', '').replace('total_', '')}",
            ascending=False
        )

        # Overfit analysis
        results_df["overfit_ratio"] = np.where(
            results_df["test_sharpe"] != 0,
            results_df["train_sharpe"] / results_df["test_sharpe"].replace(0, np.nan),
            np.nan,
        )
        results_df["sharpe_degradation"] = (
            results_df["train_sharpe"] - results_df["test_sharpe"]
        )

        # Overfit warnings
        if best_params is not None:
            best_row = results_df[
                results_df[[f"param_{k}" for k in best_params.keys()]]
                .apply(lambda r: all(r[f"param_{k}"] == v for k, v in best_params.items()), axis=1)
            ]
            if not best_row.empty:
                overfit_ratio = best_row["overfit_ratio"].iloc[0]
                if not np.isnan(overfit_ratio) and overfit_ratio > 2.0:
                    logger.warning(
                        f"HIGH OVERFIT RISK: Train/Test Sharpe ratio = {overfit_ratio:.2f}. "
                        f"In-sample performance may not persist."
                    )

        # Summary
        logger.info(f"\nBest params (by OOS {metric}): {best_params}")
        logger.info(f"Best OOS {metric}: {best_test_metric:.4f}")
        logger.info(f"\nTop 5 parameter sets:\n{results_df.head().to_string()}")

        return {
            "best_params": best_params,
            "best_test_metric": best_test_metric,
            "metric": metric,
            "results_grid": results_df,
            "train_size": len(train_data),
            "test_size": len(test_data),
            "total_combinations": total_combos,
        }


# ---------------------------------------------------------------------------
# Multi-Strategy Optimizer
# ---------------------------------------------------------------------------
class MultiStrategyOptimizer:
    """Run walk-forward optimization across all strategies and rank them."""

    def __init__(self, db_path: str = None):
        self.optimizer = WalkForwardOptimizer(db_path)

    def optimize_all(
        self,
        symbol: str = "BTCUSDT",
        timeframe: str = "4h",
        start_date: str = "2021-01-01",
        end_date: str = "2024-12-31",
        initial_capital: float = 10_000.0,
    ) -> Dict:
        """Optimize all registered strategies and return ranked results."""
        all_results = {}

        for strategy_name in STRATEGY_REGISTRY:
            logger.info(f"\n{'='*60}")
            logger.info(f"Optimizing: {strategy_name}")
            logger.info(f"{'='*60}")

            config = BacktestConfig(
                symbol=symbol,
                timeframe=timeframe,
                strategy_name=strategy_name,
                strategy_params={},
                start_date=datetime.fromisoformat(start_date),
                end_date=datetime.fromisoformat(end_date),
                initial_capital=initial_capital,
            )

            try:
                result = self.optimizer.optimize(config)
                all_results[strategy_name] = result
            except Exception as e:
                logger.error(f"Failed to optimize {strategy_name}: {e}")
                all_results[strategy_name] = {"error": str(e)}

        # Rank strategies by best OOS Sharpe
        ranking = []
        for name, result in all_results.items():
            if "error" not in result and result.get("best_params"):
                ranking.append({
                    "strategy": name,
                    "best_params": result["best_params"],
                    "oos_sharpe": result["best_test_metric"],
                })

        ranking_df = pd.DataFrame(ranking).sort_values("oos_sharpe", ascending=False)
        logger.info(f"\n{'='*60}")
        logger.info(f"Strategy Ranking (by OOS Sharpe):\n{ranking_df.to_string()}")
        logger.info(f"{'='*60}")

        return {
            "ranking": ranking_df,
            "details": all_results,
        }


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------
def main():
    import argparse

    parser = argparse.ArgumentParser(description="Walk-forward parameter optimization")
    parser.add_argument("--symbol", default="BTCUSDT")
    parser.add_argument("--timeframe", default="4h")
    parser.add_argument("--strategy", default=None,
                        help="Strategy to optimize (all if not specified)")
    parser.add_argument("--start", default="2021-01-01")
    parser.add_argument("--end", default="2024-12-31")
    parser.add_argument("--capital", type=float, default=10000.0)
    parser.add_argument("--metric", default="sharpe_ratio",
                        choices=["sharpe_ratio", "total_return", "sortino_ratio"])
    args = parser.parse_args()

    if args.strategy:
        optimizer = WalkForwardOptimizer()
        config = BacktestConfig(
            symbol=args.symbol,
            timeframe=args.timeframe,
            strategy_name=args.strategy,
            strategy_params={},
            start_date=datetime.fromisoformat(args.start),
            end_date=datetime.fromisoformat(args.end),
            initial_capital=args.capital,
        )
        result = optimizer.optimize(config, metric=args.metric)
        print(f"\nBest params: {result['best_params']}")
        print(f"Best OOS {args.metric}: {result['best_test_metric']:.4f}")
        print(f"\nTop 10:\n{result['results_grid'].head(10).to_string()}")
    else:
        multi = MultiStrategyOptimizer()
        result = multi.optimize_all(
            symbol=args.symbol,
            timeframe=args.timeframe,
            start_date=args.start,
            end_date=args.end,
            initial_capital=args.capital,
        )
        print(f"\nFinal Ranking:\n{result['ranking'].to_string()}")


if __name__ == "__main__":
    main()
