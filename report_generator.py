"""
report_generator.py - Reporting & Audit Trail
================================================
Generates:
1. Trade log export (every entry/exit with timestamp, price, reason, P&L)
2. Equity curve (daily equity progression)
3. Drawdown analysis (longest drawdown periods)
4. Rolling metrics (30/60/90-day Sharpe, return)
5. Backtest certification report (human sign-off required)
6. HTML summary report

All reports are deterministic and reproducible from the database.
"""

import os
import sys
import json
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Optional

import pandas as pd
import numpy as np
from sqlalchemy import create_engine, text

try:
    from config import DATABASE_URL, LOG_FILE
except ImportError:
    DATABASE_URL = None
    LOG_FILE = "logs/backtest.log"

logger = logging.getLogger("report_generator")


class ReportGenerator:
    """Generate comprehensive backtest reports from database records."""

    def __init__(self, db_url: str = None, output_dir: str = "reports"):
        self.engine = create_engine(db_url or DATABASE_URL) if (db_url or DATABASE_URL) else None
        self.output_dir = output_dir
        os.makedirs(output_dir, exist_ok=True)

    # ------------------------------------------------------------------
    # 1. Trade Log Export
    # ------------------------------------------------------------------
    def export_trade_log(self, run_id: str, output_path: str = None) -> pd.DataFrame:
        """Export every trade for a backtest run to CSV."""
        query = text("""
            SELECT
                tl.trade_number, tl.entry_time, tl.exit_time,
                tl.entry_price, tl.exit_price, tl.position_size,
                tl.direction, tl.pnl, tl.pnl_percent,
                tl.is_winning_trade, tl.entry_signal, tl.exit_signal,
                tl.commission_paid, tl.slippage_cost,
                tl.equity_at_entry, tl.equity_at_exit
            FROM trade_log tl
            JOIN backtest_runs br ON tl.backtest_run_id = br.id
            WHERE br.run_id = :run_id
            ORDER BY tl.trade_number
        """)

        with self.engine.connect() as conn:
            df = pd.read_sql(query, conn, params={"run_id": run_id})

        if df.empty:
            logger.warning(f"No trades found for run {run_id}")
            return df

        # Derived columns
        df["cumulative_pnl"] = df["pnl"].cumsum()
        df["cumulative_return_pct"] = df["pnl_percent"].cumsum()
        df["running_win_rate"] = (
            df["is_winning_trade"].expanding().mean()
        )

        if output_path is None:
            output_path = os.path.join(self.output_dir, f"trades_{run_id[:8]}.csv")

        df.to_csv(output_path, index=False)
        logger.info(f"Trade log exported: {output_path} ({len(df)} trades)")
        return df

    # ------------------------------------------------------------------
    # 2. Equity Curve
    # ------------------------------------------------------------------
    def generate_equity_curve(
        self, run_id: str, initial_capital: float = 10_000.0
    ) -> pd.DataFrame:
        """Build daily equity progression from trade log."""
        trades = self.export_trade_log(run_id)
        if trades.empty:
            return pd.DataFrame()

        equity = [initial_capital]
        dates = [trades["entry_time"].iloc[0]]

        current_equity = initial_capital
        for _, trade in trades.iterrows():
            current_equity += trade["pnl"]
            equity.append(current_equity)
            dates.append(trade["exit_time"])

        eq_df = pd.DataFrame({"date": dates, "equity": equity})
        eq_df["date"] = pd.to_datetime(eq_df["date"])
        eq_df = eq_df.set_index("date")

        # Calculate drawdown
        running_max = eq_df["equity"].expanding().max()
        eq_df["drawdown_pct"] = (eq_df["equity"] - running_max) / running_max * 100

        output_path = os.path.join(self.output_dir, f"equity_{run_id[:8]}.csv")
        eq_df.to_csv(output_path)
        logger.info(f"Equity curve exported: {output_path}")
        return eq_df

    # ------------------------------------------------------------------
    # 3. Drawdown Analysis
    # ------------------------------------------------------------------
    def analyze_drawdowns(self, run_id: str) -> pd.DataFrame:
        """Identify and rank drawdown periods."""
        eq_df = self.generate_equity_curve(run_id)
        if eq_df.empty:
            return pd.DataFrame()

        # Find drawdown periods
        running_max = eq_df["equity"].expanding().max()
        in_drawdown = eq_df["equity"] < running_max

        drawdown_periods = []
        start = None
        for i, (idx, row) in enumerate(eq_df.iterrows()):
            if in_drawdown.iloc[i] and start is None:
                start = idx
            elif not in_drawdown.iloc[i] and start is not None:
                dd_slice = eq_df.loc[start:idx]
                max_dd = dd_slice["drawdown_pct"].min()
                duration = (idx - start).days if hasattr(idx - start, "days") else 0
                drawdown_periods.append({
                    "start": start,
                    "end": idx,
                    "duration_days": duration,
                    "max_drawdown_pct": max_dd,
                    "recovery_equity": row["equity"],
                })
                start = None

        # Handle ongoing drawdown
        if start is not None:
            dd_slice = eq_df.loc[start:]
            max_dd = dd_slice["drawdown_pct"].min()
            end = eq_df.index[-1]
            duration = (end - start).days if hasattr(end - start, "days") else 0
            drawdown_periods.append({
                "start": start,
                "end": end,
                "duration_days": duration,
                "max_drawdown_pct": max_dd,
                "recovery_equity": None,  # still in drawdown
            })

        dd_df = pd.DataFrame(drawdown_periods)
        if not dd_df.empty:
            dd_df = dd_df.sort_values("max_drawdown_pct")

        output_path = os.path.join(self.output_dir, f"drawdowns_{run_id[:8]}.csv")
        dd_df.to_csv(output_path, index=False)
        logger.info(f"Drawdown analysis exported: {output_path}")
        return dd_df

    # ------------------------------------------------------------------
    # 4. Rolling Metrics
    # ------------------------------------------------------------------
    def compute_rolling_metrics(
        self, run_id: str, windows: List[int] = None
    ) -> pd.DataFrame:
        """Compute rolling Sharpe, return over specified windows."""
        if windows is None:
            windows = [30, 60, 90]

        trades = self.export_trade_log(run_id)
        if trades.empty:
            return pd.DataFrame()

        trades["exit_time"] = pd.to_datetime(trades["exit_time"])
        trades = trades.set_index("exit_time").sort_index()

        results = trades[["pnl_percent"]].copy()

        for w in windows:
            results[f"rolling_{w}d_return"] = (
                results["pnl_percent"].rolling(f"{w}D").sum()
            )
            results[f"rolling_{w}d_sharpe"] = (
                results["pnl_percent"].rolling(f"{w}D").mean()
                / results["pnl_percent"].rolling(f"{w}D").std()
                * np.sqrt(252)  # annualize
            )
            results[f"rolling_{w}d_trades"] = (
                results["pnl_percent"].rolling(f"{w}D").count()
            )

        output_path = os.path.join(self.output_dir, f"rolling_{run_id[:8]}.csv")
        results.to_csv(output_path)
        logger.info(f"Rolling metrics exported: {output_path}")
        return results

    # ------------------------------------------------------------------
    # 5. Backtest Certification Report
    # ------------------------------------------------------------------
    def generate_certification(self, run_id: str) -> str:
        """
        Generate a human-readable certification report.
        This MUST be reviewed and signed off before live deployment.
        """
        query = text("""
            SELECT * FROM backtest_runs WHERE run_id = :run_id
        """)

        with self.engine.connect() as conn:
            run = pd.read_sql(query, conn, params={"run_id": run_id})

        if run.empty:
            return f"ERROR: No backtest run found for {run_id}"

        r = run.iloc[0]
        trades = self.export_trade_log(run_id)
        dd_analysis = self.analyze_drawdowns(run_id)

        # Build report
        report = f"""
{'='*70}
BACKTEST CERTIFICATION REPORT
{'='*70}
Run ID:          {r['run_id']}
Generated:       {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S UTC')}

STRATEGY CONFIGURATION
{'-'*70}
Symbol:          {r['symbol']}
Timeframe:       {r['timeframe']}
Strategy:        {r['strategy_name']}
Parameters:      {r['strategy_params']}
Period:          {r['backtest_start']} to {r['backtest_end']}
Initial Capital: ${r.get('initial_capital', 'N/A')}
Slippage:        {r.get('slippage_pct', 'N/A')} bps
Commission:      {r.get('commission_pct', 'N/A')} bps

PERFORMANCE METRICS
{'-'*70}
Total Return:       {r['total_return']:.2f}%
Annualized Return:  {r.get('annualized_return', 'N/A')}%
Max Drawdown:       {r['max_drawdown']:.2f}%
Sharpe Ratio:       {r['sharpe_ratio']:.4f}
Sortino Ratio:      {r.get('sortino_ratio', 'N/A')}
Profit Factor:      {r.get('profit_factor', 'N/A')}
Win Rate:           {r['win_rate']:.2%}
Total Trades:       {r['num_trades']}
Winning Trades:     {r.get('num_winning_trades', 'N/A')}

VALIDATION STATUS
{'-'*70}
Status:          {r['validation_status']}
Notes:           {r.get('validation_notes', 'None')}

DRAWDOWN ANALYSIS
{'-'*70}
"""
        if not dd_analysis.empty:
            report += f"Longest drawdown: {dd_analysis['duration_days'].max()} days\n"
            report += f"Deepest drawdown: {dd_analysis['max_drawdown_pct'].min():.2f}%\n"
            report += f"Number of drawdown periods: {len(dd_analysis)}\n"
        else:
            report += "No drawdown periods identified.\n"

        report += f"""
PRE-DEPLOYMENT CHECKLIST
{'-'*70}
[ ] Backtest passes all validation criteria
[ ] Walk-forward optimization shows consistent OOS performance
[ ] Sharpe ratio > 1.0 on both IS and OOS
[ ] Max drawdown < 20% (ideally < 15%)
[ ] Trade count > 50 (sufficient data for statistics)
[ ] Profit factor > 1.2
[ ] Database integrity verified (checksums match, no gaps)
[ ] Code reviewed by another developer
[ ] Risk limits configured (max position, max DD stop, realistic costs)
[ ] Live paper trading on Binance testnet for 2 weeks minimum
[ ] Operational runbook written

APPROVAL
{'-'*70}
Approved for live:  {'YES' if r.get('approved_for_live') else 'NO'}
Approved by:        {r.get('approved_by', '___________________')}
Approved at:        {r.get('approved_at', '___________________')}
Signature:          ___________________

{'='*70}
THIS REPORT MUST BE REVIEWED AND SIGNED BEFORE LIVE DEPLOYMENT
{'='*70}
"""

        output_path = os.path.join(self.output_dir, f"certification_{run_id[:8]}.txt")
        with open(output_path, "w") as f:
            f.write(report)

        logger.info(f"Certification report: {output_path}")
        return report

    # ------------------------------------------------------------------
    # 6. HTML Summary Report
    # ------------------------------------------------------------------
    def generate_html_report(self, run_id: str) -> str:
        """Generate a standalone HTML summary with embedded charts."""
        query = text("SELECT * FROM backtest_runs WHERE run_id = :run_id")

        with self.engine.connect() as conn:
            run = pd.read_sql(query, conn, params={"run_id": run_id})

        if run.empty:
            return ""

        r = run.iloc[0]
        trades = self.export_trade_log(run_id)
        eq_df = self.generate_equity_curve(run_id)

        # Prepare chart data
        equity_dates = []
        equity_values = []
        drawdown_values = []
        if not eq_df.empty:
            equity_dates = [str(d) for d in eq_df.index.tolist()]
            equity_values = eq_df["equity"].tolist()
            drawdown_values = eq_df["drawdown_pct"].tolist()

        trade_pnls = trades["pnl_percent"].tolist() if not trades.empty else []
        cumulative_pnls = trades["cumulative_return_pct"].tolist() if not trades.empty else []

        html = f"""<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <title>Backtest Report - {r['run_id'][:8]}</title>
    <script src="https://cdnjs.cloudflare.com/ajax/libs/Chart.js/4.4.1/chart.umd.js"></script>
    <style>
        body {{ font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif;
               max-width: 1200px; margin: 0 auto; padding: 20px; background: #0d1117; color: #c9d1d9; }}
        h1 {{ color: #58a6ff; border-bottom: 1px solid #30363d; padding-bottom: 10px; }}
        h2 {{ color: #79c0ff; margin-top: 30px; }}
        .metrics {{ display: grid; grid-template-columns: repeat(4, 1fr); gap: 15px; margin: 20px 0; }}
        .metric {{ background: #161b22; border: 1px solid #30363d; border-radius: 8px;
                   padding: 15px; text-align: center; }}
        .metric-value {{ font-size: 24px; font-weight: bold; color: #58a6ff; }}
        .metric-label {{ font-size: 12px; color: #8b949e; margin-top: 5px; }}
        .positive {{ color: #3fb950; }}
        .negative {{ color: #f85149; }}
        .chart-container {{ background: #161b22; border: 1px solid #30363d; border-radius: 8px;
                           padding: 20px; margin: 20px 0; }}
        .status {{ display: inline-block; padding: 4px 12px; border-radius: 12px; font-weight: bold; }}
        .status-PASSED {{ background: #238636; color: white; }}
        .status-FAILED {{ background: #da3633; color: white; }}
        .status-FLAGGED {{ background: #d29922; color: white; }}
        table {{ width: 100%; border-collapse: collapse; margin: 15px 0; }}
        th, td {{ padding: 8px 12px; text-align: left; border-bottom: 1px solid #30363d; }}
        th {{ color: #8b949e; font-weight: 600; }}
    </style>
</head>
<body>
    <h1>Backtest Report</h1>
    <p>Run ID: <code>{r['run_id']}</code> | Generated: {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S UTC')}</p>
    <p>Validation: <span class="status status-{r['validation_status']}">{r['validation_status']}</span></p>

    <h2>Strategy</h2>
    <table>
        <tr><th>Symbol</th><td>{r['symbol']}</td><th>Timeframe</th><td>{r['timeframe']}</td></tr>
        <tr><th>Strategy</th><td>{r['strategy_name']}</td><th>Parameters</th><td>{r['strategy_params']}</td></tr>
        <tr><th>Period</th><td colspan="3">{r['backtest_start']} to {r['backtest_end']}</td></tr>
    </table>

    <h2>Performance</h2>
    <div class="metrics">
        <div class="metric">
            <div class="metric-value {'positive' if r['total_return'] > 0 else 'negative'}">{r['total_return']:.2f}%</div>
            <div class="metric-label">Total Return</div>
        </div>
        <div class="metric">
            <div class="metric-value">{r['sharpe_ratio']:.2f}</div>
            <div class="metric-label">Sharpe Ratio</div>
        </div>
        <div class="metric">
            <div class="metric-value negative">{r['max_drawdown']:.2f}%</div>
            <div class="metric-label">Max Drawdown</div>
        </div>
        <div class="metric">
            <div class="metric-value">{r['num_trades']}</div>
            <div class="metric-label">Total Trades</div>
        </div>
        <div class="metric">
            <div class="metric-value">{r['win_rate']:.1%}</div>
            <div class="metric-label">Win Rate</div>
        </div>
        <div class="metric">
            <div class="metric-value">{r.get('profit_factor', 'N/A')}</div>
            <div class="metric-label">Profit Factor</div>
        </div>
        <div class="metric">
            <div class="metric-value">{r.get('sortino_ratio', 'N/A')}</div>
            <div class="metric-label">Sortino Ratio</div>
        </div>
        <div class="metric">
            <div class="metric-value">{r.get('annualized_return', 'N/A')}%</div>
            <div class="metric-label">Ann. Return</div>
        </div>
    </div>

    <h2>Equity Curve</h2>
    <div class="chart-container">
        <canvas id="equityChart" height="100"></canvas>
    </div>

    <h2>Drawdown</h2>
    <div class="chart-container">
        <canvas id="drawdownChart" height="80"></canvas>
    </div>

    <h2>Trade P&L Distribution</h2>
    <div class="chart-container">
        <canvas id="pnlChart" height="80"></canvas>
    </div>

    <script>
        const equityDates = {json.dumps(equity_dates)};
        const equityValues = {json.dumps(equity_values)};
        const drawdownValues = {json.dumps(drawdown_values)};
        const tradePnls = {json.dumps(trade_pnls)};

        // Equity Chart
        new Chart(document.getElementById('equityChart'), {{
            type: 'line',
            data: {{
                labels: equityDates,
                datasets: [{{ label: 'Equity', data: equityValues,
                    borderColor: '#58a6ff', backgroundColor: 'rgba(88,166,255,0.1)',
                    fill: true, pointRadius: 0, borderWidth: 2 }}]
            }},
            options: {{ responsive: true, scales: {{
                x: {{ display: true, ticks: {{ color: '#8b949e', maxTicksLimit: 10 }} }},
                y: {{ ticks: {{ color: '#8b949e' }} }}
            }} }}
        }});

        // Drawdown Chart
        new Chart(document.getElementById('drawdownChart'), {{
            type: 'line',
            data: {{
                labels: equityDates,
                datasets: [{{ label: 'Drawdown %', data: drawdownValues,
                    borderColor: '#f85149', backgroundColor: 'rgba(248,81,73,0.2)',
                    fill: true, pointRadius: 0, borderWidth: 1 }}]
            }},
            options: {{ responsive: true, scales: {{
                x: {{ display: true, ticks: {{ color: '#8b949e', maxTicksLimit: 10 }} }},
                y: {{ ticks: {{ color: '#8b949e' }} }}
            }} }}
        }});

        // P&L Distribution
        const bins = Array.from({{length: tradePnls.length}}, (_, i) => i + 1);
        new Chart(document.getElementById('pnlChart'), {{
            type: 'bar',
            data: {{
                labels: bins,
                datasets: [{{ label: 'Trade P&L %', data: tradePnls,
                    backgroundColor: tradePnls.map(v => v >= 0 ? '#3fb950' : '#f85149'),
                    borderWidth: 0 }}]
            }},
            options: {{ responsive: true, scales: {{
                x: {{ display: true, title: {{ display: true, text: 'Trade #', color: '#8b949e' }} }},
                y: {{ ticks: {{ color: '#8b949e' }} }}
            }} }}
        }});
    </script>

    <h2>Validation Notes</h2>
    <pre>{r.get('validation_notes', 'None')}</pre>

    <hr>
    <p style="color: #8b949e; font-size: 12px;">
        Generated by Production Backtesting System.
        Human sign-off required before live deployment.
    </p>
</body>
</html>"""

        output_path = os.path.join(self.output_dir, f"report_{run_id[:8]}.html")
        with open(output_path, "w") as f:
            f.write(html)

        logger.info(f"HTML report: {output_path}")
        return output_path

    # ------------------------------------------------------------------
    # Generate All Reports
    # ------------------------------------------------------------------
    def generate_all(self, run_id: str) -> Dict[str, str]:
        """Generate all reports for a backtest run."""
        logger.info(f"Generating all reports for run {run_id[:8]}")
        paths = {}

        try:
            self.export_trade_log(run_id)
            paths["trade_log"] = os.path.join(self.output_dir, f"trades_{run_id[:8]}.csv")
        except Exception as e:
            logger.error(f"Trade log failed: {e}")

        try:
            self.generate_equity_curve(run_id)
            paths["equity_curve"] = os.path.join(self.output_dir, f"equity_{run_id[:8]}.csv")
        except Exception as e:
            logger.error(f"Equity curve failed: {e}")

        try:
            self.analyze_drawdowns(run_id)
            paths["drawdowns"] = os.path.join(self.output_dir, f"drawdowns_{run_id[:8]}.csv")
        except Exception as e:
            logger.error(f"Drawdown analysis failed: {e}")

        try:
            self.compute_rolling_metrics(run_id)
            paths["rolling_metrics"] = os.path.join(self.output_dir, f"rolling_{run_id[:8]}.csv")
        except Exception as e:
            logger.error(f"Rolling metrics failed: {e}")

        try:
            self.generate_certification(run_id)
            paths["certification"] = os.path.join(self.output_dir, f"certification_{run_id[:8]}.txt")
        except Exception as e:
            logger.error(f"Certification failed: {e}")

        try:
            self.generate_html_report(run_id)
            paths["html_report"] = os.path.join(self.output_dir, f"report_{run_id[:8]}.html")
        except Exception as e:
            logger.error(f"HTML report failed: {e}")

        logger.info(f"All reports generated: {list(paths.keys())}")
        return paths


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------
def main():
    import argparse

    parser = argparse.ArgumentParser(description="Generate backtest reports")
    parser.add_argument("run_id", help="Backtest run ID (UUID)")
    parser.add_argument("--output-dir", default="reports")
    parser.add_argument("--report", choices=["all", "trades", "equity", "drawdown", "rolling", "cert", "html"],
                        default="all")
    args = parser.parse_args()

    gen = ReportGenerator(output_dir=args.output_dir)

    if args.report == "all":
        paths = gen.generate_all(args.run_id)
        print(f"Generated reports: {paths}")
    elif args.report == "trades":
        gen.export_trade_log(args.run_id)
    elif args.report == "equity":
        gen.generate_equity_curve(args.run_id)
    elif args.report == "drawdown":
        gen.analyze_drawdowns(args.run_id)
    elif args.report == "rolling":
        gen.compute_rolling_metrics(args.run_id)
    elif args.report == "cert":
        report = gen.generate_certification(args.run_id)
        print(report)
    elif args.report == "html":
        path = gen.generate_html_report(args.run_id)
        print(f"HTML report: {path}")


if __name__ == "__main__":
    main()
