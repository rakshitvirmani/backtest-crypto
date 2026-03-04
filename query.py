#!/usr/bin/env python3
"""
query.py - Interactive SQL runner for the crypto backtesting database
=====================================================================
Edit the QUERIES list below and run:  python query.py
Each query prints results as a formatted table (pandas DataFrame).
"""

from db import get_connection

# ─────────────────────────────────────────────────────────────────────
# EDIT YOUR QUERIES HERE
# Each entry is a (label, sql) tuple. Add, remove, or modify as needed.
# ─────────────────────────────────────────────────────────────────────
QUERIES = [

    # ── Overview ──────────────────────────────────────────────────
    ("All tables in the database",
     "SHOW TABLES"),

    ("Row counts per table",
     """
     SELECT 'klines' AS table_name, COUNT(*) AS rows FROM klines
     UNION ALL
     SELECT 'backtest_runs', COUNT(*) FROM backtest_runs
     UNION ALL
     SELECT 'trade_log', COUNT(*) FROM trade_log
     UNION ALL
     SELECT 'fetch_log', COUNT(*) FROM fetch_log
     UNION ALL
     SELECT 'equity_curve', COUNT(*) FROM equity_curve
     ORDER BY rows DESC
     """),

    # ── Klines (OHLCV) ───────────────────────────────────────────
    ("Symbols & timeframes available",
     """
     SELECT symbol, timeframe, COUNT(*) AS candles,
            MIN(open_time) AS earliest_ts, MAX(open_time) AS latest_ts
     FROM klines
     GROUP BY symbol, timeframe
     ORDER BY symbol, timeframe
     """),

    ("Latest 10 candles (BTCUSDT 1d)",
     """
     SELECT open_time, open, high, low, close, volume
     FROM klines
     WHERE symbol = 'BTCUSDT' AND timeframe = '1d'
     ORDER BY open_time DESC
     LIMIT 10
     """),

    ("Daily volume stats (BTCUSDT)",
     """
     SELECT timeframe,
            ROUND(AVG(volume), 2)  AS avg_volume,
            ROUND(MAX(volume), 2)  AS max_volume,
            ROUND(MIN(volume), 2)  AS min_volume
     FROM klines
     WHERE symbol = 'BTCUSDT'
     GROUP BY timeframe
     ORDER BY timeframe
     """),

    # ── Backtest Runs ─────────────────────────────────────────────
    ("All backtest runs summary",
     """
     SELECT run_id, strategy_name, symbol, timeframe,
            ROUND(total_return, 2) AS return_pct,
            ROUND(sharpe_ratio, 2) AS sharpe,
            ROUND(max_drawdown, 2) AS max_dd,
            num_trades, ROUND(win_rate, 2) AS win_rate,
            validation_status
     FROM backtest_runs
     ORDER BY created_at DESC
     LIMIT 20
     """),

    ("Best strategies by Sharpe ratio",
     """
     SELECT strategy_name, symbol, timeframe,
            ROUND(sharpe_ratio, 2)     AS sharpe,
            ROUND(total_return, 2)     AS return_pct,
            ROUND(max_drawdown, 2)     AS max_dd,
            ROUND(profit_factor, 2)    AS profit_factor,
            num_trades
     FROM backtest_runs
     WHERE validation_status = 'PASSED'
     ORDER BY sharpe_ratio DESC
     LIMIT 10
     """),

    # ── Trade Log ─────────────────────────────────────────────────
    ("Recent trades (last 20)",
     """
     SELECT t.trade_number, t.direction, t.entry_time, t.exit_time,
            ROUND(t.entry_price, 2) AS entry, ROUND(t.exit_price, 2) AS exit,
            ROUND(t.pnl_percent, 2) AS pnl_pct, t.is_winning_trade,
            t.entry_signal, t.exit_signal
     FROM trade_log t
     ORDER BY t.id DESC
     LIMIT 20
     """),

    ("Win/loss breakdown per strategy",
     """
     SELECT b.strategy_name,
            COUNT(*)                                    AS total_trades,
            SUM(CASE WHEN t.is_winning_trade THEN 1 ELSE 0 END) AS wins,
            SUM(CASE WHEN NOT t.is_winning_trade THEN 1 ELSE 0 END) AS losses,
            ROUND(AVG(t.pnl_percent), 2)               AS avg_pnl_pct,
            ROUND(MAX(t.pnl_percent), 2)               AS best_trade,
            ROUND(MIN(t.pnl_percent), 2)               AS worst_trade
     FROM trade_log t
     JOIN backtest_runs b ON t.backtest_run_id = b.id
     GROUP BY b.strategy_name
     ORDER BY avg_pnl_pct DESC
     """),

    # ── Fetch Log ─────────────────────────────────────────────────
    ("Recent data fetches",
     """
     SELECT symbol, timeframe, fetch_start, fetch_end,
            http_status, response_time_ms, records_fetched, records_upserted
     FROM fetch_log
     ORDER BY created_at DESC
     LIMIT 10
     """),

    # ── Custom: add your own below ────────────────────────────────
    # ("My custom query",
    #  "SELECT ... FROM ... WHERE ..."),

]


def main():
    conn = get_connection()
    try:
        for label, sql in QUERIES:
            print(f"\n{'='*70}")
            print(f"  {label}")
            print(f"{'='*70}")
            try:
                df = conn.execute(sql.strip()).fetchdf()
                if df.empty:
                    print("  (no rows)")
                else:
                    print(df.to_string(index=False))
            except Exception as e:
                print(f"  ERROR: {e}")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
