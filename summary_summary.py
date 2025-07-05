import os
import pandas as pd

# === CONFIGURATION ===
SUMMARY_FOLDER = r"C:\Users\Axxela\Desktop\atm strategy final\matrix_summaries"
FINAL_OUTPUT = r"C:\Users\Axxela\Desktop\atm strategy final\combined_matrix_summary.xlsx"

# Containers
total_pnl_map = {}
minute_pnl_sum = {}
cumulative_stats = {}

# === Process each summary file ===
for file in os.listdir(SUMMARY_FOLDER):
    if file.endswith("_summary.xlsx"):
        filepath = os.path.join(SUMMARY_FOLDER, file)
        strategy_name = file.replace("_summary.xlsx", "")
        df = pd.read_excel(filepath)

        for _, row in df.iterrows():
            date = pd.to_datetime(row['date']).date()

            # 1. Total PnL per date
            if date not in total_pnl_map:
                total_pnl_map[date] = {}
            total_pnl_map[date][strategy_name] = row['total_pnl']

            # 2. Sum minute PnL across all dates for this strategy
            if strategy_name not in minute_pnl_sum:
                minute_pnl_sum[strategy_name] = {}
            for col in df.columns:
                if str(col).startswith("min_"):
                    minute_pnl_sum[strategy_name][col] = minute_pnl_sum[strategy_name].get(col, 0.0) + row[col]

            # 3. Cumulative stats
            if strategy_name not in cumulative_stats:
                cumulative_stats[strategy_name] = {
                    "total_trades": 0,
                    "total_stoploss_hit": 0,
                    "total_target_hit": 0,
                    "total_forced_close": 0,
                }
            cumulative_stats[strategy_name]["total_trades"] += row['total_trades']
            cumulative_stats[strategy_name]["total_stoploss_hit"] += row['total_stoploss_hit']
            cumulative_stats[strategy_name]["total_target_hit"] += row['total_target_hit']
            cumulative_stats[strategy_name]["total_forced_close"] += row['total_forced_close']

# === Write to final Excel ===
with pd.ExcelWriter(FINAL_OUTPUT) as writer:
    # Sheet 1: Total PnL Comparison
    total_pnl_df = pd.DataFrame.from_dict(total_pnl_map, orient='index').reset_index()
    total_pnl_df.rename(columns={'index': 'date'}, inplace=True)
    total_pnl_df.sort_values('date', inplace=True)
    total_pnl_df.to_excel(writer, sheet_name="total_pnl_comparison", index=False)

    # Sheet 2: Minute-wise summed PnL
    minute_df = pd.DataFrame.from_dict(minute_pnl_sum, orient='index').reset_index()
    minute_df.rename(columns={'index': 'strategy'}, inplace=True)
    minute_df.to_excel(writer, sheet_name="minute_pnl_summary", index=False)

    # Sheet 3: Cumulative Trade Stats
    cumulative_df = pd.DataFrame.from_dict(cumulative_stats, orient='index').reset_index()
    cumulative_df.rename(columns={'index': 'strategy'}, inplace=True)
    cumulative_df.to_excel(writer, sheet_name="cumulative_trade_stats", index=False)

print(f"✅ Final summary saved to: {FINAL_OUTPUT}")
