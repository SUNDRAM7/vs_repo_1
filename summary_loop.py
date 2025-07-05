import os
import pandas as pd
from datetime import datetime, timedelta
import re

# === CONFIGURATION ===
MATRIX_ROOT = r"C:\Users\Axxela\Desktop\atm strategy final\matrix"
SUMMARY_OUTPUT_FOLDER = r"C:\Users\Axxela\Desktop\atm strategy final\matrix_summaries"
os.makedirs(SUMMARY_OUTPUT_FOLDER, exist_ok=True)

# === Walk through each tXX folder ===
for t_folder in os.listdir(MATRIX_ROOT):
    t_path = os.path.join(MATRIX_ROOT, t_folder)
    if not os.path.isdir(t_path):
        continue

    for sl_folder in os.listdir(t_path):
        sl_path = os.path.join(t_path, sl_folder)
        if not os.path.isdir(sl_path):
            continue

        summary_data = []
        for file in os.listdir(sl_path):
            if file.endswith(".xlsx"):
                match = re.search(r'\d{4}-\d{2}-\d{2}', file)
                if match:
                    date_str = match.group()
                    file_date = datetime.strptime(date_str, "%Y-%m-%d").date()
                else:
                    print(f"⚠️ Skipping file due to unmatched pattern: {file}")
                    continue

                filepath = os.path.join(sl_path, file)
                try:
                    df = pd.read_excel(filepath)

                    s_value = df['straddle_value'].mean()
                    day_start_close_value = df['13:30_close_value'].mean()
                    straddle_cb = df['c_bid'].mean()
                    straddle_ca = df['c_ask'].mean()
                    straddle_pb = df['p_bid'].mean()
                    straddle_pa = df['p_ask'].mean()

                    df['entry_time'] = pd.to_datetime(df['entry_time'], dayfirst=True, errors='coerce')
                    df['exit_time'] = pd.to_datetime(df['exit_time'], dayfirst=True, errors='coerce')
                    df.dropna(subset=['entry_time', 'exit_time'], inplace=True)

                    df['pnl'] = df['entry_bid'] - df['exit_bid']
                    total_pnl = df['pnl'].sum()
                    total_trades = len(df)
                    total_stoploss_hit = (df['exit_reason'] == 'STOP LOSS HIT').sum()
                    total_target_hit = (df['exit_reason'] == 'TARGET HIT').sum()
                    total_forced_close = (df['exit_reason'] == 'FORCED CLOSE - SESSION END').sum()

                    df['entry_minute'] = df['entry_time'].dt.strftime('%H:%M')
                    minute_pnl_group = df.groupby('entry_minute')['pnl'].sum()
                    optimal_minute = minute_pnl_group.idxmax() if not minute_pnl_group.empty else ''
                    optimal_minute_pnl = minute_pnl_group.max() if not minute_pnl_group.empty else 0.0

                    interval_pnls = {}
                    min_time = df['entry_time'].min()
                    max_time = df['entry_time'].max()

                    start_dt = min_time.replace(second=0, microsecond=0)
                    interval_index = 1
                    while start_dt < max_time:
                        end_dt = start_dt + timedelta(minutes=1)
                        label = f"min_{interval_index}"
                        interval_df = df[(df['entry_time'] >= start_dt) & (df['entry_time'] < end_dt)]
                        interval_pnls[label] = round(interval_df['pnl'].sum(), 2)
                        start_dt = end_dt
                        interval_index += 1

                    summary_row = {
                        'date': file_date,
                        'total_pnl': round(total_pnl, 2),
                        'optimal_minute': optimal_minute,
                        'optimal_minute_pnl': round(optimal_minute_pnl, 2),
                        'total_trades': total_trades,
                        'total_stoploss_hit': total_stoploss_hit,
                        'total_target_hit': total_target_hit,
                        'total_forced_close': total_forced_close,
                        'straddle_value': s_value,
                        'day_1st_min_close_value': day_start_close_value,
                        'straddle_cb': straddle_cb,
                        'straddle_ca': straddle_ca,
                        'straddle_pb': straddle_pb,
                        'straddle_pa': straddle_pa,
                    }
                    summary_row.update(interval_pnls)
                    summary_data.append(summary_row)

                except Exception as e:
                    print(f"❌ Error processing file: {filepath}, error: {e}")
                    continue

        # Save summary for this folder
        if summary_data:
            summary_df = pd.DataFrame(summary_data)
            summary_df.sort_values(by='date', inplace=True)
            output_path = os.path.join(SUMMARY_OUTPUT_FOLDER, f"{sl_folder}_summary.xlsx")
            summary_df.to_excel(output_path, index=False)
            print(f"✅ Summary saved: {output_path}")
