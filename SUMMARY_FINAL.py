import os
import pandas as pd
from datetime import datetime, timedelta
import re

# === CONFIGURATION ===
CSV_FOLDER = r"C:\Users\Axxela\Desktop\sl55"
OUTPUT_CSV = r"C:\Users\Axxela\Desktop\summary_3year\summary_atm_sl55tp50.xlsx"

summary_data = []

for file in os.listdir(CSV_FOLDER):
    if file.endswith(".xlsx"):
        match = re.search(r'\d{4}-\d{2}-\d{2}', file)
        if match:
            date_str = match.group()
            file_date = datetime.strptime(date_str, "%Y-%m-%d").date()
        else:
            print(f"⚠️ Skipping file due to unmatched pattern: {file}")
            continue

        filepath = os.path.join(CSV_FOLDER, file)
        df = pd.read_excel(filepath)
        print(file)
        s_value=(df['straddle_value'].sum()/len(df))
        day_start_close_value = (df['13:30_close_value'].sum()/len(df))
        straddle_cb=(df['c_bid'].sum()/len(df))
        straddle_ca=(df['c_ask'].sum()/len(df))
        straddle_pb=(df['p_bid'].sum()/len(df))
        straddle_pa=(df['p_ask'].sum()/len(df))
        
        toal_trade_at_sl=(df[])
        # Parse datetime safely
        df['entry_time'] = pd.to_datetime(df['entry_time'], dayfirst=True, errors='coerce')
        df['exit_time'] = pd.to_datetime(df['exit_time'], dayfirst=True, errors='coerce')
        df.dropna(subset=['entry_time', 'exit_time'], inplace=True)

        # Calculate PnL
        df['pnl'] = df['entry_bid'] - df['exit_bid']

        # Basic Stats
        total_pnl = df['pnl'].sum()
        total_trades = len(df)
        total_stoploss_hit = (df['exit_reason'] == 'STOP LOSS HIT').sum()
        total_target_hit = (df['exit_reason'] == 'TARGET HIT').sum()
        total_forced_close = (df['exit_reason'] == 'FORCED CLOSE - SESSION END').sum()

        df['entry_minute'] = df['entry_time'].dt.strftime('%H:%M')
        minute_pnl_group = df.groupby('entry_minute')['pnl'].sum()
        optimal_minute = minute_pnl_group.idxmax() if not minute_pnl_group.empty else ''
        optimal_minute_pnl = minute_pnl_group.max() if not minute_pnl_group.empty else 0.0

        # Cumulative 30-min intervals (from first to last entry)
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

        # Compose summary row
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
            'straddle_cb':straddle_cb,
            'straddle_ca':straddle_ca,
            'straddle_pb':straddle_pb,
            'straddle_pa':straddle_pa,
        }

        summary_row.update(interval_pnls)
        summary_data.append(summary_row)

# Final summary dataframe
summary_df = pd.DataFrame(summary_data)
summary_df.sort_values(by='date', inplace=True)
summary_df.to_csv(OUTPUT_CSV, index=False)
summary_df.to_excel(OUTPUT_CSV, index=False)

print(f"✅ Summary with all 30-min PnLs saved to: {OUTPUT_CSV}")
