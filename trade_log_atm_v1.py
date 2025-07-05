import os
import re
import pandas as pd
import pymysql
from sqlalchemy import create_engine
from datetime import datetime, timedelta
import urllib.parse
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor
import math
import openpyxl

# === CONFIGURATION === #
CSV_FOLDER = r"C:\Users\Axxela\Desktop\atm strategy final\spx_daily"
OUTPUT_FOLDER = r"C:\Users\Axxela\Desktop\atm strategy final\matrix\t50\sl100_t50"
FIXED_TARGET = 0.05
DB_NAME = "sa_exp_bbo_spx"
DB_USER = "root"
DB_PASSWORD_RAW = "Axxela@123"
DB_PASSWORD = urllib.parse.quote(DB_PASSWORD_RAW)
DB_CONN_STRING = f"mysql+pymysql://{DB_USER}:{DB_PASSWORD}@localhost:3306/{DB_NAME}"

# --------- Database 2 ---------
DB2_NAME = "sa_exp_ohlc_spx"
DB2_USER = "root"
DB2_PASSWORD_RAW = "Axxela@123"  # same or different password
DB2_PASSWORD = urllib.parse.quote(DB2_PASSWORD_RAW)
DB2_CONN_STRING = f"mysql+pymysql://{DB2_USER}:{DB2_PASSWORD}@localhost:3306/{DB2_NAME}"

s_call =0 
s_close_price=0
c_b =0
c_a =0
p_b =0
p_a =0
s_put=0
straddle_value=0


engine = create_engine(DB_CONN_STRING)
engine_2 = create_engine(DB2_CONN_STRING)


class OptionTracker:
    def __init__(self):
        self.positions = []
        self.pairs = []
        #self.sl_multiplier = sl_multiplier

    def add_straddle(self, call_strike, put_strike, entry_time, call_bid, put_bid):
        if call_bid is None or put_bid is None or pd.isna(call_bid) or pd.isna(put_bid):
            return

        call_sl = 0.75*(float(min(call_bid * 2, call_bid + put_bid)))#stoploss ke ticks ka 50%
        put_sl =  0.75*(float(min(put_bid * 2, call_bid + put_bid)))

        call_pos = {
            "option_type": "C",
            "strike": call_strike,
            "symbol": generate_symbol("C", call_strike, entry_time),
            "entry_time": entry_time,
            "entry_bid": call_bid,
            "sl": call_sl,
            "target": None,
            "status": "OPEN",
            "exit_time": None,
            "exit_reason": None,
            "exit_ask": None,
            "high": call_high,

        }

        put_pos = {
            "option_type": "P",
            "strike": put_strike,
            "symbol": generate_symbol("P", put_strike, entry_time),
            "entry_time": entry_time,
            "entry_bid": put_bid,
            "sl": put_sl,
            "target": None,
            "status": "OPEN",
            "exit_time": None,
            "exit_reason": None,
            "exit_ask": None,
        }

        self.positions.extend([call_pos, put_pos])
        self.pairs.append((call_pos, put_pos))

    def update_positions(self, current_time, bid_cache):
        for call_pos, put_pos in self.pairs:
            both_open = call_pos["status"] == "OPEN" and put_pos["status"] == "OPEN"

            call_ask = get_cached_price(call_pos["symbol"], current_time, bid_cache, "ask")
            put_ask = get_cached_price(put_pos["symbol"], current_time, bid_cache, "ask")
            call_high = get_cached_price(call_pos["symbol"], current_time, bid_cache, "high_c")
            put_high = get_cached_price(put_pos["symbol"], current_time, bid_cache, "high_p") 
            call_low = get_cached_price(call_pos["symbol"], current_time, bid_cache, "low_c")
            put_low = get_cached_price(put_pos["symbol"], current_time, bid_cache, "low_p")           

            if both_open and call_ask and put_ask:
                total_pnl = (call_pos["entry_bid"] - call_low) + (put_pos["entry_bid"] - put_low)
                avg_entry = (call_pos["entry_bid"] + put_pos["entry_bid"]) *0.5

                if total_pnl >= avg_entry:
                    self._close(call_pos, current_time, call_ask, "TARGET HIT")
                    self._close(put_pos, current_time, put_ask, "TARGET HIT")
                    continue

            for pos in [call_pos, put_pos]:
                if pos["status"] == "CLOSED":
                    continue

                bid = get_cached_price(pos["symbol"], current_time, bid_cache, "bid")
                ask = get_cached_price(pos["symbol"], current_time, bid_cache, "ask")
                print(pos)

                other_pos = put_pos if pos == call_pos else call_pos

                if bid is not None and ask is not None and ask >= pos["sl"]:
                    self._close(pos, current_time, pos["sl"], "STOP LOSS HIT")
                    if other_pos["status"] == "OPEN":
                        other_pos["target"] = FIXED_TARGET
                    continue

                '''if ask is not None and (pos["entry_bid"] - ask) >= 0.5 * pos["entry_bid"]:
                    self._close(pos, current_time, ask, "50% PROFIT HIT")
                    if other_pos["status"] == "OPEN":
                        other_pos["target"] = 0.5 * other_pos["entry_bid"]
                    continue'''

                if pos["target"] is not None and ask is not None and ask <= pos["target"]:
                    self._close(pos, current_time, ask, "TARGET HIT (REVISED)")

                elif pos["target"] == FIXED_TARGET and ask is not None and ask <= FIXED_TARGET:
                    self._close(pos, current_time, ask, "TARGET HIT (POST SL)")

    def _close(self, pos, exit_time, exit_price, reason):
        pos["status"] = "CLOSED"
        pos["exit_time"] = exit_time
        pos["exit_bid"] = exit_price
        pos["exit_reason"] = reason

    def force_close_open_positions(self, final_time, bid_cache):
        for pos in self.positions:
            if pos["status"] == "OPEN":
                ask = get_cached_price(pos["symbol"], final_time, bid_cache, "ask")
                if ask is not None:
                    self._close(pos, final_time, ask, "FORCED CLOSE - SESSION END")

    def get_results_df(self):
        return pd.DataFrame(self.positions)


def generate_symbol(option_type, strike, dt):
    expiry_str = dt.strftime("%y%m%d")
    try:
        strike_str = f"{int(float(strike) * 1000):08d}"
    except (TypeError, ValueError):
        return None
    return f"SPXW  {expiry_str}{option_type}{strike_str}"


def extract_date_from_filename(filename):
    match = re.search(r'\d{4}-\d{2}-\d{2}', filename)
    if match:
        return datetime.strptime(match.group(), "%Y-%m-%d")
    return None


def preload_price_data(date_str, symbols):#working
    table_name = f"sa_exp_{date_str}"
    symbol_list = "','".join(symbols)
    query = f"""
        SELECT symbol, ts_recv, bid_px_00, ask_px_00 FROM {table_name}
        WHERE symbol IN ('{symbol_list}')
        ORDER BY symbol, ts_recv
    """
    try:
        df = pd.read_sql(query, con=engine, parse_dates=["ts_recv"])
        #print(df)
        return df
    except Exception as e:
        print(f"❌ Error loading table sa_exp_{date_str}: {e}")
        return pd.DataFrame()


def build_price_cache(price_df):
    cache = defaultdict(lambda: defaultdict(dict))
    print(7)
    for _, row in price_df.iterrows():
        symbol = row['symbol']
        ts = row['ts_recv'].replace(microsecond=0)
        cache[symbol][ts] = {
            "bid": row['bid_px_00'],
            "ask": row['ask_px_00'],
        }
        #print(cache)
    return cache
def straddle_price_data(date_str, symbols):#working
    table_name = f"sa_exp_{date_str}"
    
    query = f"""
        SELECT symbol, ts_recv, bid_px_00, ask_px_00 FROM {table_name}
        WHERE symbol = '{symbols}' AND bid_px_00 != 0 AND ask_px_00 != 0
        ORDER BY symbol, ts_recv
        
    """
    try:
        df = pd.read_sql(query, con=engine, parse_dates=["ts_recv"])
        #print(df)
        return df
    except Exception as e:
        print(f"❌ Error loading table sa_exp_{date_str}: {e}")
        return pd.DataFrame()



def get_cached_price(symbol, time_obj, cache, price_type="bid"):
    time_key = time_obj.replace(microsecond=0)
    return cache.get(symbol, {}).get(time_key, {}).get(price_type)


def process_file(csv_file_path):

    #tracker = OptionTracker(sl_multiplier=sl_multiplier)
    global straddle_value, s_call, s_put,s_close_price,c_b,c_a,p_a ,p_b
    #print(3)
    filename = os.path.basename(csv_file_path)
    fallback_date = extract_date_from_filename(filename)
    #print(fallback_date)

    # Custom date parser for format: 'DD-MM-YYYY HH:MM'
    try:
        df = pd.read_excel(csv_file_path, parse_dates=['date'], engine='openpyxl')          
        
        #print(csv_file_path)
    except Exception as e:
        print(f"❌ Failed to read {filename}: {e}")
        return pd.DataFrame()

    if df.empty or 'date' not in df.columns:
        print(f"⚠️ No valid date column in {filename}")
        return pd.DataFrame()

    tracker = OptionTracker()
    #print("class initialised")

    if fallback_date:
        sample_date = fallback_date
    else:
        print(f"❌ Could not determine date for {filename}")
        return pd.DataFrame()
    #print((df['close'][0]))
    straddle_date_str = fallback_date.strftime("%y%m%d")
    #straddle_price(df['close'][0])
    s_close_price= df['close'][0]
    straddle_strike = round(s_close_price / 5)*5
    #print(straddle_strike)
    call_straddle_symbol = generate_symbol('C',straddle_strike,fallback_date)
    put_straddle_symbol = generate_symbol('P',straddle_strike,fallback_date)
    #print(call_straddle_symbol,put_straddle_symbol)
    c_ab_p=straddle_price_data(straddle_date_str,call_straddle_symbol)
    p_ab_p=straddle_price_data(straddle_date_str,put_straddle_symbol)
    #print(c_ab_p,p_ab_p)
    c_b = c_ab_p['bid_px_00'][0]
    c_a = c_ab_p['ask_px_00'][0]
    p_b = p_ab_p['bid_px_00'][0]
    p_a = p_ab_p['ask_px_00'][0]
    s_call=(c_b + c_a)/2
    s_put=(p_b + p_a)/2
    straddle_value= s_put+ s_call
    #print(s_call,s_put,straddle_value)




    all_symbols = set()
    df = df.sort_values(by='date')
    #print(df)


    # Shift 'close' column to get previous minute close
    df['prev_close'] = df['close'].shift(1)
    #print(df['prev_close'])
    df = df.dropna(subset=['prev_close']).copy()
    #print(df)

    # Calculate ATM strikes
    df['Call_Price'] = df['prev_close'].apply(lambda x: math.ceil(x / 5) * 5)
    df['Put_Price'] = df['prev_close'].apply(lambda x: math.floor(x / 5) * 5)
    df['Call_Price_5']= df['Call_Price']+ 5
    df['Put_Price_5']= df['Put_Price'] - 5
    df['Call_Price_10']= df['Call_Price'] + 10
    df['Put_Price_10']= df['Put_Price'] - 10
    df['Call_Price_15']= df['Call_Price'] + 15
    df['Put_Price_15']= df['Put_Price'] - 15
    #print(df)
    #print(df['Call_Price'])
    #print(df)
    for _, row in df.iterrows():
        if pd.isna(row['Call_Price']) or pd.isna(row['Put_Price']) or pd.isna(row['date']):
            continue
        all_symbols.add(generate_symbol("C", row['Call_Price'], row['date']))
        all_symbols.add(generate_symbol("P", row['Put_Price'], row['date']))
        all_symbols.add(generate_symbol("C", row['Call_Price_5'], row['date']))
        all_symbols.add(generate_symbol("P", row['Put_Price_5'], row['date']))
        all_symbols.add(generate_symbol("C", row['Call_Price_10'], row['date']))
        all_symbols.add(generate_symbol("P", row['Put_Price_10'], row['date']))
        all_symbols.add(generate_symbol("C", row['Call_Price_15'], row['date']))

        all_symbols.add(generate_symbol("P", row['Put_Price_15'], row['date']))

    date_str = sample_date.strftime("%y%m%d")
    print(f"📅 Using table: sa_exp_{date_str} for {filename}")
    #print(5)
    price_df = preload_price_data(date_str, all_symbols)
    #print(6)

    if price_df.empty:
        print(f"⚠️ Skipping {filename}: No data found for table sa_exp_{date_str}")
        return pd.DataFrame()

    price_cache = build_price_cache(price_df)


    for _, row in df.iterrows():
        time_now = row['date']
        call_strike = row['Call_Price']
        put_strike = row['Put_Price']
        call_strike_5 = row['Call_Price_5']
        put_strike_5 = row['Put_Price_5']
        call_strike_10 = row['Call_Price_10']
        put_strike_10 = row['Put_Price_10']
        call_strike_15 = row['Call_Price_15']
        put_strike_15 = row['Put_Price_15']        




        if pd.isna(call_strike) or pd.isna(put_strike) or pd.isna(time_now):
            continue

        call_symbol = generate_symbol("C", call_strike, time_now)
        put_symbol = generate_symbol("P", put_strike, time_now)

        call_bid = get_cached_price(call_symbol, time_now, price_cache, "bid")
        put_bid = get_cached_price(put_symbol, time_now, price_cache, "bid")

        if (call_bid and call_bid > FIXED_TARGET) and (put_bid and put_bid > FIXED_TARGET):
            tracker.add_straddle(call_strike, put_strike, time_now, call_bid, put_bid)

        tracker.update_positions(time_now, price_cache)

    final_time = df['date'].max()
    tracker.force_close_open_positions(final_time, price_cache)

    return tracker.get_results_df()

    
def run_batch():
    os.makedirs(OUTPUT_FOLDER, exist_ok=True)
    print(1)
    files = [f for f in os.listdir(CSV_FOLDER) if f.endswith(".xlsx")]
    print(2)


    def process_and_save(file):
        print(3)
        csv_path = os.path.join(CSV_FOLDER, file)
        print(f"🔁 Processing {file}...")
        result_df = process_file(csv_path)
        #print(result_df)
        result_df['straddle_value']=straddle_value
        result_df['13:30_close_value']=s_close_price
        result_df['c_bid']=c_b
        result_df['c_ask']=c_a
        result_df['p_bid']=p_b
        result_df['p_ask']=p_a

        output_path = os.path.join(OUTPUT_FOLDER, file.replace(".xlsx", "_trades.xlsx"))
        if not result_df.empty:
            result_df.to_excel(output_path, index=False)
            print(f"✅ Saved: {output_path}")
        else:
            print(f"⚠️ No trades found for {file}")

    with ThreadPoolExecutor(max_workers=1) as executor:
        executor.map(process_and_save, files)


if __name__ == "__main__":
    run_batch()
    