import mysql.connector
import csv
import os
import re
from datetime import datetime
import multiprocessing

# ---- Configuration ----
DB_CONFIG = {
    "host": "localhost",
    "user": "root",
    "password": "Axxela@123",
    "database": "cbbo_sec_data"
}

CSV_FOLDER_PATH = r"C:\Users\Axxela\Downloads\opra-pillar-20250627"  # Update this
BATCH_SIZE = 200000  # Reduce from 1,00,000 to prevent MySQL disconnects

# ---- Helpers ----
def parse_mysql_datetime(iso_ts):
    if not iso_ts.strip():
        return None
    try:
        clean_ts = iso_ts.rstrip('Z')
        if '.' in clean_ts:
            main, frac = clean_ts.split('.')
            frac = frac[:6].ljust(6, '0')
            clean_ts = f"{main}.{frac}"
        return datetime.fromisoformat(clean_ts)
    except Exception as e:
        print(f"⚠️ Date parse error: {e} for value: {iso_ts}")
        return None

def to_int_or_none(val):
    try:
        return int(float(val)) if val.strip() else None
    except:
        return None

def to_float_or_none(val):
    try:
        return float(val) if val.strip() else None
    except:
        return None

def extract_date_from_filename(filename):
    match = re.search(r"(\d{8})", filename)
    return match.group(1) if match else None

# ---- Create Table per File ----
def create_table_for_date(date_str, cursor):
    table_name = f"cbbo_{date_str}"
    cursor.execute(f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            ts_recv DATETIME(6),
            ts_event DATETIME(6),
            rtype INT,
            publisher_id INT,
            instrument_id BIGINT,
            side CHAR(1),
            price DOUBLE,
            size INT,
            flags INT,
            bid_px_00 DOUBLE,
            ask_px_00 DOUBLE,
            bid_sz_00 INT,
            ask_sz_00 INT,
            bid_pb_00 DOUBLE,
            ask_pb_00 DOUBLE,
            symbol VARCHAR(40)
        )
    """)
    return table_name

# ---- Load File Into Table ----
def load_csv_to_table(csv_path, conn):
    filename = os.path.basename(csv_path)
    date_str = extract_date_from_filename(filename)
    if not date_str:
        print(f"⚠️ Skipping file with no valid date: {filename}")
        return

    cursor = conn.cursor()
    table_name = create_table_for_date(date_str, cursor)

    insert_query = f"""
        INSERT INTO {table_name} (
            ts_recv, ts_event, rtype, publisher_id, instrument_id, side,
            price, size, flags,
            bid_px_00, ask_px_00, bid_sz_00, ask_sz_00,
            bid_pb_00, ask_pb_00, symbol
        ) VALUES (
            %s, %s, %s, %s, %s, %s,
            %s, %s, %s,
            %s, %s, %s, %s,
            %s, %s, %s
        )
    """

    batch = []
    with open(csv_path, "r", newline='', encoding="utf-8") as file:
        reader = csv.DictReader(file)
        for idx, row in enumerate(reader, 1):
            try:
                batch.append((
                    parse_mysql_datetime(row["ts_recv"]),
                    parse_mysql_datetime(row["ts_event"]),
                    to_int_or_none(row["rtype"]),
                    to_int_or_none(row["publisher_id"]),
                    to_int_or_none(row["instrument_id"]),
                    row["side"].strip() if row["side"].strip() else None,
                    to_float_or_none(row["price"]),
                    to_int_or_none(row["size"]),
                    to_int_or_none(row["flags"]),
                    to_float_or_none(row["bid_px_00"]),
                    to_float_or_none(row["ask_px_00"]),
                    to_int_or_none(row["bid_sz_00"]),
                    to_int_or_none(row["ask_sz_00"]),
                    to_float_or_none(row["bid_pb_00"]),
                    to_float_or_none(row["ask_pb_00"]),
                    row["symbol"].strip() if row["symbol"].strip() else None
                ))
            except Exception as e:
                print(f"⚠️ Row {idx} skipped in {filename}: {e}")
                continue

            if len(batch) >= BATCH_SIZE:
                cursor.executemany(insert_query, batch)
                conn.commit()
                print(f"📦 Inserted {len(batch)} rows into {table_name}")
                batch.clear()

        if batch:
            cursor.executemany(insert_query, batch)
            conn.commit()
            print(f"📦 Inserted final {len(batch)} rows into {table_name}")

    cursor.close()

# ---- Worker for Multiprocessing ----
def parallel_worker(csv_file):
    from mysql.connector import connect
    full_path = os.path.join(CSV_FOLDER_PATH, csv_file)
    try:
        conn = connect(**DB_CONFIG)
        print(f"➡️ Processing {csv_file} in PID {os.getpid()}")
        load_csv_to_table(full_path, conn)
        conn.close()
    except Exception as e:
        print(f"❌ Failed to process {csv_file}: {e}")

# ---- Main Function ----
def run_pipeline():
    all_files = sorted(f for f in os.listdir(CSV_FOLDER_PATH) if f.endswith(".csv"))
    print(f"🔍 Found {len(all_files)} CSV files...\n")

    num_processes = min(2, multiprocessing.cpu_count())  # Use 4 or fewer processes
    with multiprocessing.Pool(processes=num_processes) as pool:
        pool.map(parallel_worker, all_files)

    print("\n✅ All files imported using multiprocessing.")

# ---- Entry Point ----
if __name__ == "__main__":
    multiprocessing.freeze_support()  # Required for Windows
    run_pipeline()
