import os
import pandas as pd
import mysql.connector
from multiprocessing import Pool

# === Configuration ===
CSV_FOLDER = r"C:\Users\Axxela\Desktop\New folder (2)"  # <-- Change this if needed
MYSQL_CONFIG = {
    'host': 'localhost',
    'user': 'root',
    'password': 'Axxela@123',
    'database': 'ohlc_opt_data'
}

# === New OHLC Schema ===
COLUMN_DEFINITIONS = {
    'ts_event': 'DATETIME(6)',
    'high': 'DOUBLE',
    'rtype': 'INT',
    'low': 'DOUBLE',
    'publisher_id': 'INT',
    'close': 'DOUBLE',
    'instrument_id': 'BIGINT',
    'volume': 'BIGINT',
    'open': 'DOUBLE',
    'symbol': 'VARCHAR(40)'
}
COLUMNS = list(COLUMN_DEFINITIONS.keys())

# === Per-file import function ===
def import_csv_file(file_path):
    try:
        import re
        filename = os.path.basename(file_path)

        # Extract date from filename like: opra-pillar-20250625.ohlcv-1m.csv
        match = re.search(r'(\d{8})\.ohlcv-1m', filename)
        if not match:
            print(f"⚠️ Skipping invalid file name format: {filename}")
            return

        date_str = match.group(1)  # e.g., "20250625"
        table_name = f"ohlc_{date_str[2:]}"  # → ohlc_250625

        # Connect to MySQL
        conn = mysql.connector.connect(**MYSQL_CONFIG)
        cursor = conn.cursor()

        # Read CSV
        df = pd.read_csv(file_path, na_values=["", "null", "NULL"], sep=',')


        # Convert ts_event (ISO 8601 format with nanoseconds) to datetime
        if 'ts_event' in df.columns:
            df['ts_event'] = pd.to_datetime(df['ts_event'], utc=True, errors='coerce')

        # Ensure column order
        df = df[COLUMNS]

        # Drop table if exists
        cursor.execute(f"DROP TABLE IF EXISTS `{table_name}`")

        # Create table
        create_cols = [f"`{col}` {COLUMN_DEFINITIONS[col]}" for col in COLUMNS]
        create_sql = f"CREATE TABLE `{table_name}` ({', '.join(create_cols)})"
        cursor.execute(create_sql)

        # Insert data
        insert_sql = f"INSERT INTO `{table_name}` VALUES ({', '.join(['%s'] * len(COLUMNS))})"
        data = df.where(pd.notnull(df), None).values.tolist()
        cursor.executemany(insert_sql, data)
        conn.commit()

        cursor.close()
        conn.close()
        print(f"✅ Imported: {filename} → {table_name}")
    except Exception as e:
        print(f"❌ Error with {filename}: {e}")


# === Main multiprocessing driver ===
def main():
    # Recursively walk through all subdirectories to find .csv files
    csv_files = []
    for root, _, files in os.walk(CSV_FOLDER):
        for f in files:
            if f.endswith('.csv'):
                full_path = os.path.join(root, f)
                csv_files.append(full_path)

    if not csv_files:
        print("⚠️ No CSV files found.")
        return

    # Use multiprocessing to import each CSV
    with Pool(processes=min(4, len(csv_files))) as pool:
        pool.map(import_csv_file, csv_files)


if __name__ == "__main__":
    main()
