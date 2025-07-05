import os
import pandas as pd
import mysql.connector
from multiprocessing import Pool, cpu_count

# === Configuration ===
CSV_FOLDER = r'C:\Users\Axxela\Desktop\OPRA-20250701-4JRUGBH6X9'  # <-- Change this
MYSQL_CONFIG = {
    'host': 'localhost',
    'user': 'root',
    'password': 'Axxela@123',
    'database': 'newsnp500_bb0_data'
}

# === Fixed schema ===
COLUMN_DEFINITIONS = {
    'ts_recv': 'DATETIME(6)',
    'ts_event': 'DATETIME(6)',
    'rtype': 'INT',
    'publisher_id': 'INT',
    'instrument_id': 'BIGINT',
    'side': 'CHAR(1)',
    'price': 'DOUBLE',
    'size': 'INT',
    'flags': 'INT',
    'bid_px_00': 'DOUBLE',
    'ask_px_00': 'DOUBLE',
    'bid_sz_00': 'INT',
    'ask_sz_00': 'INT',
    'bid_pb_00': 'DOUBLE',
    'ask_pb_00': 'DOUBLE',
    'symbol': 'VARCHAR(40)'
}
COLUMNS = list(COLUMN_DEFINITIONS.keys())

# === Per-file import function ===
def import_csv_file(filename):
    try:
        # Build table name
        date_part = filename.split('_')[-1].replace('.csv', '')
        table_name = f'cbbo_{date_part}'

        # Connect inside subprocess
        conn = mysql.connector.connect(**MYSQL_CONFIG)
        cursor = conn.cursor()

        # Load and process DataFrame
        file_path = os.path.join(CSV_FOLDER, filename)
        df = pd.read_csv(file_path, na_values=["", "null", "NULL"])

        # Convert timestamps
        for col in ['ts_recv', 'ts_event']:
            if col in df.columns:
                df[col] = pd.to_datetime(df[col], errors='coerce')

        # Ensure correct column order
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
    csv_files = [f for f in os.listdir(CSV_FOLDER) if f.endswith('.csv')]
    with Pool(processes=min(2, len(csv_files))) as pool:
        pool.map(import_csv_file, csv_files)

if __name__ == "__main__":
    main()
