import csv
import pymysql  # Use psycopg2 for PostgreSQL
from tqdm import tqdm  # Optional, for progress bar


# === CONFIGURATION ===
DB_CONFIG = {
    'host': '192.168.102.245',
    'user': 'option_backtest',
    'password': 'Axxela',
    'database': 'sa_exp_bbo_spx',
    'port': 3306  # PostgreSQL default is 5432
}


OUTPUT_CSV = 'table_row_counts_sa_exp_bbo_spx.csv'


# === CONNECT TO DATABASE ===
conn = pymysql.connect(**DB_CONFIG)
cursor = conn.cursor()


# === GET TABLE NAMES ===
cursor.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = %s", (DB_CONFIG['database'],))
tables = [row[0] for row in cursor.fetchall()]


# === COUNT ROWS IN EACH TABLE ===
results = []


print(f"Counting rows in {len(tables)} tables...")


for table in tqdm(tables):
    try:
        cursor.execute(f"SELECT COUNT(*) FROM `{table}`")
        count = cursor.fetchone()[0]
        results.append((table, count))
    except Exception as e:
        print(f"Error counting table {table}: {e}")
        results.append((table, 'ERROR'))


# === WRITE TO CSV ===
with open(OUTPUT_CSV, mode='w', newline='') as file:
    writer = csv.writer(file)
    writer.writerow(['Table Name', 'Row Count'])
    writer.writerows(results)


print(f"\nDone! Row counts saved to {OUTPUT_CSV}")


# === CLEAN UP ===
cursor.close()
conn.close()
