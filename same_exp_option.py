import mysql.connector
import re

# --- Source and destination databases ---
source_db = "snp500_bbo_data"
dest_db = "sa_exp_bbo_spx"

# --- Connect to MySQL ---
conn = mysql.connector.connect(
    host="localhost",
    user="root",
    password="Axxela@123"
)
cursor = conn.cursor()

# --- Ensure destination DB exists ---
cursor.execute(f"CREATE DATABASE IF NOT EXISTS {dest_db}")

# --- Fetch all tables from source DB ---
cursor.execute(f"USE {source_db}")
cursor.execute("SHOW TABLES")
tables = [row[0] for row in cursor.fetchall()]

# --- Filter tables matching pattern cbbo_YYYYMMDD ---
pattern = re.compile(r'^cbbo_(\d{8})$')
matched_tables = [t for t in tables if pattern.match(t)]

for table in matched_tables:
    date_str = pattern.match(table).group(1)       # '20240614'
    yyyy_mm_dd = f"{date_str[:4]}-{date_str[4:6]}-{date_str[6:]}"  # '2024-06-14'
    yymmdd = date_str[2:]                          # '240614'
    new_table = f"sa_exp_{yymmdd}"

    print(f"Creating {dest_db}.{new_table} from {source_db}.{table}...")

    query = f"""
        CREATE TABLE {dest_db}.{new_table} AS
        SELECT * FROM {source_db}.{table}
        WHERE symbol LIKE 'SPXW  {yymmdd}%'
          AND DATE(ts_recv) = '{yyyy_mm_dd}';
    """

    try:
        cursor.execute(query)
        print(f"✅ Created table: {dest_db}.{new_table}")
    except Exception as e:
        print(f"❌ Error with {table}: {e}")

cursor.close()
conn.close()
