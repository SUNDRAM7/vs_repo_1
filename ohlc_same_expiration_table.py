import mysql.connector
import re


# --- Databases ---
source_db = "ohlc_opt_data_updated"
dest_db = "sd_exp_ohlc_spx_updated"


# --- Connect to MySQL ---
conn = mysql.connector.connect(
    host="localhost",
    user="root",
    password="Axxela@123"
)
cursor = conn.cursor()


# --- Ensure target DB exists ---
cursor.execute(f"CREATE DATABASE IF NOT EXISTS `{dest_db}`")


# --- Use source DB and get tables ---
cursor.execute(f"USE `{source_db}`")
cursor.execute("SHOW TABLES")
tables = [row[0] for row in cursor.fetchall()]
print(tables)


# --- Match ohlc_yymmdd tables ---
pattern = re.compile(r'^ohlc_(\d{6})$')
matched_tables = [t for t in tables if pattern.match(t)]
print(matched_tables)


for table in matched_tables:
    try:
        yymmdd = pattern.match(table).group(1)  # e.g., '240602'
        yyyy_mm_dd = f"20{yymmdd[:2]}-{yymmdd[2:4]}-{yymmdd[4:]}"  # e.g., 2024-06-02
        symbol_prefix = f"SPXW  {yymmdd}"
        new_table = f"ohlc_exp_updated_{yymmdd}"


        print(f"🔄 Processing `{source_db}`.`{table}` → `{dest_db}`.`{new_table}`")


        query = f"""
            CREATE TABLE `{dest_db}`.`{new_table}` AS
            SELECT * FROM `{source_db}`.`{table}`
            WHERE `symbol` LIKE '{symbol_prefix}%'
              AND `ts_event` IS NOT NULL
              AND DATE(`ts_event`) = '{yyyy_mm_dd}';
        """


        cursor.execute(query)
        print(f"✅ Created table: `{dest_db}`.`{new_table}`")
    except Exception as e:
        print(f"❌ Error processing table `{table}`: {e}")


# --- Cleanup ---
cursor.close()
conn.close()
