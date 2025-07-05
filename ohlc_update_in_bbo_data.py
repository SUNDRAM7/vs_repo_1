import mysql.connector
from mysql.connector import errorcode


DB_CONFIG = {
    'user': 'root',
    'password': 'Axxela@123',
    'host': 'localhost',
}
BATCH_SIZE = 1000
DB_SOURCE = 'ohlc_opt_data'
DB_TARGET = 'sa_exp_bbo_spx'


def connect():
    return mysql.connector.connect(**DB_CONFIG)


def get_matching_tables(cursor):
    cursor.execute(f"SHOW TABLES FROM {DB_SOURCE}")
    source_tables = [row[0] for row in cursor.fetchall() if row[0].startswith('ohlc_')]

    suffixes = [tbl.replace('ohlc_', '') for tbl in source_tables]
    matching = []

    for suffix in suffixes:
        src = f"ohlc_{suffix}"
        tgt = f"sa_exp_{suffix}"
        cursor.execute(f"SHOW TABLES FROM {DB_TARGET} LIKE '{tgt}'")
        if cursor.fetchone():
            matching.append((src, tgt))

    return matching


def ensure_columns(cursor, db, table, columns):
    cursor.execute(f"SHOW COLUMNS FROM {db}.{table}")
    existing_cols = {row[0] for row in cursor.fetchall()}

    for col in columns:
        if col not in existing_cols:
            print(f"Adding column `{col}` to {db}.{table}")
            cursor.execute(f"ALTER TABLE {db}.{table} ADD COLUMN `{col}` DOUBLE DEFAULT NULL")


def update_in_batches(cnx, src_table, tgt_table):
    cursor = cnx.cursor(dictionary=True)

    # Get total matching rows
    count_query = f"""
        SELECT COUNT(*) AS cnt
        FROM {DB_TARGET}.{tgt_table} b
        JOIN {DB_SOURCE}.{src_table} o
        ON b.instrument_id = o.instrument_id AND b.ts_recv = o.ts_event
    """
    cursor.execute(count_query)
    total_rows = cursor.fetchone()['cnt']
    print(f"Updating {total_rows} rows in table {tgt_table}...")

    for offset in range(0, total_rows, BATCH_SIZE):
        print(offset)
        print(f"  - Processing batch {offset} to {offset + BATCH_SIZE}")

        update_query = f"""
            UPDATE {DB_TARGET}.{tgt_table} b
            JOIN (
                SELECT o.symbol, o.ts_event, o.open, o.high, o.low, o.close
                FROM {DB_SOURCE}.{src_table} o
                LIMIT {offset}, {BATCH_SIZE}
            ) AS o
            ON b.instrument_id = o.instrument_id AND b.ts_recv = o.ts_event
            SET 
                b.open = o.open,
                b.high = o.high,
                b.low  = o.low,
                b.close = o.close
        """
        cursor.execute(update_query)
        cnx.commit()

    cursor.close()


def main():
    try:
        cnx = connect()
        cursor = cnx.cursor()

        table_pairs = get_matching_tables(cursor)
        print(f"Found {len(table_pairs)} matching table pairs")

        for src, tgt in table_pairs:
            print(f"\n🔄 Processing: {src} ➜ {tgt}")
            ensure_columns(cursor, DB_TARGET, tgt, ['open', 'high', 'low', 'close'])
            update_in_batches(cnx, src, tgt)

        print("\n✅ All tables processed.")
        cursor.close()
        cnx.close()

    except mysql.connector.Error as err:
        if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
            print("Access denied: check username/password")
        elif err.errno == errorcode.ER_BAD_DB_ERROR:
            print("Database does not exist")
        else:
            print(err)


if __name__ == "__main__":
    main()