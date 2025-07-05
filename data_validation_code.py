import pymysql
import pandas as pd

# ==== CONFIG ====
db_config = {
    'host': 'localhost',
    'user': 'root',
    'password': 'Axxela@123',
    'database': 'sa_exp_bbo_spx',
    'port': 3306
}

column_to_check = 'open'  # Change this to any column you're interested in
output_file = 'null_report.xlsx'

# ==== SCRIPT ====
connection = pymysql.connect(**db_config)
cursor = connection.cursor()

# Get all tables
cursor.execute(f"""
    SELECT table_name 
    FROM information_schema.tables 
    WHERE table_schema = '{db_config['database']}'
""")
tables = [row[0] for row in cursor.fetchall()]

results = []

for table in tables:
    # Check if the column exists in this table
    cursor.execute(f"""
        SELECT COUNT(*) 
        FROM information_schema.columns
        WHERE table_schema = '{db_config['database']}'
          AND table_name = '{table}'
          AND column_name = '{column_to_check}'
    """)
    has_column = cursor.fetchone()[0] > 0

    # Get total rows
    try:
        cursor.execute(f"SELECT COUNT(*) FROM {table}")
        total_rows = cursor.fetchone()[0]
    except Exception as e:
        total_rows = f"Error: {e}"

    # Get NULL count if the column exists
    if has_column:
        try:
            cursor.execute(f"""
                SELECT COUNT(*) 
                FROM {table} 
                WHERE {column_to_check} IS NULL
            """)
            null_count = cursor.fetchone()[0]
        except Exception as e:
            null_count = f"Error: {e}"
    else:
        null_count = 'Column Not Found'

    results.append({
        'Table': table,
        'Total Rows': total_rows,
        f'NULLs in {column_to_check}': null_count
    })

# Save to Excel
df = pd.DataFrame(results)
df.to_excel(output_file, index=False)

print(f"✅ Report generated: {output_file}")
cursor.close()
connection.close()