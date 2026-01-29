import sqlite3
import os

# Paths
base_dir = os.path.dirname(os.path.abspath(__file__))
db_path = os.path.join(base_dir, "..", "database", "sales.db")
sql_path = os.path.join(base_dir, "..", "sql", "day6", "inventory_stats.sql")
output_path = os.path.join(base_dir, "..", "outputs", "day6_inventory_results.txt")

# Connect to SQLite DB
conn = sqlite3.connect(db_path)
cursor = conn.cursor()

# قراءة الاستعلامات من ملف SQL
with open(sql_path, "r") as f:
    sql_content = f.read()

# تقسيم الاستعلامات على أساس ";" 
queries = [q.strip() for q in sql_content.split(";") if q.strip()]

# تنفيذ كل استعلام وحفظ النتائج
with open(output_path, "w") as f:
    for i, query in enumerate(queries, start=1):
        f.write(f"--- Query {i} ---\n")
        cursor.execute(query)
        rows = cursor.fetchall()
        for row in rows:
            f.write(str(row) + "\n")
        f.write("\n")

print(f"[INFO] Inventory statistics executed. Results saved to {output_path}")

# Close connection
conn.close()
