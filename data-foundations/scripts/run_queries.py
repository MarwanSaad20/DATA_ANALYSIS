import sqlite3
import os

# =========================
# Paths & Setup
# =========================
base_dir = os.path.dirname(os.path.abspath(__file__))  # scripts/
project_root = os.path.abspath(os.path.join(base_dir, ".."))

db_path = os.path.join(project_root, "database", "sales.db")
output_path = os.path.join(project_root, "outputs", "query_results.txt")

day5_sql_dir = os.path.join(project_root, "sql", "day5")

# =========================
# Connect to Database
# =========================
conn = sqlite3.connect(db_path)
cursor = conn.cursor()

# =========================
# Day 4 Queries (Foundational SQL)
# =========================
day4_queries = {
    "Day 4 | Total Sales by Product": """
        SELECT Product, SUM(Quantity * Unit_Price) AS Total_Sales
        FROM sales
        GROUP BY Product
        ORDER BY Total_Sales DESC;
    """,

    "Day 4 | Sales by Region": """
        SELECT Region, SUM(Quantity * Unit_Price) AS Region_Sales
        FROM sales
        GROUP BY Region
        ORDER BY Region_Sales DESC;
    """,

    "Day 4 | High Value Orders (>500)": """
        SELECT Order_ID, Product, Quantity, Unit_Price,
               Quantity * Unit_Price AS Total_Order_Value
        FROM sales
        WHERE Quantity * Unit_Price > 500
        ORDER BY Total_Order_Value DESC;
    """
}

# =========================
# Execute & Save Results
# =========================
with open(output_path, "w", encoding="utf-8") as f:

    # ---------- Day 4 ----------
    f.write("##############################\n")
    f.write("## DAY 4 – SQL FOUNDATIONS ##\n")
    f.write("##############################\n\n")

    for title, query in day4_queries.items():
        f.write(f"=== {title} ===\n")
        cursor.execute(query)
        rows = cursor.fetchall()

        for row in rows:
            f.write(str(row) + "\n")
        f.write("\n")

    # ---------- Day 5 ----------
    f.write("\n\n##############################\n")
    f.write("## DAY 5 – STRATEGIC SQL ANALYTICS ##\n")
    f.write("##############################\n\n")

    if not os.path.exists(day5_sql_dir):
        f.write("[ERROR] Day 5 SQL directory not found.\n")
    else:
        sql_files = sorted([
            file for file in os.listdir(day5_sql_dir)
            if file.endswith(".sql")
        ])

        for sql_file in sql_files:
            file_path = os.path.join(day5_sql_dir, sql_file)

            f.write(f"\n--- Executing: {sql_file} ---\n")

            with open(file_path, "r", encoding="utf-8") as sql:
                sql_script = sql.read()

            try:
                statements = sql_script.split(";")
                for stmt in statements:
                    stmt = stmt.strip()
                    if stmt:
                        cursor.execute(stmt)
                        rows = cursor.fetchall()
                        for row in rows:
                            f.write(str(row) + "\n")

            except Exception as e:
                f.write(f"[ERROR] {e}\n")

# =========================
# Close Connection
# =========================
conn.close()

print("[INFO] Day 4 and Day 5 analysis executed successfully.")
print(f"[INFO] Results saved to: {output_path}")
