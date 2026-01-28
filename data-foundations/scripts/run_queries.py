import sqlite3
import os

# Paths
base_dir = os.path.dirname(os.path.abspath(__file__))  # scripts/
db_path = os.path.join(base_dir, "..", "database", "sales.db")
output_path = os.path.join(base_dir, "..", "outputs", "query_results.txt")

# Connect to DB
conn = sqlite3.connect(db_path)
cursor = conn.cursor()

# قائمة الاستعلامات التحليلية (نماذج اليوم الرابع)
queries = {
    "Total Sales by Product": """
        SELECT Product, SUM(Quantity*Unit_Price) AS Total_Sales
        FROM sales
        GROUP BY Product
        ORDER BY Total_Sales DESC;
    """,
    "Sales by Region": """
        SELECT Region, SUM(Quantity*Unit_Price) AS Region_Sales
        FROM sales
        GROUP BY Region
        ORDER BY Region_Sales DESC;
    """,
    "Top Orders (>500)": """
        SELECT Order_ID, Product, Quantity, Unit_Price, Quantity*Unit_Price AS Total
        FROM sales
        WHERE Quantity*Unit_Price > 500;
    """
}

# تنفيذ الاستعلامات وحفظ النتائج
with open(output_path, "w") as f:
    for name, query in queries.items():
        f.write(f"=== {name} ===\n")
        cursor.execute(query)
        rows = cursor.fetchall()
        for row in rows:
            f.write(str(row) + "\n")
        f.write("\n")

print(f"[INFO] Queries executed and results saved to {output_path}")

# Close connection
conn.close()
