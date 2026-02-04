import sqlite3
import pandas as pd
from pathlib import Path


BASE = Path(r"C:\Data_Analysis\dss_sales_inventory")

DATA = BASE / "data" / "processed"
SQL_FILE = BASE / "analysis" / "sql" / "advanced_analysis.sql"
DB_FILE = BASE / "analysis" / "analytics.db"

OUTPUT_DIR = BASE / "reporting" / "outputs"


def load_tables(conn):
    sales = pd.read_csv(DATA / "sales_features.csv")
    inventory = pd.read_csv(DATA / "inventory_features.csv")

    sales.to_sql("sales_features", conn, if_exists="replace", index=False)
    inventory.to_sql("inventory_features", conn, if_exists="replace", index=False)


def run_sql_file(conn):
    with open(SQL_FILE, "r", encoding="utf-8") as f:
        sql_text = f.read()

    queries = [q.strip() for q in sql_text.split(";") if q.strip()]

    results = []
    for q in queries:
        df = pd.read_sql_query(q, conn)
        results.append(df)

    return results


def main():
    conn = sqlite3.connect(DB_FILE)

    load_tables(conn)
    results = run_sql_file(conn)

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    # الترتيب يعتمد على ترتيب الاستعلامات داخل advanced_analysis.sql
    results[0].to_csv(OUTPUT_DIR / "product_performance.csv", index=False)
    results[1].to_csv(OUTPUT_DIR / "demand_pressure.csv", index=False)
    results[2].to_csv(OUTPUT_DIR / "inventory_pressure.csv", index=False)

    conn.close()

    print("SQL analytics layer executed successfully.")
    print(f"Outputs saved in: {OUTPUT_DIR}")


if __name__ == "__main__":
    main()
