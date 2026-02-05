import sqlite3
import pandas as pd
from pathlib import Path

BASE = Path(r"C:\Data_Analysis\dss_sales_inventory")

DATA = BASE / "data" / "processed"
ADVANCED_SQL_FILE = BASE / "analysis" / "sql" / "advanced_analysis.sql"
VIEWS_SQL_FILE = BASE / "analysis" / "sql" / "views.sql"
DB_FILE = BASE / "analysis" / "analytics.db"

OUTPUT_DIR = BASE / "reporting" / "outputs"


def load_tables(conn):
    """تحميل الجداول من CSV إلى قاعدة البيانات"""
    sales = pd.read_csv(DATA / "sales_features.csv")
    inventory = pd.read_csv(DATA / "inventory_features.csv")

    sales.to_sql("sales_features", conn, if_exists="replace", index=False)
    inventory.to_sql("inventory_features", conn, if_exists="replace", index=False)


def run_sql_file(conn, sql_file):
    """
    تنفيذ SQL من ملف محدد.
    - استعلامات SELECT تعيد DataFrame
    - باقي الاستعلامات (CREATE/DROP/INSERT) تُنفذ فقط
    """
    with open(sql_file, "r", encoding="utf-8") as f:
        sql_text = f.read()

    queries = [q.strip() for q in sql_text.split(";") if q.strip()]
    results = []

    for q in queries:
        q_upper = q.upper()
        try:
            if q_upper.startswith("SELECT"):
                df = pd.read_sql_query(q, conn)
                results.append(df)
            else:
                conn.execute(q)
                results.append(None)  # للحفاظ على ترتيب النتائج
        except Exception as e:
            print(f"SQL execution error:\n{q}\nError: {e}")
            raise

    return results


def export_views_as_csv(conn, view_names, output_dir):
    """
    بعد إنشاء الـ Views، ننفذ SELECT * FROM view_name لكل View
    لنصدر CSV لكل واحدة
    """
    for view_name in view_names:
        try:
            df = pd.read_sql_query(f"SELECT * FROM {view_name}", conn)
            df.to_csv(output_dir / f"{view_name}.csv", index=False)
        except Exception as e:
            print(f"Error exporting view {view_name}: {e}")
            raise


def main():
    conn = sqlite3.connect(DB_FILE)

    # تحميل الجداول
    load_tables(conn)

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    # -----------------------------
    # تشغيل advanced_analysis.sql
    # -----------------------------
    results = run_sql_file(conn, ADVANCED_SQL_FILE)

    # التصدير حسب ترتيب SELECT فقط
    if results[0] is not None:
        results[0].to_csv(OUTPUT_DIR / "product_performance.csv", index=False)
    if results[1] is not None:
        results[1].to_csv(OUTPUT_DIR / "demand_pressure.csv", index=False)
    if results[2] is not None:
        results[2].to_csv(OUTPUT_DIR / "inventory_pressure.csv", index=False)

    # -----------------------------
    # تشغيل views.sql
    # -----------------------------
    run_sql_file(conn, VIEWS_SQL_FILE)

    # تصدير كل View كـ CSV منفصل
    view_names = [
        "product_performance_view",
        "demand_pressure_view",
        "inventory_status_view"
    ]
    export_views_as_csv(conn, view_names, OUTPUT_DIR)

    conn.close()
    print("SQL analytics layer executed successfully.")
    print(f"Outputs saved in: {OUTPUT_DIR}")


if __name__ == "__main__":
    main()
