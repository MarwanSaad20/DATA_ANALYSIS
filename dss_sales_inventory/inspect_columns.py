import os
import pandas as pd
import sqlite3
from collections import defaultdict

PROJECT_ROOT = os.path.dirname(os.path.abspath(__file__))

# -----------------------------------------
# Columns required by KPI layer
# -----------------------------------------
REQUIRED_COLUMNS = [
    "on_hand_quantity",
    "stock_on_hand",
    "predicted_demand",
    "demand_mean",
    "daily_sales",
    "avg_daily_sales",
    "unit_price",
    "unit_cost",
    "revenue",
    "profit",
    "risk_score"
]

# -----------------------------------------
# Storage
# -----------------------------------------
column_locations = defaultdict(list)

# -----------------------------------------
# Scan CSV files
# -----------------------------------------
def scan_csv_files(root):
    for root_dir, _, files in os.walk(root):
        for file in files:
            if file.lower().endswith(".csv"):
                path = os.path.join(root_dir, file)
                try:
                    df = pd.read_csv(path, nrows=1)
                    for col in df.columns:
                        column_locations[col].append(
                            f"CSV -> {os.path.relpath(path, PROJECT_ROOT)}"
                        )
                except Exception as e:
                    print(f"[WARN] Could not read CSV: {path} -> {e}")

# -----------------------------------------
# Scan SQLite databases
# -----------------------------------------
def scan_sqlite_files(root):
    for root_dir, _, files in os.walk(root):
        for file in files:
            if file.lower().endswith(".db"):
                db_path = os.path.join(root_dir, file)

                try:
                    conn = sqlite3.connect(db_path)
                    cur = conn.cursor()

                    tables = cur.execute(
                        "SELECT name FROM sqlite_master WHERE type='table';"
                    ).fetchall()

                    for (table_name,) in tables:
                        try:
                            cols = cur.execute(
                                f"PRAGMA table_info('{table_name}')"
                            ).fetchall()

                            for col in cols:
                                col_name = col[1]
                                column_locations[col_name].append(
                                    f"DB -> {os.path.relpath(db_path, PROJECT_ROOT)} :: table={table_name}"
                                )
                        except Exception:
                            pass

                    conn.close()

                except Exception as e:
                    print(f"[WARN] Could not open DB: {db_path} -> {e}")

# -----------------------------------------
# Report
# -----------------------------------------
def print_report():

    print("\n" + "=" * 80)
    print("KPI DATA DEPENDENCY INSPECTION REPORT")
    print("=" * 80)

    print("\n[1] Required KPI columns availability\n")

    for col in REQUIRED_COLUMNS:
        if col in column_locations:
            print(f"[FOUND] {col}")
            for loc in column_locations[col]:
                print(f"        - {loc}")
        else:
            print(f"[MISSING] {col}")

    print("\n" + "-" * 80)
    print("[2] All discovered columns and their locations\n")

    for col in sorted(column_locations.keys()):
        print(f"\n{col}")
        for loc in column_locations[col]:
            print(f"    - {loc}")

    print("\n" + "=" * 80)
    print("Inspection completed.")
    print("=" * 80)


if __name__ == "__main__":
    print("Scanning project for data columns ...")

    scan_csv_files(PROJECT_ROOT)
    scan_sqlite_files(PROJECT_ROOT)

    print_report()
