import pandas as pd
import sqlite3
from pathlib import Path


BASE_DIR = Path(__file__).resolve().parent

OUTPUT_FILE = BASE_DIR / "week11_data_audit.csv"

CSV_FOLDERS = [
    BASE_DIR / "data" / "raw",
    BASE_DIR / "data" / "processed",
    BASE_DIR / "analysis" / "forecast",
    BASE_DIR / "analysis" / "kpis",
    BASE_DIR / "analysis" / "risk",
    BASE_DIR / "reporting"  # إضافة مجلد reporting
]

SQLITE_DB = BASE_DIR / "analysis" / "analytics.db"


FINANCIAL_KEYWORDS = [
    "cost", "price", "revenue", "profit", "margin",
    "amount", "sales", "income", "expense"
]


def is_financial_candidate(col_name):
    name = col_name.lower()
    return any(k in name for k in FINANCIAL_KEYWORDS)


def analyze_dataframe(df, source_type, source_name, source_path):
    rows = []

    for col in df.columns:
        series = df[col]

        non_null = series.notna().sum()
        unique = series.nunique(dropna=True)

        example_value = None
        try:
            example_value = series.dropna().iloc[0]
        except:
            pass

        rows.append({
            "source_type": source_type,
            "source_name": source_name,
            "source_path": str(source_path),
            "column_name": col,
            "pandas_dtype": str(series.dtype),
            "non_null_count": int(non_null),
            "unique_count": int(unique),
            "sample_value": str(example_value),
            "financial_candidate": is_financial_candidate(col)
        })

    return rows


def scan_csv_sources():
    all_rows = []

    print("\n[Scanning CSV files]")

    for folder in CSV_FOLDERS:

        if not folder.exists():
            print(f"   Folder not found: {folder}")
            continue

        for file in folder.glob("*.csv"):
            print(f" - {file}")

            try:
                df = pd.read_csv(file)
            except Exception as e:
                print(f"   !! Failed to read {file}: {e}")
                continue

            rows = analyze_dataframe(
                df=df,
                source_type="csv",
                source_name=file.name,
                source_path=file
            )

            all_rows.extend(rows)

    return all_rows


def scan_sqlite_sources():
    all_rows = []

    print("\n[Scanning SQLite database]")

    if not SQLITE_DB.exists():
        print(f"Database not found: {SQLITE_DB}")
        return all_rows

    conn = sqlite3.connect(SQLITE_DB)

    try:
        tables = pd.read_sql(
            "SELECT name FROM sqlite_master WHERE type='table'",
            conn
        )["name"].tolist()

        for table in tables:
            print(f" - table: {table}")

            try:
                df = pd.read_sql(f"SELECT * FROM {table} LIMIT 1000", conn)
            except Exception as e:
                print(f"   !! Failed to read table {table}: {e}")
                continue

            rows = analyze_dataframe(
                df=df,
                source_type="sqlite_table",
                source_name=table,
                source_path=SQLITE_DB
            )

            for r in rows:
                r["sqlite_declared_type"] = get_sqlite_declared_type(conn, table, r["column_name"])

            all_rows.extend(rows)

    finally:
        conn.close()

    return all_rows


def get_sqlite_declared_type(conn, table, column):
    try:
        info = pd.read_sql(f"PRAGMA table_info({table})", conn)
        row = info[info["name"] == column]
        if not row.empty:
            return row.iloc[0]["type"]
    except:
        pass

    return None


def main():

    print("============================================")
    print("Week 11 – Global Data Sources Audit Script")
    print("============================================")

    rows = []

    rows.extend(scan_csv_sources())
    rows.extend(scan_sqlite_sources())

    if not rows:
        print("No data sources found.")
        return

    df = pd.DataFrame(rows)

    if "sqlite_declared_type" not in df.columns:
        df["sqlite_declared_type"] = None

    ordered_cols = [
        "source_type",
        "source_name",
        "source_path",
        "column_name",
        "pandas_dtype",
        "sqlite_declared_type",
        "non_null_count",
        "unique_count",
        "financial_candidate",
        "sample_value"
    ]

    df = df[ordered_cols]

    df.sort_values(
        by=["source_type", "source_name", "column_name"],
        inplace=True
    )

    df.to_csv(OUTPUT_FILE, index=False, encoding="utf-8-sig")

    print("\n============================================")
    print("Audit finished.")
    print(f"Output file created:\n{OUTPUT_FILE}")
    print("============================================")


if __name__ == "__main__":
    main()