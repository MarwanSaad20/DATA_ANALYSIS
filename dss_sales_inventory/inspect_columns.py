import os
import pandas as pd

PROJECT_ROOT = os.path.dirname(os.path.abspath(__file__))

SCAN_FOLDERS = [
    "ingestion",
    "cleaning",
    "features",
    "analysis",
    "reporting"
]

SUPPORTED_EXTENSIONS = (".csv", ".xlsx", ".xls", ".parquet")

# ---------------------------------------
# Heuristic keywords for sensitivity
# ---------------------------------------
SENSITIVITY_KEYWORDS = [
    "sales",
    "profit",
    "revenue",
    "stock",
    "inventory",
    "cost",
    "price",
    "demand",
    "risk",
    "volatility",
    "std",
    "var",
    "mean",
    "quantity",
    "units",
    "remaining",
    "ratio"
]

# store global candidates
GLOBAL_CANDIDATES = []


def is_candidate_column(col_name: str):
    c = col_name.lower()
    return any(k in c for k in SENSITIVITY_KEYWORDS)


def inspect_file(file_path):
    global GLOBAL_CANDIDATES

    try:
        if file_path.endswith(".csv"):
            df = pd.read_csv(file_path, nrows=5)
        elif file_path.endswith((".xlsx", ".xls")):
            df = pd.read_excel(file_path, nrows=5)
        elif file_path.endswith(".parquet"):
            df = pd.read_parquet(file_path)
        else:
            return

        print("\n" + "=" * 100)
        print(f"FILE : {file_path}")
        print("-" * 100)

        for col, dtype in zip(df.columns, df.dtypes):
            print(f"{col:35s} | {dtype}")

            # candidate only if numeric
            if pd.api.types.is_numeric_dtype(dtype):
                if is_candidate_column(col):
                    GLOBAL_CANDIDATES.append({
                        "file": file_path,
                        "column": col,
                        "dtype": str(dtype)
                    })

    except Exception as e:
        print("\n" + "=" * 90)
        print(f"FILE : {file_path}")
        print("ERROR reading file:")
        print(e)


def print_recommended_columns():
    print("\n\n" + "=" * 110)
    print("RECOMMENDED COLUMNS FOR SENSITIVITY ANALYSIS")
    print("=" * 110)

    if not GLOBAL_CANDIDATES:
        print("No suitable sensitivity candidates found.")
        return

    df = pd.DataFrame(GLOBAL_CANDIDATES)

    # remove duplicates (same file + column)
    df = df.drop_duplicates(subset=["file", "column"])

    for file_path in sorted(df["file"].unique()):
        sub = df[df["file"] == file_path]

        print("\n" + "-" * 110)
        print(f"SOURCE FILE: {file_path}")
        print("-" * 110)

        for _, r in sub.iterrows():
            print(f"  - {r['column']:30s} | {r['dtype']}")

    print("\n" + "=" * 110)
    print("Total candidate variables:", len(df))
    print("=" * 110)


def main():

    print("\nScanning project for data files ...\n")

    for folder in SCAN_FOLDERS:
        folder_path = os.path.join(PROJECT_ROOT, folder)

        if not os.path.exists(folder_path):
            continue

        for root, _, files in os.walk(folder_path):
            for f in files:
                if f.lower().endswith(SUPPORTED_EXTENSIONS):
                    full_path = os.path.join(root, f)
                    inspect_file(full_path)

    # Final recommendation section
    print_recommended_columns()


if __name__ == "__main__":
    main()
