import pandas as pd

# Load data
file_path = r"C:\Data_Analysis\data-foundations\data\sales.csv"
df = pd.read_csv(file_path)

# =========================
# Basic Overview
# =========================
print("=== Data Preview ===")
print(df.head(), "\n")

# =========================
# Check Missing Values
# =========================
missing_counts = df.isnull().sum()
print("=== Missing Values ===")
print(missing_counts, "\n")

# =========================
# Check Duplicate Rows
# =========================
duplicates = df.duplicated().sum()
print(f"Duplicate Rows: {duplicates}\n")

# =========================
# Check Basic Stats
# =========================
print("=== Basic Statistics ===")
print(df.describe(include='all'), "\n")

# =========================
# Optional: Detect Potential Biases
for col in df.columns:
    print(f"{col} unique values count: {df[col].nunique()}")
