import pandas as pd

# =========================
# Load Data
# =========================
file_path = r"C:\Data_Analysis\data-foundations\data\sales.csv"
df = pd.read_csv(file_path)

# =========================
# Quick Overview
# =========================
print("=== Data Preview ===")
print(df.head(), "\n")

print("=== Data Shape ===")
print(f"Rows: {df.shape[0]}, Columns: {df.shape[1]}\n")

# =========================
# Data Types Inspection
# =========================
print("=== Raw Data Types ===")
print(df.dtypes, "\n")

# =========================
# Analytical Classification
# =========================
data_classification = {}

for col in df.columns:
    dtype = df[col].dtype

    if pd.api.types.is_numeric_dtype(dtype):
        data_classification[col] = "Numerical"
    elif pd.api.types.is_datetime64_any_dtype(dtype):
        data_classification[col] = "Date/Time"
    else:
        data_classification[col] = "Categorical / Text"

print("=== Analytical Column Classification ===")
for column, classification in data_classification.items():
    print(f"- {column}: {classification}")


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
