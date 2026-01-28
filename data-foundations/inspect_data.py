import pandas as pd
import matplotlib.pyplot as plt

# =========================
# Load Data
# =========================
file_path = r"C:\Data_Analysis\data-foundations\data\sales.csv"
df = pd.read_csv(file_path)

# =========================
# Basic Structure Check
# =========================
print("Rows:", df.shape[0])
print("Columns:", df.shape[1])
print(df.head())

# =========================
# Detect Data Limitations
# =========================
print("\nMissing Values:")
print(df.isnull().sum())

print("\nUnique Values per Column:")
for col in df.columns:
    print(col, ":", df[col].nunique())

# =========================
# Visualization: Sales by Product
# =========================
sales_by_product = df.groupby("Product")["Quantity"].sum()
sales_by_product.plot(kind="bar")
plt.title("Sales Distribution by Product")
plt.xlabel("Product")
plt.ylabel("Total Quantity Sold")
plt.tight_layout()
plt.savefig("sales_by_product.png")
plt.show()

# =========================
# Visualization: Sales by Region
# =========================
sales_by_region = df.groupby("Region")["Quantity"].sum()
sales_by_region.plot(kind="bar")
plt.title("Sales Distribution by Region")
plt.xlabel("Region")
plt.ylabel("Total Quantity Sold")
plt.tight_layout()
plt.savefig("sales_by_region.png")
plt.show()
