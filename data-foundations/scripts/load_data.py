import pandas as pd
import sqlite3
import os

# ---------------------------
# Paths (نسبي بالنسبة للسكريبت)
# ---------------------------
base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))  # مجلد data-foundations
data_path = os.path.join(base_dir, "data", "sales.csv")
db_path = os.path.join(base_dir, "database", "sales.db")
schema_path = os.path.join(base_dir, "sql", "schema.sql")

# ---------------------------
# 1️⃣ التحقق من وجود ملف CSV
# ---------------------------
if not os.path.exists(data_path):
    raise FileNotFoundError(f"CSV file not found: {data_path}")
print(f"[INFO] Loaded CSV file: {data_path}")

# ---------------------------
# 2️⃣ تحميل البيانات
# ---------------------------
df = pd.read_csv(data_path)
print(f"[INFO] {len(df)} rows loaded from CSV.")

# ---------------------------
# 3️⃣ الاتصال بقاعدة البيانات
# ---------------------------
conn = sqlite3.connect(db_path)
print(f"[INFO] Connected to SQLite database: {db_path}")

# ---------------------------
# 4️⃣ تنفيذ schema.sql إذا موجود
# ---------------------------
if os.path.exists(schema_path):
    with open(schema_path, "r") as f:
        schema_sql = f.read()
    conn.executescript(schema_sql)
    print(f"[INFO] Executed schema from: {schema_path}")
else:
    print(f"[WARNING] Schema file not found: {schema_path}")

# ---------------------------
# 5️⃣ إدخال البيانات في الجدول مع التعامل مع التكرارات
# ---------------------------
try:
    # إذا كان الجدول موجود مسبقًا، استبدله لتجنب IntegrityError
    df.to_sql("sales", conn, if_exists="replace", index=False)
    print(f"[INFO] Inserted {len(df)} rows into 'sales' table (if_exists='replace').")
except sqlite3.IntegrityError as e:
    print(f"[ERROR] IntegrityError: {e}")
finally:
    conn.close()
    print("[INFO] Database connection closed.")
