
# Decision Support System for Sales & Inventory

نظام دعم قرار للمبيعات والمخزون – الإصدار 1.6 (أسبوع 1 + 2 + 3 + 4)

## نظرة عامة
نظام يحول البيانات الخام إلى بيانات جاهزة للتحليل، مع دعم قرارات المخزون والتسعير قصيرة المدى.  
الأسبوع الرابع أضاف **تحليل السلاسل الزمنية (Time Series Analysis)** لكل المنتجات، مع استخراج **توصيات لكل منتج** وحفظ **CSV summary** ورسوم بيانية لكل منتج وأيضًا رسم شامل لجميع المنتجات.

## هيكل المجلدات
```

dss_sales_inventory/
│
├─ data/
│   ├─ raw/                  # sales.csv, inventory.csv
│   └─ processed/            # cleaned و features CSVs
│
├─ ingestion/
│   └─ ingestion.py
│
├─ cleaning/
│   └─ cleaning.py
│
├─ features/
│   └─ features.py
│
├─ analysis/
│   ├─ analysis.py
│   ├─ run_sql_layer.py       # SQL analytics layer runner
│   ├─ analytics.db           # SQLite database لتخزين نتائج SQL
│   └─ time_series/
│       ├─ time_series_analysis.py   # الأسبوع الرابع: Time Series Analysis
│       ├─ trend_insights.md        # Markdown report
│       └─ **pycache**/
│
├─ reporting/
│   └─ outputs/
│       ├─ product_performance_view.csv
│       ├─ demand_pressure_view.csv
│       ├─ inventory_status_view.csv
│       ├─ time_series_summary.csv    # الأسبوع الرابع: CSV summary
│       └─ plots/
│           ├─ combined_all_products_trend.png
│           ├─ trend_product_XX.png (لكل منتج محدد في التحليل)
│
├─ logs/                     # pipeline_{run_id}.log
├─ pipeline.py
└─ README.md

```

## تدفق البيانات (Data Flow)
```

Raw Data (data/raw/)
│
▼
Ingestion → dict {'sales': df[...], 'inventory': df[...]} (correlation_id)
│
▼
Cleaning → cleaned dict + CSVs
│
▼
Features → featured dict + daily features + CSVs
│
▼
Analysis → summary CSV + plots
│      └─ Time Series Analysis (أسبوع 4)
│         ├─ trend_insights.md
│         ├─ time_series_summary.csv
│         └─ plots (رسوم لكل منتج + رسم شامل)
│
▼
SQL Analytics → SQLite (analytics.db) + CSV outputs (Views لحظية)
│
▼
Reporting → جاهزية القرار

````

```mermaid
graph TD
    Raw["Raw Data (data/raw/)"] --> Ingestion
    Ingestion --> Cleaning
    Cleaning --> Features
    Features --> Analysis["Analysis Python"]
    Features --> SQLLayer["SQL Analytics Layer<br>advanced_analysis.sql"]
    SQLLayer --> Views["SQL Views<br>views.sql<br>(product_performance_view, demand_pressure_view, inventory_status_view)"]
    Analysis --> Reporting["reporting/outputs/"]
    Views --> Reporting
````

## المتطلبات

* Python 3.9+
* pandas, matplotlib, seaborn, uuid, sqlite3

## تعليمات التشغيل

1. تأكد من وجود sales.csv و inventory.csv في data/raw/
2. شغل من جذر المشروع: `python pipeline.py`
3. تحقق من logs (يحتوي correlation_id لكل run)
4. بعد التشغيل، النتائج الجديدة موجودة في: `reporting/outputs/`
   **يشمل ذلك:**

   * CSV summary لكل منتج (`time_series_summary.csv`)
   * Markdown report (`trend_insights.md`)
   * Plots فردية لكل منتج + رسم شامل لجميع المنتجات

## المراحل والمخرجات

* **Ingestion**: تحميل + dict أولي
* **Cleaning**: تنظيف + referential checks
* **Features**: daily aggregation + stock_ratio
* **Analysis**:

  * إحصاءات + رسوم لكل منتج
  * **Time Series Analysis (أسبوع 4)**:

    * الكشف عن الاتجاه (Trend) لكل منتج: Up / Down / Stable
    * توليد توصية ذكية بناءً على المخزون والطلب
    * حفظ CSV summary + Plots فردية + رسم شامل لجميع المنتجات
* **SQL Analytics (أسبوع 2 + 3)**:

  * `advanced_analysis.sql`: المنطق الأساسي باستخدام CTEs
  * `views.sql`: إنشاء 3 Views لحظية:

    * **product_performance_view**: متوسط المبيعات اليومية، الإيراد اليومي، performance_score
    * **demand_pressure_view**: نسبة المبيعات اليومية إلى المخزون المتاح، مستوى ضغط الطلب
    * **inventory_status_view**: stock_ratio مقارنة بـ reorder_point، حالة المخزون (safe/risk/critical)

## التوسع المستقبلي

* Power BI dashboard باستخدام CSVs و SQLite db
* ML models في /ml باستخدام daily_quantity_sold و stock_ratio كميزات رئيسية للتنبؤ بالطلب وقياس المخاطر

## ملاحظات لمطور جديد

* تمرير dict لتجنب الاعتماديات
* correlation_id لتتبع runs في logs
* كل دالة unit-testable
* SQL layer يستخدم CTEs و subqueries لتحليلات متقدمة
* ملفات CSV القديمة موجودة فقط للتوافق مع الإصدار القديم (يمكن حذفها لاحقًا)
* Time Series Analysis مدمج الآن مع التوصيات لكل منتج وتصدير CSV

```

---
