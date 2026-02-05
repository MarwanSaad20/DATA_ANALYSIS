تمام مروان، سأعد لك نسخة **محدثة كاملة من README.md** تعكس الهيكل الفعلي للمشروع الحالي بعد الأسبوع الثالث، مع توضيح **الملفات القديمة والجديدة** في `reporting/outputs` حتى لا يحدث أي التباس لمطور جديد.

---

```markdown
# Decision Support System for Sales & Inventory

نظام دعم قرار للمبيعات والمخزون – الإصدار 1.5 (أسبوع 1 + 2 + 3)

## نظرة عامة
نظام يحول البيانات الخام إلى بيانات جاهزة للتحليل، مع دعم قرارات المخزون والتسعير قصيرة المدى.  
يستخدم dict لتمرير البيانات بين المراحل مع correlation_id لتتبع كل run.  
الأسبوع الثاني والثالث أضافا SQL layer لتحليلات متقدمة مع Views لحظية لتعزيز سرعة اتخاذ القرار.

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
│   └─ sql/
│       ├─ advanced_analysis.sql
│       └─ views.sql          # الأسبوع الثالث: Views لحظية
│
├─ reporting/
│   └─ outputs/
│       ├─ analysis_summary.csv
│       ├─ product_performance_view.csv  # جديد الأسبوع الثالث
│       ├─ demand_pressure_view.csv      # جديد الأسبوع الثالث
│       ├─ inventory_status_view.csv     # جديد الأسبوع الثالث
│       ├─ plots/
│           ├─ histogram_daily_quantity.png
│           └─ trend_daily_revenue.png
│       ├─ (ملفات قديمة غير مستخدمة: product_performance.csv, demand_pressure.csv, inventory_pressure.csv)
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
* pandas, matplotlib, seaborn, uuid, sqlite3 (لـ SQL layer)

## تعليمات التشغيل

1. تأكد من وجود sales.csv و inventory.csv في data/raw/
2. شغل من جذر المشروع: `python pipeline.py`
3. تحقق من logs (يحتوي correlation_id لكل run)
4. بعد التشغيل، النتائج الجديدة موجودة في: `reporting/outputs/`

## المراحل والمخرجات

* **Ingestion**: تحميل + dict أولي
* **Cleaning**: تنظيف + referential checks
* **Features**: daily aggregation + stock_ratio
* **Analysis**: إحصاءات + رسوم
* **SQL Analytics (أسبوع 2 + 3)**:

  * `advanced_analysis.sql`: المنطق الأساسي باستخدام CTEs
  * `views.sql`: إنشاء 3 Views لحظية:

    * **product_performance_view**: متوسط المبيعات اليومية، الإيراد اليومي، performance_score
    * **demand_pressure_view**: نسبة المبيعات اليومية إلى المخزون المتاح، مستوى ضغط الطلب
    * **inventory_status_view**: stock_ratio مقارنة بـ reorder_point، حالة المخزون (safe/risk/critical)
  * النتائج تُصدر CSV مباشرة و تُخزن في SQLite db (analytics.db)

## التوسع المستقبلي

* Power BI dashboard باستخدام CSVs و SQLite db
* ML models في /ml باستخدام daily_quantity_sold و stock_ratio كميزات رئيسية للتنبؤ بالطلب وقياس المخاطر

## ملاحظات لمطور جديد

* تمرير dict لتجنب الاعتماديات
* correlation_id لتتبع runs في logs
* كل دالة unit-testable
* SQL layer يستخدم CTEs و subqueries لتحليلات متقدمة
* ملفات CSV القديمة موجودة فقط للتوافق مع الإصدار القديم (يمكن حذفها لاحقًا)

```

---

✅ **هذا التحديث يعكس:**
1. جميع الملفات الحالية كما في شجرة المشروع.  
2. توضيح الملفات القديمة مقابل الملفات الجديدة (Views الأسبوع الثالث).  
3. SQL layer الجديد والأسبوع الثالث مدمج بشكل كامل في README.  

---

