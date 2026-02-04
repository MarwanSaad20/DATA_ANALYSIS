تمام، مروان. إليك نسخة **محدثة من README.md** لتشمل تحديثات الأسبوع الثاني، أي **SQL analytics layer والملفات الجديدة**، مع تعديل هيكل المجلدات والمخرجات:

```markdown
# Decision Support System for Sales & Inventory

نظام دعم قرار للمبيعات والمخزون – الإصدار 1.5 (أسبوع 1 + أسبوع 2)

## نظرة عامة
نظام يحول البيانات الخام إلى بيانات جاهزة للتحليل، مع دعم قرارات المخزون والتسعير قصيرة المدى.  
يستخدم dict لتمرير البيانات بين المراحل مع correlation_id لتتبع كل run.  
يتيح الأسبوع الثاني تحليل متقدم عبر SQL (advanced_analysis.sql) مع مؤشرات أداء المنتج، ضغط الطلب، وضغط المخزون.

## هيكل المجلدات
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
│       └─ advanced_analysis.sql
│
├─ reporting/
│   └─ outputs/              
│       ├─ analysis_summary.csv
│       ├─ product_performance.csv
│       ├─ demand_pressure.csv
│       ├─ inventory_pressure.csv
│       └─ plots/
│           ├─ histogram_daily_quantity.png
│           └─ trend_daily_revenue.png
│
├─ logs/                     # pipeline_{run_id}.log
├─ pipeline.py
└─ README.md

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
SQL Analytics → SQLite (analytics.db) + CSV outputs (product_performance, demand_pressure, inventory_pressure)
│
▼
Reporting → جاهزية للقرار

```

## المتطلبات
- Python 3.9+
- pandas, matplotlib, seaborn, uuid, sqlite3 (لـ SQL layer)

## تعليمات التشغيل
1. تأكد من وجود sales.csv و inventory.csv في data/raw/
2. شغل من جذر المشروع: `python pipeline.py`
3. تحقق من logs (يحتوي correlation_id لكل run)
4. بعد التشغيل، النتائج الجديدة موجودة في: `reporting/outputs/`

## المراحل والمخرجات
- Ingestion: تحميل + dict أولي
- Cleaning: تنظيف + referential checks
- Features: daily aggregation + stock_ratio
- Analysis: إحصاءات + رسوم
- SQL Analytics (أسبوع 2): مؤشرات أداء المنتج، ضغط الطلب، ضغط المخزون → CSVs + SQLite db

## التوسع المستقبلي
- Power BI dashboard باستخدام CSVs و SQLite db
- ML models في /ml باستخدام daily_quantity_sold و stock_ratio كميزات رئيسية للتنبؤ بالطلب وقياس المخاطر

## ملاحظات لمطور جديد
- تمرير dict لتجنب الاعتماديات.
- correlation_id لتتبع runs في logs.
- كل دالة unit-testable.
- SQL layer يستخدم CTEs و subqueries لتحليلات متقدمة.
```

إذا أحببت، أستطيع أن أصنع لك **نسخة README نهائية وجاهزة للـ Markdown مع جدول مختصر لكل ملف + وظيفة + مخرجات** لتسهيل فهم المشروع لأي مطور جديد.

هل تريد أن أفعل ذلك الآن؟
