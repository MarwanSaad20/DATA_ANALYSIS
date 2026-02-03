```markdown
# Decision Support System for Sales & Inventory

نظام دعم قرار للمبيعات والمخزون – الأسبوع 1: بناء الأساس الهيكلي والـ Pipeline (إصدار 1.5)

## نظرة عامة
نظام يحول البيانات الخام إلى بيانات جاهزة للتحليل، مع دعم قرارات المخزون والتسعير قصيرة المدى. يستخدم dict لتمرير البيانات بين المراحل مع correlation_id لتتبع كل run.

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
│   └─ analysis.py
│
├─ reporting/
│   └─ outputs/              # analysis_summary.csv + plots/
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
Reporting → جاهزية للقرار (ملاحظة: daily_* أعمدة جاهزة لـ Forecasting/ML)
```

## المتطلبات
- Python 3.9+
- pandas, matplotlib, seaborn, uuid (لـ correlation_id)

## تعليمات التشغيل
1. تأكد من وجود sales.csv و inventory.csv في data/raw/
2. شغل من الجذر: `python pipeline.py`
3. تحقق من logging (يحتوي correlation_id لكل run).

## المراحل والمخرجات
- Ingestion: تحميل + dict أولي
- Cleaning: تنظيف + referential checks
- Features: daily aggregation + stock_ratio
- Analysis: إحصاءات + رسوم

## التوسع المستقبلي
- الأسبوع 2+: SQL Views و advanced_analysis.sql
- لاحقًا: Power BI للـ dashboard، ML في /ml (استخدام daily_quantity_sold و stock_ratio كميزات رئيسية لـ forecasting demand و risk models).

## ملاحظات لمطور جديد
- تمرير dict لتجنب الاعتماديات.
- correlation_id لتتبع runs في logs.
- كل دالة unit-testable.
```