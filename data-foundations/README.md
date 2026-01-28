# Data Foundations

This repository covers the foundational stage of intelligent data analysis.
The focus is on understanding data structure, data types, and analytical readiness.

---

## Dataset Description

- File: `sales.csv`
- Location: `data/`
- Each row represents **one sales transaction**.

---

## Day 1 – Data Types & Structure Understanding

### Objective
Understand the structure of the dataset and classify each column based on its analytical data type.

---

### Tools Used
- Python
- Pandas
- CSV file

---

### Column Classification
### Column Classification

| Column Name | Analytical Type |
|------------|-----------------|
| Order_ID | Categorical (Identifier) |
| Product | Categorical (Nominal) |
| Category | Categorical (Nominal) |
| Quantity | Numerical (Discrete) |
| Unit_Price | Numerical (Continuous) |
| Region | Categorical (Nominal) |


> ⚠️ Note: Classification is analytical, not purely technical.
Some numerical-looking values may represent categorical information.

---

### Outcome
- Clear understanding of what each column represents
- Awareness of which columns are suitable for aggregation and analysis
- Foundation established for SQL querying and data quality checks

---

Next steps will focus on **data quality, missing values, and hidden inconsistencies**.
