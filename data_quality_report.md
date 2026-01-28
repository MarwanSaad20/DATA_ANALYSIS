# Day 2 – Data Sources & Quality Report

## Dataset
- File: sales.csv
- Source: Local CSV / Kaggle (replace with actual Kaggle link if applicable)

## 1. Missing Values
| Column | Missing Count |
|--------|---------------|
| Order_ID | 0 |
| Product | 0 |
| Category | 0 |
| Quantity | 0 |
| Unit_Price | 0 |
| Region | 0 |

## 2. Duplicate Rows
- Total duplicates: 0

## 3. Basic Stats
- Quantity: min=2, max=6, mean≈4
- Unit_Price: min=50, max=900, mean≈450
- Other columns: categorical overview

## 4. Potential Biases
- Region: 4 unique values (North, South, East, West) → fairly balanced
- Category: 2 unique values (Electronics, Accessories)
- Order_ID: 30 unique identifiers → all unique

## Conclusion
- Dataset is **complete** and **clean** for basic analysis
- Minimal risk of bias for current dataset
- Ready for SQL querying and aggregation

> Next step: Use SQL for simple queries (SELECT, WHERE, ORDER BY, COUNT, SUM, AVG)
