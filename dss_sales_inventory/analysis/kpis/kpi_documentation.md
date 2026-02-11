# KPI Layer Documentation Report

## 1. Execution Overview
- **Total Products Processed:** 100
- **Status:** Success

## 2. Data Sources & Deduplication
| Dataset | Raw Rows | Unique Products | Status |
|---|---|---|---|
| Inventory View | 36500 | 100 | Deduped |
| Inventory Features | 36500 | 100 | Deduped |
| Forecast | 2800 | 100 | Aggregated |
| Risk Results | 100 | 100 | Deduped |

## 3. Column Resolutions
- **Stock Source:** `stock_on_hand`
- **Forecast Source:** `forecast_quantity`
- **Profit Source:** `expected_profit_mean`

## 4. KPI Statistics
|                       |   Min |    Mean |   Median |   Max |
|:----------------------|------:|--------:|---------:|------:|
| inventory_risk_score  |     0 |  8.1073 |  5.56388 |   100 |
| demand_pressure_index |     0 | 20.8221 | 16.8421  |   100 |
| profitability_margin  |     0 | 19.4514 | 12.9103  |   100 |

## 5. Decision Flag Distribution
- **Safe:** 98
- **Requires Intervention:** 2

## 6. Top 10 Products by Risk & Pressure
|   product_id |   inventory_risk_score |   demand_pressure_index | decision_flag         |
|-------------:|-----------------------:|------------------------:|:----------------------|
|           62 |               100      |               0.0951791 | Safe                  |
|           67 |                20.3508 |               0.0842623 | Safe                  |
|           81 |                20.0896 |               0.0301259 | Safe                  |
|            3 |                17.0824 |              16.7027    | Safe                  |
|           40 |                16.5615 |               0.014861  | Safe                  |
|           85 |                15.7128 |               0.0115914 | Safe                  |
|           46 |                15.0653 |               8.81492   | Safe                  |
|           35 |                14.8192 |              27.6035    | Safe                  |
|            5 |                14.7211 |              20.3751    | Safe                  |
|           90 |                13.6883 |              90.6485    | Requires Intervention |