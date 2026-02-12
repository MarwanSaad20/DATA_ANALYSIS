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
|                       |   Min |     Mean |   Median |   Max |
|:----------------------|------:|---------:|---------:|------:|
| inventory_risk_score  |     0 |  8.46902 |   5.9671 |   100 |
| demand_pressure_index |     0 | 20.8221  |  16.8421 |   100 |
| profitability_margin  |     0 | 19.4455  |  12.8972 |   100 |

## 5. Decision Flag Distribution
- **Safe:** 98
- **Requires Intervention:** 2

## 6. Top 10 Products by Risk & Pressure
|   product_id |   inventory_risk_score |   demand_pressure_index | decision_flag         |
|-------------:|-----------------------:|------------------------:|:----------------------|
|           62 |               100      |               0.0951791 | Safe                  |
|           67 |                21.2455 |               0.0842623 | Safe                  |
|           81 |                20.4605 |               0.0301259 | Safe                  |
|            3 |                17.4115 |              16.7027    | Safe                  |
|           40 |                17.2484 |               0.014861  | Safe                  |
|           85 |                17.2266 |               0.0115914 | Safe                  |
|           46 |                15.4717 |               8.81492   | Safe                  |
|            5 |                15.1348 |              20.3751    | Safe                  |
|           35 |                15.0323 |              27.6035    | Safe                  |
|           90 |                13.9763 |              90.6485    | Requires Intervention |