/*
===============================================================================
DSS – Sales & Inventory
Advanced Analytics Layer (Week 2)

File      : advanced_analysis.sql
Project   : dss_sales_inventory (v1.5)
Data Base : SQLite (assumed)

Input tables (loaded from data/processed):
  - sales_features
      (product_id, date, daily_quantity_sold, daily_revenue)

  - inventory_features
      (product_id, date, stock_on_hand, reorder_point, stock_ratio)

Grain:
  One row per (product_id, date)

This file produces THREE analytical result sets:
  1) Product Performance
  2) Demand Pressure
  3) Inventory Pressure
===============================================================================
*/



/* =======================================================================
   1) PRODUCT PERFORMANCE
   -----------------------------------------------------------------------
   Purpose:
     Operational evaluation of each product performance.

   Output (per product):
     - total_units_sold
     - total_revenue
     - avg_daily_revenue
   ======================================================================= */

WITH product_base AS (
    SELECT
        product_id,
        SUM(daily_quantity_sold)      AS total_units_sold,
        SUM(daily_revenue)            AS total_revenue,
        COUNT(DISTINCT date)          AS active_days
    FROM sales_features
    GROUP BY product_id
)

SELECT
    product_id,
    total_units_sold,
    total_revenue,
    ROUND(
        total_revenue / NULLIF(active_days, 0),
        4
    ) AS avg_daily_revenue
FROM product_base
ORDER BY total_revenue DESC;



/* =======================================================================
   2) DEMAND PRESSURE
   -----------------------------------------------------------------------
   Purpose:
     Measure demand level and short-term demand trend per product.

   Indicators:
     - avg_daily_demand
     - avg_last_7_days
     - avg_prev_7_days
     - demand_trend_ratio
     - demand_volatility  (population std approximation)

   Notes:
     SQLite has no native STDDEV function.
     Volatility is computed as:
       sqrt( avg(x^2) - avg(x)^2 )
   ======================================================================= */

WITH ordered_sales AS (
    SELECT
        product_id,
        date,
        daily_quantity_sold,
        ROW_NUMBER() OVER (
            PARTITION BY product_id
            ORDER BY date
        ) AS rn_desc
    FROM sales_features
),

base_stats AS (
    SELECT
        product_id,
        AVG(daily_quantity_sold) AS avg_daily_demand,
        AVG(daily_quantity_sold * daily_quantity_sold) AS avg_sq
    FROM sales_features
    GROUP BY product_id
),

last_7_days AS (
    SELECT
        product_id,
        AVG(daily_quantity_sold) AS avg_last_7_days
    FROM ordered_sales
    WHERE rn_desc <= 7
    GROUP BY product_id
),

prev_7_days AS (
    SELECT
        product_id,
        AVG(daily_quantity_sold) AS avg_prev_7_days
    FROM ordered_sales
    WHERE rn_desc BETWEEN 8 AND 14
    GROUP BY product_id
)

SELECT
    b.product_id,

    b.avg_daily_demand,

    l.avg_last_7_days,
    p.avg_prev_7_days,

    ROUND(
        l.avg_last_7_days / NULLIF(p.avg_prev_7_days, 0),
        4
    ) AS demand_trend_ratio,

    ROUND(
        SQRT(
            b.avg_sq - (b.avg_daily_demand * b.avg_daily_demand)
        ),
        4
    ) AS demand_volatility

FROM base_stats b
LEFT JOIN last_7_days l
    ON b.product_id = l.product_id
LEFT JOIN prev_7_days p
    ON b.product_id = p.product_id
ORDER BY b.product_id;



/* =======================================================================
   3) INVENTORY PRESSURE
   -----------------------------------------------------------------------
   Purpose:
     Detect inventory risk and operational pressure per product.

   Indicators:
     - avg_stock_ratio
     - days_below_reorder
     - risk_flag

   Risk rule (v1.5 – SQL layer):
     risk_flag = 1 if:
       avg_stock_ratio < 1.5
       OR
       days_below_reorder > 3
   ======================================================================= */

WITH inventory_base AS (
    SELECT
        product_id,
        AVG(stock_ratio) AS avg_stock_ratio,
        SUM(
            CASE
                WHEN stock_on_hand < reorder_point THEN 1
                ELSE 0
            END
        ) AS days_below_reorder
    FROM inventory_features
    GROUP BY product_id
)

SELECT
    product_id,
    ROUND(avg_stock_ratio, 4) AS avg_stock_ratio,
    days_below_reorder,

    CASE
        WHEN avg_stock_ratio < 1.5
             OR days_below_reorder > 3
        THEN 1
        ELSE 0
    END AS risk_flag

FROM inventory_base
ORDER BY risk_flag DESC, avg_stock_ratio ASC;
