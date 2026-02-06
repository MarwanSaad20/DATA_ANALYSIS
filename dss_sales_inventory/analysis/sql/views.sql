-- =========================================
-- 1. Product Performance View
-- =========================================
DROP VIEW IF EXISTS product_performance_view;

CREATE VIEW product_performance_view AS
WITH daily_sales AS (
    SELECT product_id, date, daily_quantity_sold, daily_revenue
    FROM sales_features
)
SELECT
    product_id,
    ROUND(AVG(daily_quantity_sold),2) AS avg_daily_sales,
    ROUND(SUM(daily_revenue),2) AS total_revenue,
    CASE 
        WHEN AVG(daily_quantity_sold) > 20 THEN 'High'
        WHEN AVG(daily_quantity_sold) BETWEEN 10 AND 20 THEN 'Medium'
        ELSE 'Low'
    END AS performance_score
FROM daily_sales
GROUP BY product_id;

-- =========================================
-- 2. Demand Pressure View
-- =========================================
DROP VIEW IF EXISTS demand_pressure_view;

CREATE VIEW demand_pressure_view AS
WITH daily_demand AS (
    SELECT s.product_id, s.date, s.daily_quantity_sold, i.stock_on_hand
    FROM sales_features s
    JOIN inventory_features i
    ON s.product_id = i.product_id AND s.date = i.date
)
SELECT
    product_id,
    ROUND(AVG(daily_quantity_sold) / NULLIF(AVG(stock_on_hand),0),2) AS demand_pressure_ratio,
    CASE
        WHEN AVG(daily_quantity_sold) / NULLIF(AVG(stock_on_hand),0) > 1 THEN 'High'
        ELSE 'Normal'
    END AS demand_pressure_level
FROM daily_demand
GROUP BY product_id;

-- =========================================
-- 3. Inventory Status View
-- =========================================
DROP VIEW IF EXISTS inventory_status_view;

CREATE VIEW inventory_status_view AS
SELECT
    product_id,
    stock_on_hand,
    reorder_point,
    CASE 
        WHEN stock_on_hand < reorder_point THEN 'Low'
        ELSE 'Safe'
    END AS inventory_status,
    ROUND(stock_ratio,2) AS stock_ratio
FROM inventory_features;

-- =========================================
-- 4. Daily Product Sales View (جديد للـ Short-Term Forecast)
-- =========================================
DROP VIEW IF EXISTS daily_product_sales_view;

CREATE VIEW daily_product_sales_view AS
SELECT
    product_id,
    DATE(date) AS date,        -- تم التصحيح: العمود الموجود فعلياً في CSV هو date
    SUM(quantity) AS quantity_sold
FROM sales_clean
GROUP BY product_id, DATE(date);
