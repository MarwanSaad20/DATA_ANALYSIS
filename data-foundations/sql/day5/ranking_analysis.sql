-- أفضل 3 منتجات حسب المبيعات
SELECT
    product,
    SUM(quantity * unit_price) AS total_sales
FROM sales
GROUP BY product
ORDER BY total_sales DESC
LIMIT 3;
-- ترتيب المناطق حسب الأداء
SELECT
    region,
    SUM(quantity * unit_price) AS region_sales
FROM sales
GROUP BY region
ORDER BY region_sales DESC;
