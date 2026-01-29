-- إجمالي المبيعات لكل منتج
SELECT
    product,
    SUM(quantity * unit_price) AS total_sales
FROM sales
GROUP BY product
ORDER BY total_sales DESC;

-- عدد الطلبات لكل منتج
SELECT
    product,
    COUNT(*) AS order_count
FROM sales
GROUP BY product
ORDER BY order_count DESC;
