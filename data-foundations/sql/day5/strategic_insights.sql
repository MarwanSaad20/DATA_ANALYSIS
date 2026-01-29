-- الطلبات ذات القيمة العالية
SELECT
    order_id,
    product,
    quantity * unit_price AS order_value
FROM sales
ORDER BY order_value DESC;

-- المنتجات ذات الطلب العالي ولكن قيمة أقل
SELECT
    product,
    COUNT(*) AS frequency,
    SUM(quantity * unit_price) AS total_sales
FROM sales
GROUP BY product
ORDER BY frequency DESC;
