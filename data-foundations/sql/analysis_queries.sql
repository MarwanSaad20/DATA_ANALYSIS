SELECT * FROM sales;

SELECT
    product,
    SUM(quantity * unit_price) AS total_sales
FROM sales
GROUP BY product
ORDER BY total_sales DESC;


SELECT
    category,
    AVG(unit_price) AS avg_price
FROM sales
GROUP BY category;

SELECT
    region,
    COUNT(*) AS total_orders
FROM sales
GROUP BY region;


SELECT *
FROM sales
WHERE quantity >= 5
ORDER BY quantity DESC;
