-- عدد السجلات الكلي
SELECT COUNT(*) AS total_rows FROM sales;

-- اكتمال البيانات لكل عمود
SELECT
    COUNT(order_id) AS order_id_complete,
    COUNT(product) AS product_complete,
    COUNT(quantity) AS quantity_complete,
    COUNT(unit_price) AS price_complete,
    COUNT(region) AS region_complete
FROM sales;

-- عدد المنتجات والمناطق (Cardinality)
SELECT
    COUNT(DISTINCT product) AS unique_products,
    COUNT(DISTINCT region) AS unique_regions
FROM sales;
