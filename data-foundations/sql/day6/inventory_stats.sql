-- ========================================
-- DAY 6: Inventory Statistics (SUM & AVG)
-- ========================================

-- إجمالي الكمية المباعة لكل منتج
SELECT Product, SUM(Quantity) AS Total_Quantity
FROM sales
GROUP BY Product
ORDER BY Total_Quantity DESC;

-- متوسط سعر الوحدة لكل منتج
SELECT Product, AVG(Unit_Price) AS Avg_Unit_Price
FROM sales
GROUP BY Product
ORDER BY Avg_Unit_Price DESC;

-- إجمالي قيمة المخزون لكل منتج (Quantity * Unit_Price)
SELECT Product, SUM(Quantity * Unit_Price) AS Total_Value
FROM sales
GROUP BY Product
ORDER BY Total_Value DESC;

-- متوسط قيمة الطلب لكل منتج
SELECT Product, AVG(Quantity * Unit_Price) AS Avg_Order_Value
FROM sales
GROUP BY Product
ORDER BY Avg_Order_Value DESC;
