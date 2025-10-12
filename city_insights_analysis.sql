-- Top 10 SKUs across all cities
SELECT sku_name, brand, SUM(est_qty_sold) AS total_sold
FROM blinkit_city_insights
GROUP BY sku_name, brand
ORDER BY total_sold DESC
LIMIT 50;

-- Top 5 SKUs per city
SELECT city_name, sku_name, SUM(est_qty_sold) AS total_sold
FROM blinkit_city_insights
GROUP BY city_name, sku_name
ORDER BY city_name, total_sold DESC
LIMIT 50;

-- SKUs that run out of stock most frequently
SELECT sku_name, city_name,
       ROUND(AVG(1 - wt_osa_ls), 2) AS avg_out_of_stock_ratio,
       SUM(est_qty_sold) AS total_sold
FROM blinkit_city_insights
GROUP BY sku_name, city_name
HAVING SUM(est_qty_sold) > 0
ORDER BY avg_out_of_stock_ratio DESC
LIMIT 50;

-- top SKUs or brands contributing most to sales value
SELECT brand, SUM(est_sales_sp) AS total_revenue
FROM blinkit_city_insights
GROUP BY brand
ORDER BY total_revenue DESC
LIMIT 10;

--cities contribute most to total sales or have highest average basket value
SELECT city_name,
       SUM(est_sales_sp) AS total_sales,
       SUM(est_qty_sold) AS total_units,
       ROUND(SUM(est_sales_sp)/SUM(est_qty_sold),2) AS avg_unit_price
FROM blinkit_city_insights
GROUP BY city_name
ORDER BY total_sales DESC
LIMIT 10;

-- most popular categories/subcategories overall and in specific cities.
SELECT category_name, sub_category_name, SUM(est_sales_sp) AS total_sales
FROM blinkit_city_insights
GROUP BY category_name, sub_category_name
ORDER BY total_sales DESC;

-- Test whether deeper discounts actually increase sales.
SELECT ROUND(discount,2) AS discount_band,
       ROUND(AVG(est_qty_sold),2) AS avg_qty_sold
FROM blinkit_city_insights
GROUP BY ROUND(discount,2)
ORDER BY discount_band;