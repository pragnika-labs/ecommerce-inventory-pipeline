--which products lost the most revenue due to stockouts?
SELECT product_id, 
product_name, 
SUM(lost_sales) AS total_lost_units, 
SUM(lost_revenue) AS total_lost_revenue,
MIN(CASE WHEN stock_remaining = 0 THEN date END) AS first_stockout_date
FROM sales_clean
GROUP BY product_id, product_name
HAVING SUM(lost_revenue) > 0
ORDER BY total_lost_revenue DESC;