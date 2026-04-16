--which products earn the most?
SELECT product_id, product_name, category, SUM(quantity) AS total_ordered, SUM(revenue) AS total_revenue
FROM sales_clean
GROUP BY product_id, product_name, category
ORDER BY total_revenue DESC
LIMIT 10;