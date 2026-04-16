-- which product category makes the most money?
SELECT category, SUM(revenue) AS total_revenue, COUNT(order_id) AS total_orders, 
FROM sales_clean
GROUP BY category
ORDER BY revenue DESC;