--which cities drive the most revenue and orders?
SELECT city,
COUNT(order_id) AS total_orders,
SUM(revenue) AS total_revenue
FROM sales_clean
WHERE city != 'Unknown'
GROUP BY city
ORDER BY total_revenue DESC;