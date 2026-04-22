--which products need to be restocked right now?
SELECT product_id,
product_name,
MIN(stock_remaining) AS current_stock,
ROUND((SUM(quantity) / 30.0) * 2, 0) AS reorder_threshold,
SUM(lost_revenue) AS lost_revenue_so_far,
CASE
WHEN MIN(stock_remaining) < ROUND((SUM(quantity) / 30.0) * 2, 0)
THEN 'REORDER NOW'
ELSE 'OK'
END AS alert_status
FROM sales_clean
GROUP BY product_id, product_name
ORDER BY alert_status DESC, lost_revenue_so_far DESC