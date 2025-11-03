SELECT 
    c.customer_id,
    c.customer_name,
    SUM(o.sales) AS total_spent,
    COUNT(o.order_id) AS total_orders
FROM customers c
JOIN Orders o
    ON c.customer_id = o.customer_id
GROUP BY 
    c.customer_id, 
    c.customer_name
ORDER BY total_spent DESC;
