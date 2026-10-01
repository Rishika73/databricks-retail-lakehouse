SELECT
    ROUND(SUM(total_revenue), 2) AS total_revenue,
    SUM(total_orders) AS total_orders,
    ROUND(AVG(avg_order_value), 2) AS avg_order_value,
    SUM(unique_customers) AS customer_activity_count
FROM retail_lakehouse.gold.daily_sales_performance;
