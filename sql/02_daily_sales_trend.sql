SELECT
    order_date,
    total_orders,
    unique_customers,
    units_sold,
    total_revenue,
    avg_order_value
FROM retail_lakehouse.gold.daily_sales_performance
ORDER BY order_date;
