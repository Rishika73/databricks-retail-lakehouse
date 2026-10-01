SELECT
    segment,
    unique_customers,
    total_orders,
    units_sold,
    total_revenue,
    avg_order_value,
    avg_lifetime_value
FROM retail_lakehouse.gold.segment_performance
ORDER BY total_revenue DESC;
