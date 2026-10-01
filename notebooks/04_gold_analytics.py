# Databricks notebook source
from pyspark.sql.functions import (
    col,
    count,
    countDistinct,
    sum as spark_sum,
    avg,
    max as spark_max,
    round as spark_round,
    coalesce,
    lit,
    to_date
)

CATALOG = "retail_lakehouse"

SILVER_CUSTOMERS = f"{CATALOG}.silver.customers"
SILVER_ORDERS = f"{CATALOG}.silver.orders"

GOLD_CUSTOMER_360 = f"{CATALOG}.gold.customer_360"
GOLD_SEGMENT = f"{CATALOG}.gold.segment_performance"
GOLD_DAILY = f"{CATALOG}.gold.daily_sales_performance"

customers_df = spark.table(SILVER_CUSTOMERS)
orders_df = spark.table(SILVER_ORDERS)

print("Gold analytics build started.")

# COMMAND ----------

customer_order_metrics_df = (
    orders_df
    .filter(col("is_valid_order") == True)
    .groupBy("customer_id")
    .agg(
        countDistinct("order_id").alias("total_orders"),
        spark_sum("quantity").alias("total_units"),
        spark_round(
            spark_sum("order_amount"), 2
        ).alias("total_revenue"),
        spark_round(
            avg("order_amount"), 2
        ).alias("avg_order_value"),
        spark_max("order_timestamp").alias("last_order_timestamp")
    )
)

customer_360_df = (
    customers_df.alias("c")
    .join(
        customer_order_metrics_df.alias("o"),
        col("c.customer_id") == col("o.customer_id"),
        "left"
    )
    .select(
        col("c.customer_id"),
        col("c.customer_name"),
        col("c.email"),
        col("c.country"),
        col("c.state"),
        col("c.segment"),
        col("c.signup_date"),
        col("c.loyalty_tier"),
        col("c.lifetime_value"),
        col("c.customer_status"),

        coalesce(col("o.total_orders"), lit(0)).alias("total_orders"),
        coalesce(col("o.total_units"), lit(0)).alias("total_units"),
        coalesce(col("o.total_revenue"), lit(0.0)).alias("total_revenue"),
        coalesce(col("o.avg_order_value"), lit(0.0)).alias("avg_order_value"),

        col("o.last_order_timestamp")
    )
)

display(customer_360_df)

# COMMAND ----------

segment_performance_df = (
    customer_360_df
    .groupBy("segment")
    .agg(
        countDistinct("customer_id").alias("unique_customers"),
        spark_sum("total_orders").alias("total_orders"),
        spark_sum("total_units").alias("units_sold"),
        spark_round(
            spark_sum("total_revenue"), 2
        ).alias("total_revenue"),
        spark_round(
            avg("avg_order_value"), 2
        ).alias("avg_order_value"),
        spark_round(
            avg("lifetime_value"), 2
        ).alias("avg_lifetime_value")
    )
    .orderBy(col("total_revenue").desc())
)

display(segment_performance_df)

# COMMAND ----------

daily_sales_performance_df = (
    orders_df
    .filter(col("is_valid_order") == True)
    .withColumn(
        "order_date",
        to_date(col("order_timestamp"))
    )
    .groupBy("order_date")
    .agg(
        countDistinct("order_id").alias("total_orders"),
        countDistinct("customer_id").alias("unique_customers"),
        spark_sum("quantity").alias("units_sold"),
        spark_round(
            spark_sum("order_amount"), 2
        ).alias("total_revenue"),
        spark_round(
            avg("order_amount"), 2
        ).alias("avg_order_value")
    )
    .orderBy("order_date")
)

display(daily_sales_performance_df)

# COMMAND ----------

(
    customer_360_df.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(GOLD_CUSTOMER_360)
)

(
    segment_performance_df.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(GOLD_SEGMENT)
)

(
    daily_sales_performance_df.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(GOLD_DAILY)
)

print("Gold tables refreshed successfully.")

# COMMAND ----------

customer_360_count = spark.table(GOLD_CUSTOMER_360).count()
segment_count = spark.table(GOLD_SEGMENT).count()
daily_sales_count = spark.table(GOLD_DAILY).count()

print(f"Customer 360: {customer_360_count}")
print(f"Segment Performance: {segment_count}")
print(f"Daily Sales Performance: {daily_sales_count}")

assert customer_360_count == 12, "Expected 12 Customer 360 rows."
assert segment_count == 3, "Expected 3 segment rows."
assert daily_sales_count == 14, "Expected 14 daily sales rows."

print("Gold analytics validation passed.")