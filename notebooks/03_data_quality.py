# Databricks notebook source
# Pipeline quality checks

checks = {}

# 1. Customer IDs must be unique
checks["unique_customer_ids"] = (
    spark.table("retail_lakehouse.silver.customers")
    .groupBy("customer_id")
    .count()
    .filter("count > 1")
    .count() == 0
)

# 2. Order IDs must be unique
checks["unique_order_ids"] = (
    spark.table("retail_lakehouse.silver.orders")
    .groupBy("order_id")
    .count()
    .filter("count > 1")
    .count() == 0
)

# 3. Customer IDs cannot be null
checks["no_null_customer_ids"] = (
    spark.table("retail_lakehouse.silver.customers")
    .filter("customer_id IS NULL")
    .count() == 0
)

# 4. Valid Silver orders cannot have negative amounts
checks["no_negative_order_amounts"] = (
    spark.table("retail_lakehouse.silver.orders")
    .filter("order_amount < 0")
    .count() == 0
)

# 5. Valid Silver orders must have positive quantities
checks["positive_order_quantity"] = (
    spark.table("retail_lakehouse.silver.orders")
    .filter("quantity <= 0")
    .count() == 0
)

# 6. Quarantine should contain exactly the rejected records
checks["expected_quarantine_count"] = (
    spark.table("retail_lakehouse.bronze.orders_quarantine")
    .count() == 3
)

# Print results
for check_name, passed in checks.items():
    status = "PASS" if passed else "FAIL"
    print(f"{check_name}: {status}")

# Fail the task if any check fails
assert all(checks.values()), "Pipeline data quality checks failed."

print("\nAll pipeline quality checks passed.")

# COMMAND ----------

