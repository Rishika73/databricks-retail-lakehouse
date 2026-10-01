# Databricks notebook source
from pyspark.sql.functions import (
    col,
    trim,
    lower,
    initcap,
    upper,
    to_date,
    to_timestamp,
    current_timestamp,
    when,
    lit
)

CATALOG = "retail_lakehouse"

BRONZE_CUSTOMERS = f"{CATALOG}.bronze.customers_raw"
BRONZE_ORDERS = f"{CATALOG}.bronze.orders_raw"

SILVER_CUSTOMERS = f"{CATALOG}.silver.customers"
SILVER_ORDERS = f"{CATALOG}.silver.orders"
QUARANTINE_ORDERS = f"{CATALOG}.bronze.orders_quarantine"

print("Silver transformation started.")

# COMMAND ----------

bronze_customers_df = spark.table(BRONZE_CUSTOMERS)

silver_customers_df = (
    bronze_customers_df
    .dropDuplicates(["customer_id"])

    .withColumn("customer_id", trim(col("customer_id")))
    .withColumn("customer_name", trim(col("customer_name")))
    .withColumn("email", lower(trim(col("email"))))
    .withColumn("country", initcap(trim(col("country"))))
    .withColumn("state", trim(col("state")))
    .withColumn("segment", initcap(trim(col("segment"))))
    .withColumn("loyalty_tier", initcap(trim(col("loyalty_tier"))))
    .withColumn("signup_date", to_date(col("signup_date"), "yyyy-MM-dd"))

    .withColumn(
        "customer_status",
        when(col("is_active") == 1, lit("ACTIVE"))
        .otherwise(lit("INACTIVE"))
    )

    .withColumn("processed_timestamp", current_timestamp())
)

display(silver_customers_df)

# COMMAND ----------

bronze_orders_df = spark.table(BRONZE_ORDERS)

validated_orders_df = (
    bronze_orders_df

    .withColumn("order_id", trim(col("order_id")))
    .withColumn("customer_id", trim(col("customer_id")))
    .withColumn("product_id", trim(col("product_id")))
    .withColumn("order_timestamp", to_timestamp(col("order_timestamp")))
    .withColumn("order_status", upper(trim(col("order_status"))))
    .withColumn("payment_method", upper(trim(col("payment_method"))))

    .withColumn(
        "validation_error",
        when(col("order_id").isNull(), lit("Missing order_id"))
        .when(col("customer_id").isNull(), lit("Missing customer_id"))
        .when(col("quantity") <= 0, lit("Quantity must be greater than zero"))
        .when(col("order_amount") < 0, lit("Order amount cannot be negative"))
        .otherwise(lit(None))
    )

    .withColumn(
        "is_valid_order",
        col("validation_error").isNull()
    )

    .withColumn("processed_timestamp", current_timestamp())
)

display(validated_orders_df)

# COMMAND ----------

valid_orders_df = (
    validated_orders_df
    .filter(col("is_valid_order") == True)
    .drop("validation_error")
)

quarantined_orders_df = (
    validated_orders_df
    .filter(col("is_valid_order") == False)
)

print("Valid orders:", valid_orders_df.count())
print("Quarantined orders:", quarantined_orders_df.count())

# COMMAND ----------

(
    silver_customers_df.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(SILVER_CUSTOMERS)
)

print("Silver customers table refreshed.")

# COMMAND ----------

(
    valid_orders_df.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(SILVER_ORDERS)
)

print("Silver orders table refreshed.")

# COMMAND ----------

(
    quarantined_orders_df.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(QUARANTINE_ORDERS)
)

print("Order quarantine table refreshed.")

# COMMAND ----------

silver_customer_count = spark.table(SILVER_CUSTOMERS).count()
silver_order_count = spark.table(SILVER_ORDERS).count()
quarantine_count = spark.table(QUARANTINE_ORDERS).count()

print(f"Silver customers: {silver_customer_count}")
print(f"Silver valid orders: {silver_order_count}")
print(f"Quarantined orders: {quarantine_count}")

assert silver_customer_count == 12, "Expected 12 Silver customers."
assert silver_order_count == 14, "Expected 14 valid Silver orders."
assert quarantine_count == 3, "Expected 3 quarantined orders."

print("Silver transformation validation passed.")

# COMMAND ----------

