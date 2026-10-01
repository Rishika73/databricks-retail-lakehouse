# Databricks notebook source
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    LongType,
    DoubleType
)

from pyspark.sql.functions import current_timestamp, lit
from delta.tables import DeltaTable

CATALOG = "retail_lakehouse"
BRONZE_SCHEMA = "bronze"

spark.sql(f"USE CATALOG {CATALOG}")

CUSTOMERS_TABLE = f"{CATALOG}.{BRONZE_SCHEMA}.customers_raw"
ORDERS_TABLE = f"{CATALOG}.{BRONZE_SCHEMA}.orders_raw"

print("Bronze ingestion started.")
print(f"Customers table: {CUSTOMERS_TABLE}")
print(f"Orders table: {ORDERS_TABLE}")

# COMMAND ----------

customer_schema = StructType([
    StructField("customer_id", StringType(), False),
    StructField("customer_name", StringType(), True),
    StructField("email", StringType(), True),
    StructField("country", StringType(), True),
    StructField("state", StringType(), True),
    StructField("segment", StringType(), True),
    StructField("signup_date", StringType(), True),
    StructField("loyalty_tier", StringType(), True),
    StructField("lifetime_value", DoubleType(), True),
    StructField("is_active", IntegerType(), True)
])

# Initial batch
customer_batch_001 = [
    ("C001", "Ava Johnson", "ava.johnson@example.com", "USA", "California", "Consumer", "2025-01-15", "Gold", 8420.50, 1),
    ("C002", "Liam Smith", "liam.smith@example.com", "USA", "Texas", "Corporate", "2025-02-03", "Silver", 4250.00, 1),
    ("C003", "Sophia Brown", "sophia.brown@example.com", "USA", "New York", "Consumer", "2025-02-18", "Platinum", 12750.75, 1),
    ("C004", "Noah Davis", "noah.davis@example.com", "USA", "Florida", "Small Business", "2025-03-09", "Bronze", 2100.25, 1),
    ("C005", "Emma Wilson", "emma.wilson@example.com", "Canada", "Ontario", "Corporate", "2025-03-21", "Gold", 9650.00, 1),
    ("C006", "Oliver Taylor", "oliver.taylor@example.com", "USA", "Illinois", "Consumer", "2025-04-07", "Silver", 5380.40, 1),
    ("C007", "Mia Anderson", "mia.anderson@example.com", "Canada", "British Columbia", "Consumer", "2025-04-19", "Gold", 7890.20, 1),
    ("C008", "Ethan Thomas", "ethan.thomas@example.com", "USA", "Washington", "Corporate", "2025-05-02", "Platinum", 15420.80, 1),
    ("C009", "Isabella Moore", "isabella.moore@example.com", "USA", "Arizona", "Small Business", "2025-05-14", "Bronze", 1850.60, 1),
    ("C010", "Lucas Martin", "lucas.martin@example.com", "USA", "Massachusetts", "Consumer", "2025-06-01", "Silver", 4925.30, 1)
]

# Incremental batch:
# C001 is an update; C011 and C012 are new customers.
customer_batch_002 = [
    ("C001", "Ava Johnson", "ava.johnson@example.com", "USA", "California", "Consumer", "2025-01-15", "Platinum", 10500.00, 1),
    ("C011", "Sophia Martinez", "sophia.martinez@example.com", "USA", "Texas", "Consumer", "2026-09-25", "Gold", 3200.00, 1),
    ("C012", "Daniel Wilson", "daniel.wilson@example.com", "USA", "Washington", "Small Business", "2026-09-27", "Silver", 5100.00, 1)
]

customers_001_df = (
    spark.createDataFrame(customer_batch_001, customer_schema)
    .withColumn("ingestion_timestamp", current_timestamp())
    .withColumn("source_system", lit("retail_crm"))
    .withColumn("batch_id", lit("customer_batch_001"))
)

customers_002_df = (
    spark.createDataFrame(customer_batch_002, customer_schema)
    .withColumn("ingestion_timestamp", current_timestamp())
    .withColumn("source_system", lit("retail_crm"))
    .withColumn("batch_id", lit("customer_batch_002"))
)

customer_source_df = customers_001_df.unionByName(customers_002_df)

# Keep the latest record from the incremental batch for duplicate customer IDs.
customer_source_df.createOrReplaceTempView("incoming_customers")

latest_customer_source_df = spark.sql("""
    SELECT *
    FROM (
        SELECT *,
               ROW_NUMBER() OVER (
                   PARTITION BY customer_id
                   ORDER BY batch_id DESC
               ) AS row_num
        FROM incoming_customers
    )
    WHERE row_num = 1
""").drop("row_num")

if not spark.catalog.tableExists(CUSTOMERS_TABLE):

    (
        latest_customer_source_df.write
        .format("delta")
        .mode("overwrite")
        .saveAsTable(CUSTOMERS_TABLE)
    )

    print("Created Bronze customers table.")

else:

    customers_delta = DeltaTable.forName(
        spark,
        CUSTOMERS_TABLE
    )

    (
        customers_delta.alias("target")
        .merge(
            latest_customer_source_df.alias("source"),
            "target.customer_id = source.customer_id"
        )
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )

    print("Bronze customers MERGE completed.")

# COMMAND ----------

order_schema = StructType([
    StructField("order_id", StringType(), False),
    StructField("customer_id", StringType(), True),
    StructField("product_id", StringType(), True),
    StructField("quantity", LongType(), True),
    StructField("order_amount", DoubleType(), True),
    StructField("order_timestamp", StringType(), True),
    StructField("order_status", StringType(), True),
    StructField("payment_method", StringType(), True)
])

order_batch_001 = [
    ("O1001", "C001", "P101", 2, 120.50, "2026-09-01 10:15:00", "completed", "credit_card"),
    ("O1002", "C002", "P103", 1, 75.00, "2026-09-02 11:30:00", "completed", "paypal"),
    ("O1003", "C003", "P102", 3, 210.75, "2026-09-03 14:10:00", "completed", "credit_card"),
    ("O1004", "C001", "P105", 1, 45.25, "2026-09-05 09:20:00", "returned", "credit_card"),
    ("O1005", "C004", "P104", 4, 320.00, "2026-09-06 16:45:00", "completed", "debit_card"),
    ("O1006", "C005", "P101", 2, 120.50, "2026-09-08 13:00:00", "completed", "paypal"),
    ("O1007", "C006", "P106", 1, 500.00, "2026-09-10 18:30:00", "completed", "credit_card"),
    ("O1008", "C007", "P103", 2, 150.00, "2026-09-12 12:15:00", "cancelled", "credit_card"),
    ("O1009", "C008", "P102", 1, 70.25, "2026-09-15 15:40:00", "completed", "debit_card"),
    ("O1010", "C003", "P105", 3, 135.75, "2026-09-18 17:10:00", "completed", "paypal"),
    ("O1011", "C009", "P104", 2, 160.00, "2026-09-20 10:05:00", "completed", "credit_card"),
    ("O1012", "C010", "P106", 1, 500.00, "2026-09-22 19:25:00", "completed", "debit_card")
]

# Includes two valid records and three intentionally bad records.
# The bad records will be quarantined in the Silver transformation notebook.
order_batch_002 = [
    ("O1013", "C011", "P102", 2, 180.00, "2026-09-25 10:30:00", "completed", "credit_card"),
    ("O1014", None, "P103", 1, 75.00, "2026-09-26 11:15:00", "completed", "paypal"),
    ("O1015", "C012", "P104", -2, 160.00, "2026-09-27 14:20:00", "completed", "debit_card"),
    ("O1016", "C005", "P101", 1, -120.50, "2026-09-28 09:45:00", "completed", "credit_card"),
    ("O1017", "C001", "P106", 1, 500.00, "2026-09-29 16:10:00", "completed", "paypal")
]

orders_001_df = (
    spark.createDataFrame(order_batch_001, order_schema)
    .withColumn("ingestion_timestamp", current_timestamp())
    .withColumn("source_system", lit("retail_pos"))
    .withColumn("batch_id", lit("order_batch_001"))
)

orders_002_df = (
    spark.createDataFrame(order_batch_002, order_schema)
    .withColumn("ingestion_timestamp", current_timestamp())
    .withColumn("source_system", lit("retail_pos"))
    .withColumn("batch_id", lit("order_batch_002"))
)

order_source_df = orders_001_df.unionByName(orders_002_df)

if not spark.catalog.tableExists(ORDERS_TABLE):

    (
        order_source_df.write
        .format("delta")
        .mode("overwrite")
        .saveAsTable(ORDERS_TABLE)
    )

    print("Created Bronze orders table.")

else:

    orders_delta = DeltaTable.forName(
        spark,
        ORDERS_TABLE
    )

    (
        orders_delta.alias("target")
        .merge(
            order_source_df.alias("source"),
            "target.order_id = source.order_id"
        )
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )

    print("Bronze orders MERGE completed.")

# COMMAND ----------

customer_count = spark.table(CUSTOMERS_TABLE).count()
order_count = spark.table(ORDERS_TABLE).count()

print("Bronze ingestion complete.")
print(f"Bronze customers: {customer_count}")
print(f"Bronze raw orders: {order_count}")

assert customer_count == 12, "Expected 12 Bronze customers."
assert order_count == 17, "Expected 17 raw Bronze orders."

print("Bronze ingestion validation passed.")