import time
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.appName("spark_job").getOrCreate()

customers = spark.read.csv("file:///opt/spark-apps/data/customers.csv", header=True, inferSchema=True)
orders = spark.read.csv("file:///opt/spark-apps/data/orders.csv", header=True, inferSchema=True)

# 1-3: cleaning and filtering
customers = customers.filter(F.col("age") >= 18)
orders = orders.filter(F.col("status") == "Completed")
orders = orders.filter(F.col("quantity") >= 2)

# --- Spark Job 1: Revenue per country ---
result = (orders.join(customers, "customer_id")
          .withColumn("revenue", F.col("quantity") * F.col("price"))
          .groupBy("country")
          .agg(F.sum("revenue").alias("total_revenue"))
          .orderBy(F.col("total_revenue").desc()))
count_countries = result.count()  # Triggers Action 1

# --- Spark Job 2: Revenue per category ---
cat_result = (orders.groupBy("category")
              .agg(F.sum(F.col("quantity") * F.col("price")).alias("cat_revenue"))
              .orderBy(F.col("cat_revenue").desc()))
cat_result.take(5)  # Triggers Action 2

# --- Spark Job 3: Average customer age per country ---
age_result = customers.groupBy("country").agg(F.avg("age").alias("avg_age"))
age_result.count()  # Triggers Action 3

# --- Spark Job 4: Final print ---
result.show()  # Triggers Action 4

print("All 4 Jobs completed successfully! Sleeping so Prometheus can scrape metrics...")
# keep the driver alive so Prometheus can scrape port 4040
time.sleep(600)
spark.stop()
