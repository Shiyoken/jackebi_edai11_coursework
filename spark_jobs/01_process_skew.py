"""
01_skew_experiment.py
---------------------
Process the skewed dataset with Spark.
Compares three approaches to handle data skew:
  - Naive   : Plain groupBy, one task gets ~80% of data (HCMC)
  - Salting : Spread hot keys across N buckets, two-phase aggregation
  - AQE     : Spark 3.x adaptive query execution, auto-splits skewed partitions
"""
import sys
import time
import argparse
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

def timed(label, action):
    start = time.time()
    result = action()
    elapsed = time.time() - start 
    print(f" [{label}] {elapsed:.2f}s")
    return result 

def make_spark(app, *, aqe=False):
    builder = (
        SparkSession.builder
        .appName(app)
        # S3A / MinIO configuration
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
        .config("spark.hadoop.fs.s3a.access.key", "admin")
        .config("spark.hadoop.fs.s3a.secret.key", "password123")
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        # Delta Lake catalog
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        # Skew benchmark configs
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.sql.adaptive.enabled",          str(aqe).lower())
        .config("spark.sql.adaptive.skewJoin.enabled", str(aqe).lower())
        .config("spark.sql.adaptive.coalescePartitions.enabled", str(aqe).lower())
        .config("spark.sql.debug.maxToStringFields",   "100")
    )
    if aqe:
        builder = (
            builder
            .config("spark.sql.adaptive.skewJoin.skewedPartitionFactor", "2.0")
            .config("spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes", "1048576")
        )
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark

# Approach 1 — Naive/Baseline: Suffers from Skew
def run_naive(spark):
    orders = spark.read.parquet("s3a://raw-landing/orders/")
    return (
        orders
        .groupBy("shipping_city", "status")
        .agg(
            F.count("order_id").alias("num_orders"),
            F.sum("total_amount").alias("total_sales"),
            F.countDistinct("customer_id").alias("unique_customers"),
        )
        .orderBy(F.desc("num_orders"))
    )

# Approach 2 — Salting 
SALT = 8
def run_salting(spark):
    orders = spark.read.parquet("s3a://raw-landing/orders/")
    salted_orders = orders.withColumn("salt", (F.rand() * SALT).cast("int"))

    partial = (
        salted_orders 
        .groupBy("shipping_city", "status", "salt")
        .agg(
            F.count("order_id").alias("num_orders"),
            F.sum("total_amount").alias("total_sales"),
            F.approx_count_distinct("customer_id").alias("unique_customers"),
        )
    )
    return (
        partial
        .groupBy("shipping_city", "status")
        .agg(
            F.sum("num_orders").alias("num_orders"),
            F.sum("total_sales").alias("total_sales"),
            F.sum("unique_customers").alias("unique_customers"),
        )
        .orderBy(F.desc("num_orders"))
    )

# Approach 3 - AQE
def run_aqe(spark):
    orders = spark.read.parquet("s3a://raw-landing/orders/")
    return (
        orders
        .groupBy("shipping_city", "status")
        .agg(
            F.count("order_id").alias("num_orders"),
            F.sum("total_amount").alias("total_sales"),
            F.countDistinct("customer_id").alias("unique_customers"),
        )
        .orderBy(F.desc("num_orders"))
    )

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Data Skew Benchmark Runner")
    parser.add_argument("--scenario", choices=["baseline", "salting", "aqe", "all"], default="all",
                        help="Choose scenario to run (baseline, salting, aqe, or all)")
    parser.add_argument("--pause", type=int, default=180,
                        help="Seconds to pause keeping Spark UI (localhost:4040) alive for screenshots (default 180s)")
    args = parser.parse_args()

    print(f"\n==== DATA SKEW BENCHMARK EXPERIMENT [SCENARIO: {args.scenario.upper()}] ====")

    if args.scenario in ["baseline", "all"]:
        print("\n--- Running Baseline (No AQE, Plain GroupBy) ---")
        spark = make_spark("skew_baseline", aqe=False)
        timed("Naive Baseline", lambda: run_naive(spark).collect())
        print(f"\n[INFO] Spark UI is live at http://localhost:4040")
        print(f"[INFO] Sleeping for {args.pause}s so you can capture 'spark_ui_skew_baseline.png'...")
        time.sleep(args.pause)
        spark.stop()

    elif args.scenario == "salting":
        print("\n--- Running Salting Optimization ---")
        spark = make_spark("skew_salting", aqe=False)
        timed("Key Salting", lambda: run_salting(spark).collect())
        print(f"\n[INFO] Spark UI is live at http://localhost:4040")
        print(f"[INFO] Sleeping for {args.pause}s...")
        time.sleep(args.pause)
        spark.stop()

    elif args.scenario == "aqe":
        print("\n--- Running AQE Optimization ---")
        spark = make_spark("skew_aqe", aqe=True)
        timed("AQE Optimization", lambda: run_aqe(spark).collect())
        print(f"\n[INFO] Spark UI is live at http://localhost:4040")
        print(f"[INFO] Sleeping for {args.pause}s so you can capture 'spark_ui_aqe_plan.png'...")
        time.sleep(args.pause)
        spark.stop()

    print("\nProgram finished successfully.")
