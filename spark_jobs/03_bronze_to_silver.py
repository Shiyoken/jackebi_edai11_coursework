import argparse 
import os
import sys
import time
from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.types import LongType, IntegerType, TimestampType

# Create spark session
def create_spark_session():
    minio_endpoint = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
    minio_access_key = os.getenv("MINIO_ACCESS_KEY", "admin")
    minio_secret_key = os.getenv("MINIO_SECRET_KEY", "password123")

    spark = (
        SparkSession.builder 
        .appName("DP2-Bronze-To-Silver")
        .config("spark.hadoop.fs.s3a.endpoint", minio_endpoint)
        .config("spark.hadoop.fs.s3a.access.key", minio_access_key)
        .config("spark.hadoop.fs.s3a.secret.key", minio_secret_key)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark 

# Silver Transformation
# The order for every process: Trim/Cast -> Filter -> Dedupe
def process_customers(spark, bronze_base, silver_base):
    print("\n--- Processing stg_customers ---")
    df = spark.read.format("delta").load(f"{bronze_base}/raw_customers")
    bronze_count = df.count() 

    # Step 1: Trim String & Cast Timestamps
    cleaned_df = (
        df
        .withColumn("customer_id", F.trim(F.col("customer_id")))
        .withColumn("city", F.trim(F.col("city")))
        .withColumn("segment", F.upper(F.trim(F.col("segment"))))
        .withColumn("signup_ts", F.col("signup_ts").cast(TimestampType()))
        .withColumn("updated_ts", F.col("updated_ts").cast(TimestampType()))
    )
    if "signup_device" in cleaned_df.columns:
        cleaned_df = cleaned_df.withColumn("signup_device", F.upper(F.trim(F.col("signup_device"))))
    if "device_type" in cleaned_df.columns:
        cleaned_df = cleaned_df.withColumn("device_type", F.upper(F.trim(F.col("device_type"))))

    # Step 2: Filter anomalies (null primary keys)
    valid_df = cleaned_df.filter(F.col("customer_id").isNotNull() & F.col("updated_ts").isNotNull())

    # Step 3: Deterministic deduplication on (customer_id, updated_ts)
    window_spec = Window.partitionBy("customer_id", "updated_ts").orderBy(
        F.desc("_ingest_ts"), F.desc("_source_file")
    )
    stg_df = (
        valid_df
        .withColumn("row_num", F.row_number().over(window_spec))
        .filter(F.col("row_num") == 1)
        .drop("row_num")
        .withColumn("_processed_ts", F.current_timestamp())
    )
    (
        stg_df.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .save(f"{silver_base}/stg_customers")
    )
    silver_count = spark.read.format("delta").load(f"{silver_base}/stg_customers").count()
    print(f" Bronze: {bronze_count:,} -> Silver: {silver_count:,} rows")

def process_products(spark, bronze_base, silver_base):
    print("\n--- Processing stg_products ---")
    df = spark.read.format("delta").load(f"{bronze_base}/raw_products")
    bronze_count = df.count()

    # Step 1: Trim strings & Cast types FIRST
    cleaned_df = (
        df
        .withColumn("product_id", F.trim(F.col("product_id")))
        .withColumn("category", F.upper(F.trim(F.col("category"))))
        .withColumn("brand", F.trim(F.col("brand")))
        .withColumn("base_price", F.col("base_price").cast(LongType()))
        .withColumn("created_ts", F.col("created_ts").cast(TimestampType()))
    )

    # Step 2: Filter corrupt/negative prices SECOND
    valid_df = cleaned_df.filter(F.col("product_id").isNotNull() & (F.col("base_price") > 0))

    # Step 3: Deterministic deduplication on product_id THIRD
    window_spec = Window.partitionBy("product_id").orderBy(
        F.desc("created_ts"), F.desc("_ingest_ts"), F.desc("_source_file")
    )
    stg_df = (
        valid_df
        .withColumn("row_num", F.row_number().over(window_spec))
        .filter(F.col("row_num") == 1)
        .drop("row_num")
        .withColumn("_processed_ts", F.current_timestamp())
    )

    (
        stg_df.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .save(f"{silver_base}/stg_products")
    )
    silver_count = spark.read.format("delta").load(f"{silver_base}/stg_products").count()
    print(f" Bronze: {bronze_count:,} -> Silver: {silver_count:,} rows")

def process_orders(spark, bronze_base, silver_base):
    print("\n--- Processing stg_orders ---")
    df = spark.read.format("delta").load(f"{bronze_base}/raw_orders")
    bronze_count = df.count()

    valid_statuses = ["COMPLETED", "SHIPPED", "PROCESSING", "PAID", "CANCELLED", "RETURNED"]

    # Step 1: Trim strings & Cast types FIRST
    cleaned_df = (
        df
        .withColumn("order_id", F.trim(F.col("order_id")))
        .withColumn("customer_id", F.trim(F.col("customer_id")))
        .withColumn("status", F.upper(F.trim(F.col("status"))))
        .withColumn("shipping_city", F.trim(F.col("shipping_city")))
        .withColumn("shipping_fee", F.col("shipping_fee").cast(LongType()))
        .withColumn("total_amount", F.col("total_amount").cast(LongType()))
        .withColumn("order_ts", F.col("order_ts").cast(TimestampType()))
    )

    # Step 2: Filter anomalies SECOND (after status is uppercased & amounts cast)
    valid_df = (
        cleaned_df
        .filter(F.col("order_id").isNotNull())
        .filter(F.col("status").isin(valid_statuses))
        .filter((F.col("total_amount") >= 0) & (F.col("shipping_fee") >= 0))
    )

    # Step 3: Deterministic deduplication on order_id THIRD
    window_spec = Window.partitionBy("order_id").orderBy(
        F.desc("order_ts"), F.desc("_ingest_ts"), F.desc("_source_file")
    )
    stg_df = (
        valid_df
        .withColumn("row_num", F.row_number().over(window_spec))
        .filter(F.col("row_num") == 1)
        .drop("row_num")
        .withColumn("_processed_ts", F.current_timestamp())
    )

    (
        stg_df.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .partitionBy("order_date")
        .save(f"{silver_base}/stg_orders")
    )
    silver_count = spark.read.format("delta").load(f"{silver_base}/stg_orders").count()
    print(f" Bronze: {bronze_count:,} -> Silver: {silver_count:,} rows")

def process_order_items(spark, bronze_base, silver_base):
    print("\n--- Processing stg_order_items (Deduplication) ---")
    df = spark.read.format("delta").load(f"{bronze_base}/raw_order_items")
    bronze_count = df.count()

    cleaned_df = (
        df
        .withColumn("order_item_id", F.trim(F.col("order_item_id")))
        .withColumn("order_id", F.trim(F.col("order_id")))
        .withColumn("product_id", F.trim(F.col("product_id")))
        .withColumn("quantity", F.col("quantity").cast(IntegerType()))
        .withColumn("unit_price", F.col("unit_price").cast(LongType()))
        .withColumn("created_ts", F.col("created_ts").cast(TimestampType()))
    )
    if "discount_amount" in cleaned_df.columns:
        cleaned_df = cleaned_df.withColumn("discount_amount", F.col("discount_amount").cast(LongType()))

    # Step 2: Filter corrupt quantities/prices SECOND (so corrupt rows never win row_num == 1)
    valid_df = (
        cleaned_df
        .filter(F.col("order_item_id").isNotNull())
        .filter((F.col("quantity") > 0) & (F.col("unit_price") >= 0))
    )
    valid_count = valid_df.count()
    anomalies_dropped = bronze_count - valid_count
    # Step 3: Deterministic deduplication on order_item_id THIRD
    window_spec = Window.partitionBy("order_item_id").orderBy(
        F.desc("created_ts"), F.desc("_ingest_ts"), F.desc("_source_file")
    )
    stg_df = (
        valid_df
        .withColumn("row_num", F.row_number().over(window_spec))
        .filter(F.col("row_num") == 1)
        .drop("row_num")
        .withColumn("_processed_ts", F.current_timestamp())
    )

    (
        stg_df.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .partitionBy("order_date")
        .save(f"{silver_base}/stg_order_items")
    )
    silver_count = spark.read.format("delta").load(f"{silver_base}/stg_order_items").count()
    dupes_removed = valid_count - silver_count
    dupe_pct = round((dupes_removed / valid_count) * 100, 2)
    print(f" Bronze: {bronze_count:,} -> Valid: {valid_count:,} -> Silver: {silver_count:,} rows")
    print(f" Anomalies Dropped: {anomalies_dropped:,} rows")
    print(f" Duplicates Removed: {dupes_removed:,} ({dupe_pct}%)")


def process_payments(spark, bronze_base, silver_base):
    print("\n--- Processing stg_payments ---")
    df = spark.read.format("delta").load(f"{bronze_base}/raw_payments")
    bronze_count = df.count()

    valid_methods = ["VNPAY", "MOMO", "ZALOPAY", "CREDIT_CARD", "COD"]
    valid_statuses = ["SUCCESS", "FAILED", "REFUNDED"]

    # Step 1: Trim strings & Cast types FIRST
    cleaned_df = (
        df
        .withColumn("payment_id", F.trim(F.col("payment_id")))
        .withColumn("order_id", F.trim(F.col("order_id")))
        .withColumn("method", F.upper(F.trim(F.col("method"))))
        .withColumn("status", F.upper(F.trim(F.col("status"))))
        .withColumn("amount", F.col("amount").cast(LongType()))
        .withColumn("failure_reason", F.upper(F.trim(F.col("failure_reason"))))
        .withColumn("payment_ts", F.col("payment_ts").cast(TimestampType()))
        .withColumn("updated_ts", F.col("updated_ts").cast(TimestampType()))
    )

    # Step 2: Filter anomalies SECOND (enforcing valid_methods, valid_statuses & amount >= 0)
    valid_df = (
        cleaned_df
        .filter(F.col("payment_id").isNotNull())
        .filter(F.col("amount") >= 0)
        .filter(F.col("method").isin(valid_methods) & F.col("status").isin(valid_statuses))
    )

    # Step 3: Deterministic deduplication on payment_id THIRD
    window_spec = Window.partitionBy("payment_id").orderBy(
        F.desc("updated_ts"), F.desc("payment_ts"), F.desc("_ingest_ts"), F.desc("_source_file")
    )
    stg_df = (
        valid_df
        .withColumn("row_num", F.row_number().over(window_spec))
        .filter(F.col("row_num") == 1)
        .drop("row_num")
        .withColumn("_processed_ts", F.current_timestamp())
    )

    (
        stg_df.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .partitionBy("payment_date")
        .save(f"{silver_base}/stg_payments")
    )
    silver_count = spark.read.format("delta").load(f"{silver_base}/stg_payments").count()
    print(f"  Bronze: {bronze_count:,} -> Silver: {silver_count:,} rows")


# ------------------------------------------------------------------------------
# 3. Quality Validation Gate for Silver Layer
# ------------------------------------------------------------------------------
def validate_silver_layer(spark, silver_base):
    print("\n==================================================")
    print("RUNNING SILVER QUALITY VALIDATION GATE")
    print("==================================================")

    tables = {
        "stg_customers": ("customer_id", "updated_ts"),
        "stg_products": ("product_id", None),
        "stg_orders": ("order_id", None),
        "stg_order_items": ("order_item_id", None),
        "stg_payments": ("payment_id", None),
    }

    all_passed = True
    for tbl_name, (pk1, pk2) in tables.items():
        try:
            df = spark.read.format("delta").load(f"{silver_base}/{tbl_name}")
            total_rows = df.count()

            # Check primary key uniqueness
            if pk2:
                unique_keys = df.select(pk1, pk2).distinct().count()
            else:
                unique_keys = df.select(pk1).distinct().count()

            if total_rows == 0:
                print(f" FAILED: {tbl_name} is empty!")
                all_passed = False
            elif total_rows != unique_keys:
                diff = total_rows - unique_keys
                print(f" FAILED: {tbl_name} has {diff:,} duplicate keys!")
                all_passed = False
            else:
                print(f" PASSED: {tbl_name:<16} | Rows: {total_rows:>8,} | PK Unique: 100%")

        except Exception as e:
            print(f" ERROR reading {tbl_name}: {e}")
            all_passed = False

    print("==================================================")
    if all_passed:
        print(" SILVER QUALITY GATE PASSED: All tables cleaned & deduplicated.")
    else:
        print(" SILVER QUALITY GATE FAILED.")
    print("==================================================")
    return all_passed


# ------------------------------------------------------------------------------
# 4. Main Execution Flow
# ------------------------------------------------------------------------------
def main():
    parser = argparse.ArgumentParser(description="DP2 Part 1: Bronze to Silver Pipeline")
    parser.add_argument(
        "--stage",
        choices=["all", "ingest", "validate"],
        default="all",
        help="Pipeline stage to execute (default: all)"
    )
    args = parser.parse_args()

    start_time = time.time()
    spark = None
    validation_passed = True

    try:
        spark = create_spark_session()
        print("==================================================")
        print(f"STARTING BRONZE -> SILVER PIPELINE | STAGE: {args.stage.upper()}")
        print("==================================================")

        BRONZE_BASE = "s3a://lakehouse-bronze"
        SILVER_BASE = "s3a://lakehouse-silver"

        if args.stage in ("all", "ingest"):
            process_customers(spark, BRONZE_BASE, SILVER_BASE)
            process_products(spark, BRONZE_BASE, SILVER_BASE)
            process_orders(spark, BRONZE_BASE, SILVER_BASE)
            process_order_items(spark, BRONZE_BASE, SILVER_BASE)
            process_payments(spark, BRONZE_BASE, SILVER_BASE)

        if args.stage in ("all", "validate"):
            validation_passed = validate_silver_layer(spark, SILVER_BASE)

        elapsed = round(time.time() - start_time, 2)
        print(f"\n Silver processing ({args.stage}) finished in {elapsed}s.")

    except Exception as e:
        print(f"\n FATAL ERROR in Silver Pipeline: {type(e).__name__}: {e}")
        validation_passed = False

    finally:
        if spark:
            spark.stop()
        if not validation_passed:
            sys.exit(1)
        sys.exit(0)


if __name__ == "__main__":
    main()