import argparse
import os
import sys
import time
import traceback

from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, LongType, StringType

GOLD_BASE = "s3a://lakehouse-gold"
FEATURE_TABLE = "feat_customer_orders_90d"

# Anchor evaluation timestamp right after offline dataset window (2026-07-01 00:00:00 UTC)
FEATURE_ANCHOR_TS = "2026-07-01 00:00:00"


def create_spark_session():
    spark = (
        SparkSession.builder
        .appName("DP3-Gold-To-Features")
        .config("spark.hadoop.fs.s3a.endpoint", os.getenv("MINIO_ENDPOINT", "http://minio:9000"))
        .config("spark.hadoop.fs.s3a.access.key", os.getenv("MINIO_ACCESS_KEY", "admin"))
        .config("spark.hadoop.fs.s3a.secret.key", os.getenv("MINIO_SECRET_KEY", "password123"))
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark


def pg_settings():
    return {
        "url": os.getenv("PG_JDBC_URL", "jdbc:postgresql://postgres:5432/datawarehouse"),
        "user": os.getenv("PG_USER", "food_delivery_user"),
        "password": os.getenv("PG_PASSWORD", "food_delivery_password"),
    }
# Feature Table Computation

def compute_customer_features(spark):
    print("\n==================================================")
    print(f"COMPUTING OFFLINE FEATURE TABLE: {FEATURE_TABLE}")
    print("==================================================")

    # 1. Load Gold Tables
    fact_orders = spark.read.format("delta").load(f"{GOLD_BASE}/fact_orders")
    fact_sales  = spark.read.format("delta").load(f"{GOLD_BASE}/fact_sales")
    dim_status  = spark.read.format("delta").load(f"{GOLD_BASE}/dim_order_status")
    dim_prod    = spark.read.format("delta").load(f"{GOLD_BASE}/dim_product").filter(F.col("product_key") != -1)
    dim_cust    = spark.read.format("delta").load(f"{GOLD_BASE}/dim_customer").filter(F.col("customer_key") != -1)

    anchor_ts    = F.to_timestamp(F.lit(FEATURE_ANCHOR_TS))
    anchor_date  = F.to_date(anchor_ts)
    date_30d_ago = F.date_sub(anchor_date, 30)
    date_90d_ago = F.date_sub(anchor_date, 90)

    # Resolve order date: use order_ts if present, else parse integer date_key (e.g. 20260515)
    if "order_ts" in fact_orders.columns:
        order_date_expr = F.to_date(F.col("order_ts"))
    else:
        order_date_expr = F.to_date(F.col("date_key").cast("string"), "yyyyMMdd")

    dated_orders = fact_orders.withColumn("order_date", order_date_expr)

    # Sanity Check 1: Check anchor against ALL orders BEFORE any date filter
    max_order_d = dated_orders.agg(F.max("order_date")).first()[0]
    print(f"  Max order date in dataset: {max_order_d} | Feature anchor: {FEATURE_ANCHOR_TS[:10]}")
    if max_order_d and str(max_order_d) >= FEATURE_ANCHOR_TS[:10]:
        print(f" WARNING: Orders exist on or after the anchor date ({FEATURE_ANCHOR_TS[:10]}) and are excluded!")

    # Map SCD2 customer_key back to natural customer_id
    key_to_customer = dim_cust.select("customer_key", "customer_id").distinct()

    # Filter valid orders strictly BEFORE anchor_date (zero point-in-time leakage)
    valid_orders = (
        dated_orders
        .join(dim_status.select("order_status_key", "order_status", "counts_as_sale"), on="order_status_key", how="inner")
        .filter((F.col("counts_as_sale") == True) & (~F.col("order_status").isin("CANCELLED", "RETURNED")))
        .join(key_to_customer, on="customer_key", how="inner")
        .filter(F.col("order_date") < anchor_date)
        .cache()
    )
    # 2. RFM Aggregation by natural customer_id
    rfm_agg = (
        valid_orders
        .groupBy("customer_id")
        .agg(
            F.sum(F.when(F.col("order_date") >= date_30d_ago, 1).otherwise(0)).cast(IntegerType()).alias("orders_count_30d"),
            F.sum(F.when(F.col("order_date") >= date_90d_ago, 1).otherwise(0)).cast(IntegerType()).alias("orders_count_90d"),
            F.sum(F.when(F.col("order_date") >= date_90d_ago, F.col("total_amount")).otherwise(0)).cast(LongType()).alias("total_spend_90d"),
            F.datediff(anchor_date, F.max("order_date")).cast(IntegerType()).alias("days_since_last_order")
        )
        .withColumn(
            "avg_order_value_90d",
            F.when(
                F.col("orders_count_90d") > 0,
                (F.col("total_spend_90d") / F.col("orders_count_90d")).cast(LongType())
            ).otherwise(F.lit(0).cast(LongType()))
        )
    )

    # 3. Favorite Category (Top purchased category per customer in last 90 days)
    sales_by_cat = (
        fact_sales
        .join(dim_prod.select("product_key", "category"), on="product_key", how="inner")
        .join(valid_orders.select("order_id", "customer_id", "order_date"), on="order_id", how="inner")
        .filter(F.col("order_date") >= date_90d_ago)
        .groupBy("customer_id", "category")
        .agg(F.count("*").alias("cat_purchases"))
    )
    cat_window = Window.partitionBy("customer_id").orderBy(F.col("cat_purchases").desc(), F.col("category"))
    fav_cat = (
        sales_by_cat
        .withColumn("rank", F.row_number().over(cat_window))
        .filter(F.col("rank") == 1)
        .select("customer_id", F.col("category").alias("favorite_category"))
    )
    # 4. Master customer list (all current active customer_ids)
    cust_base = dim_cust.filter(F.col("is_current") == True).select("customer_id").distinct()

    feat_df = (
        cust_base
        .join(rfm_agg, on="customer_id", how="left")
        .join(fav_cat, on="customer_id", how="left")
        .withColumn("event_timestamp", anchor_ts)
        .withColumn("created_ts", F.current_timestamp())
        # Impute defaults for inactive / 0-order customers
        .withColumn("orders_count_30d", F.coalesce(F.col("orders_count_30d"), F.lit(0).cast(IntegerType())))
        .withColumn("orders_count_90d", F.coalesce(F.col("orders_count_90d"), F.lit(0).cast(IntegerType())))
        .withColumn("total_spend_90d", F.coalesce(F.col("total_spend_90d"), F.lit(0).cast(LongType())))
        .withColumn("avg_order_value_90d", F.coalesce(F.col("avg_order_value_90d"), F.lit(0).cast(LongType())))
        .withColumn("days_since_last_order", F.coalesce(F.col("days_since_last_order"), F.lit(999).cast(IntegerType())))
        .withColumn("favorite_category", F.coalesce(F.col("favorite_category"), F.lit("NONE").cast(StringType())))
        .select(
            "customer_id",
            "event_timestamp",
            "created_ts",
            "orders_count_30d",
            "orders_count_90d",
            "total_spend_90d",
            "avg_order_value_90d",
            "days_since_last_order",
            "favorite_category"
        )
    )

    # 5. Write to Lakehouse Gold (Single Source of Truth)
    target_path = f"{GOLD_BASE}/{FEATURE_TABLE}"
    (
        feat_df.write
        .format("delta")
        .option("truncate", "true")
        .option("cascadeTruncate", "true")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .save(target_path)
    )

    # Unpersist cached orders DataFrame
    valid_orders.unpersist()

    # Read back saved snapshot to ensure identical created_ts across stores
    gold_df = spark.read.format("delta").load(target_path)
    gold_count = gold_df.count()
    print(f"  OK   Built Lakehouse {FEATURE_TABLE} | Rows: {gold_count:>8,}")

    # 6. Sync Identical Snapshot to PostgreSQL DWH
    pg = pg_settings()
    (
        gold_df.write
        .format("jdbc")
        .option("url", pg["url"])
        .option("dbtable", FEATURE_TABLE)
        .option("user", pg["user"])
        .option("password", pg["password"])
        .option("driver", "org.postgresql.Driver")
        .option("batchsize", 10000)
        .mode("overwrite")
        .save()
    )
    # Verify PostgreSQL row count
    pg_count = (
        spark.read
        .format("jdbc")
        .option("url", pg["url"])
        .option("query", f"SELECT count(*) AS c FROM {FEATURE_TABLE}")
        .option("user", pg["user"])
        .option("password", pg["password"])
        .option("driver", "org.postgresql.Driver")
        .load()
        .first()["c"]
    )

    if pg_count != gold_count:
        raise RuntimeError(f"Row mismatch: Postgres has {pg_count:,} rows, Delta has {gold_count:,}!")

    print(f" OK Synced to PostgreSQL DWH | Verified Rows: {pg_count:>8,}")
    return gold_count

# DP3 Quality Gate Validation

def validate_feature_layer(spark):
    print("\n==================================================")
    print("RUNNING DP3 QUALITY VALIDATION GATE")
    print("==================================================")

    feat_path = f"{GOLD_BASE}/{FEATURE_TABLE}"
    df = spark.read.format("delta").load(feat_path)
    passed = True

    # Check 1: Primary Key Uniqueness on (customer_id, event_timestamp)
    total_rows = df.count()
    pk_count = df.select("customer_id", "event_timestamp").distinct().count()
    if total_rows == pk_count:
        print(f" PASSED PK Check: {FEATURE_TABLE:<26} | Rows: {total_rows:>7,} | PK Unique: 100%")
    else:
        print(f" FAILED PK Check: {FEATURE_TABLE} has {total_rows - pk_count} duplicate primary keys!")
        passed = False

    # Check 2: Customer Completeness (Every unique current customer represented)
    n_unique_customers = (
        spark.read.format("delta")
        .load(f"{GOLD_BASE}/dim_customer")
        .filter((F.col("customer_key") != -1) & (F.col("is_current") == True))
        .select("customer_id")
        .distinct()
        .count()
    )
    if total_rows == n_unique_customers:
        print(f"  PASSED Completeness: {total_rows:,} feature rows == {n_unique_customers:,} active customers.")
    else:
        print(f"  FAILED Completeness: {total_rows:,} feature rows vs {n_unique_customers:,} active customers!")
        passed = False

    # Check 3: Zero-Null Check across ALL 9 feature columns
    feature_cols = [
        "customer_id", "event_timestamp", "created_ts", "orders_count_30d",
        "orders_count_90d", "total_spend_90d", "avg_order_value_90d",
        "days_since_last_order", "favorite_category"
    ]
    null_counts = df.agg(*[F.sum(F.col(c).isNull().cast("int")).alias(c) for c in feature_cols]).first().asDict()
    bad_nulls = {k: v for k, v in null_counts.items() if v > 0}
    if not bad_nulls:
        print(f"  PASSED Null Check: 0 null values across all {len(feature_cols)} feature columns.")
    else:
        print(f"  FAILED Null Check: Found null values: {bad_nulls}!")
        passed = False

    # Check 4: Mathematical Invariant: orders_30d <= orders_90d
    invariant_violations = df.filter(F.col("orders_count_30d") > F.col("orders_count_90d")).count()
    if invariant_violations == 0:
        print(f"  PASSED Invariant Check: orders_count_30d <= orders_count_90d for 100% of rows.")
    else:
        print(f"  FAILED Invariant Check: {invariant_violations} rows violate orders_30d <= orders_90d!")
        passed = False

    # Check 5: Zero-order Consistency (Spend = 0 if Orders = 0)
    bad_spend = df.filter((F.col("orders_count_90d") == 0) & (F.col("total_spend_90d") != 0)).count()
    if bad_spend == 0:
        print(f"  PASSED Consistency Check: 0 spend for all customers with 0 orders.")
    else:
        print(f"  FAILED Consistency Check: {bad_spend} customers have spend but 0 orders!")
        passed = False

    # Check 6: Category Lineage Check (orders_90d > 0 ==> favorite_category != 'NONE')
    bad_fav = df.filter((F.col("orders_count_90d") > 0) & (F.col("favorite_category") == "NONE")).count()
    if bad_fav == 0:
        print(f"  PASSED Category Lineage: Every customer with orders has a resolved favorite_category.")
    else:
        print(f"  FAILED Category Lineage: {bad_fav} customers have orders but 'NONE' category!")
        passed = False

    # Check 7: Monetary Non-negativity
    negative_spends = df.filter((F.col("total_spend_90d") < 0) | (F.col("avg_order_value_90d") < 0)).count()
    if negative_spends == 0:
        print(f"  PASSED Monetary Check: All spend and AOV values are non-negative.")
    else:
        print(f"  FAILED Monetary Check: {negative_spends} rows have negative monetary values!")
        passed = False

    # Check 8: PostgreSQL DWH Parity Check (validates both stores in validate stage)
    try:
        pg = pg_settings()
        pg_count = (
            spark.read
            .format("jdbc")
            .option("url", pg["url"])
            .option("query", f"SELECT count(*) AS c FROM {FEATURE_TABLE}")
            .option("user", pg["user"])
            .option("password", pg["password"])
            .option("driver", "org.postgresql.Driver")
            .load()
            .first()["c"]
        )
        if pg_count == total_rows:
            print(f"  PASSED DWH Parity: PostgreSQL table has {pg_count:,} rows (matches Delta 100%).")
        else:
            print(f"  FAILED DWH Parity: PostgreSQL has {pg_count:,} rows vs Delta {total_rows:,}!")
            passed = False
    except Exception as e:
        print(f" WARNING DWH Parity: Could not verify PostgreSQL table: {e}")

    print("==================================================")
    if passed:
        print("DP3 QUALITY GATE PASSED: Offline Feature Store 100% Healthy.")
    else:
        print("DP3 QUALITY GATE FAILED.")
    print("==================================================")
    return passed

def main():
    parser = argparse.ArgumentParser(description="DP3: Gold to Offline Feature Store Table")
    parser.add_argument("--stage", choices=["all", "ingest", "validate"], default="all")
    args = parser.parse_args()

    start_time = time.time()
    spark = None
    validation_passed = True

    try:
        spark = create_spark_session()
        if args.stage in ("all", "ingest"):
            compute_customer_features(spark)
        if args.stage in ("all", "validate"):
            validation_passed = validate_feature_layer(spark)

        elapsed = round(time.time() - start_time, 2)
        print(f"\nFeature processing ({args.stage}) finished in {elapsed}s.")
    except Exception as e:
        print(f"\nFATAL ERROR in DP3 Feature Pipeline: {type(e).__name__}: {e}")
        traceback.print_exc()
        validation_passed = False
    finally:
        if spark:
            spark.stop()
        if not validation_passed:
            sys.exit(1)
        sys.exit(0)


if __name__ == "__main__":
    main()