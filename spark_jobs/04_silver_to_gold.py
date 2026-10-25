import argparse
import os
import sys
import time
import traceback
from datetime import date, datetime
from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.types import (
    BooleanType,
    DateType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)
# 1. Spark Session Builder (Pinned to UTC)
def create_spark_session():
    minio_endpoint = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
    minio_access_key = os.getenv("MINIO_ACCESS_KEY", "admin")
    minio_secret_key = os.getenv("MINIO_SECRET_KEY", "password123")

    spark = (
        SparkSession.builder
        .appName("DP2-Silver-To-Gold")
        .config("spark.jars.packages", "org.postgresql:postgresql:42.6.0") # Download the driver here

        .config("spark.hadoop.fs.s3a.endpoint", minio_endpoint)
        .config("spark.hadoop.fs.s3a.access.key", minio_access_key)
        .config("spark.hadoop.fs.s3a.secret.key", minio_secret_key)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark

def write_gold_table(spark, df, gold_base, table_name, partition_col=None):
    target_path = f"{gold_base}/{table_name}"
    writer = (
        df.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
    )
    if partition_col:
        writer = writer.partitionBy(partition_col)
    writer.save(target_path)
    count = spark.read.format("delta").load(target_path).count()
    print(f"  ✅ Built {table_name:<22} | Rows: {count:>8,}")
    return count

# 2. Dimension Builders (Each includes a -1 'Unknown' member row)
def build_dim_date(spark, stg_orders, stg_payments, gold_base):
    print("\n--- Building Gold Dimensions ---")
    order_bounds = stg_orders.select(
        F.min("order_date").alias("min_d"),
        F.max("order_date").alias("max_d")
    )
    pay_bounds = stg_payments.select(
        F.min("payment_date").alias("min_d"),
        F.max("payment_date").alias("max_d")
    )
    bounds = order_bounds.unionAll(pay_bounds).agg(
        F.min("min_d").alias("start_date"),
        F.max("max_d").alias("end_date")
    ).collect()[0]

    start_date = bounds["start_date"] or date(2025, 1, 1)
    end_date = bounds["end_date"] or date(2026, 12, 31)

    date_df = (
        spark.sql(f"SELECT explode(sequence(DATE'{start_date}', DATE'{end_date}', INTERVAL 1 DAY)) AS full_date")
        .withColumn("date_key", F.date_format("full_date", "yyyyMMdd").cast(IntegerType()))
        .withColumn("year", F.year("full_date").cast(IntegerType()))
        .withColumn("quarter", F.quarter("full_date").cast(IntegerType()))
        .withColumn("month", F.month("full_date").cast(IntegerType()))
        .withColumn("month_name", F.date_format("full_date", "MMMM"))
        .withColumn("day", F.dayofmonth("full_date").cast(IntegerType()))
        .withColumn("day_of_week", F.dayofweek("full_date").cast(IntegerType()))
        .withColumn("day_name", F.date_format("full_date", "EEEE"))
        .withColumn("is_weekend", F.dayofweek("full_date").isin([1, 7]))
        .select(
            "date_key", "full_date", "year", "quarter", "month",
            "month_name", "day", "day_of_week", "day_name", "is_weekend"
        )
    )
    unknown_schema = StructType([
        StructField("date_key", IntegerType(), False),
        StructField("full_date", DateType(), True),
        StructField("year", IntegerType(), True),
        StructField("quarter", IntegerType(), True),
        StructField("month", IntegerType(), True),
        StructField("month_name", StringType(), True),
        StructField("day", IntegerType(), True),
        StructField("day_of_week", IntegerType(), True),
        StructField("day_name", StringType(), True),
        StructField("is_weekend", BooleanType(), True),
    ])
    unknown_row = spark.createDataFrame(
        [(-1, date(1970, 1, 1), 1970, 1, 1, "Unknown", 1, 1, "Unknown", False)],
        schema=unknown_schema
    )

    dim_date = unknown_row.unionByName(date_df)
    write_gold_table(spark, dim_date, gold_base, "dim_date")
    return spark.read.format("delta").load(f"{gold_base}/dim_date")


def build_dim_customer(spark, stg_customers, gold_base):
    df = stg_customers
    if "signup_device" not in df.columns and "device_type" in df.columns:
        df = df.withColumnRenamed("device_type", "signup_device")

    w_asc = Window.partitionBy("customer_id").orderBy(F.asc("updated_ts"))

    scd2_df = (
        df
        .withColumn("ver_num", F.row_number().over(w_asc))
        .withColumn("next_updated_ts", F.lead("updated_ts").over(w_asc))
        .withColumn(
            "effective_from",
            F.when(
                F.col("ver_num") == 1,
                F.least(F.col("signup_ts"), F.col("updated_ts"))
            ).otherwise(F.col("updated_ts"))
        )
        .withColumn(
            "effective_to",
            F.coalesce(
                F.col("next_updated_ts"),
                F.to_timestamp(F.lit("9999-12-31 23:59:59"))
            )
        )
        .withColumn("is_current", F.col("next_updated_ts").isNull())
        .withColumn("customer_key", F.xxhash64("customer_id", "effective_from").cast(LongType()))
        .select(
            "customer_key", "customer_id", "city", "segment",
            "signup_device", "signup_ts", "effective_from", "effective_to", "is_current"
        )
    )

    unknown_schema = StructType([
        StructField("customer_key", LongType(), False),
        StructField("customer_id", StringType(), True),
        StructField("city", StringType(), True),
        StructField("segment", StringType(), True),
        StructField("signup_device", StringType(), True),
        StructField("signup_ts", TimestampType(), True),
        StructField("effective_from", TimestampType(), True),
        StructField("effective_to", TimestampType(), True),
        StructField("is_current", BooleanType(), True),
    ])
    unknown_row = spark.createDataFrame(
        [(
            -1, "UNKNOWN", "Unknown", "UNKNOWN", "UNKNOWN",
            datetime(1970, 1, 1, 0, 0, 0),
            datetime(1970, 1, 1, 0, 0, 0),
            datetime(9999, 12, 31, 23, 59, 59),
            True
        )],
        schema=unknown_schema
    )
    dim_customer = unknown_row.unionByName(scd2_df)
    write_gold_table(spark, dim_customer, gold_base, "dim_customer")
    return spark.read.format("delta").load(f"{gold_base}/dim_customer")

def build_dim_product(spark, stg_products, gold_base):
    df = stg_products
    if "updated_ts" not in df.columns:
        df = df.withColumn("updated_ts", F.col("created_ts"))

    prod_df = (
        df
        .withColumn("product_key", F.xxhash64("product_id").cast(LongType()))
        .withColumn("is_active", F.col("is_active").cast(BooleanType()))
        .select(
            "product_key", "product_id", "category", "brand",
            "base_price", "is_active", "created_ts", "updated_ts"
        )
    )
    unknown_schema = StructType([
        StructField("product_key", LongType(), False),
        StructField("product_id", StringType(), True),
        StructField("category", StringType(), True),
        StructField("brand", StringType(), True),
        StructField("base_price", LongType(), True),
        StructField("is_active", BooleanType(), True),
        StructField("created_ts", TimestampType(), True),
        StructField("updated_ts", TimestampType(), True),
    ])
    unknown_row = spark.createDataFrame(
        [(
            -1, "UNKNOWN", "UNKNOWN", "Unknown", 0, False,
            datetime(1970, 1, 1, 0, 0, 0), datetime(1970, 1, 1, 0, 0, 0)
        )],
        schema=unknown_schema
    )

    dim_product = unknown_row.unionByName(prod_df)
    write_gold_table(spark, dim_product, gold_base, "dim_product")
    return spark.read.format("delta").load(f"{gold_base}/dim_product")


def build_dim_payment_method(spark, stg_payments, gold_base):
    w = Window.orderBy("method")
    method_df = (
        stg_payments
        .select("method")
        .distinct()
        .filter(F.col("method").isNotNull())
        .withColumn("method_key", F.row_number().over(w).cast(IntegerType()))
        .withColumn(
            "method_type",
            F.when(F.col("method").isin("MOMO", "ZALOPAY", "VNPAY"), F.lit("E_WALLET"))
             .when(F.col("method") == "CREDIT_CARD", F.lit("CARD"))
             .when(F.col("method") == "COD", F.lit("CASH"))
             .otherwise(F.lit("OTHER"))
        )
        .select("method_key", "method", "method_type")
    )

    unknown_schema = StructType([
        StructField("method_key", IntegerType(), False),
        StructField("method", StringType(), True),
        StructField("method_type", StringType(), True),
    ])
    unknown_row = spark.createDataFrame([(-1, "UNKNOWN", "UNKNOWN")], schema=unknown_schema)

    dim_payment_method = unknown_row.unionByName(method_df)
    write_gold_table(spark, dim_payment_method, gold_base, "dim_payment_method")
    return spark.read.format("delta").load(f"{gold_base}/dim_payment_method")


def build_dim_payment_status(spark, stg_payments, gold_base):
    w = Window.orderBy("payment_status", "failure_reason")
    status_df = (
        stg_payments
        .select(
            F.col("status").alias("payment_status"),
            F.coalesce(F.col("failure_reason"), F.lit("NONE")).alias("failure_reason")
        )
        .distinct()
        .withColumn("payment_status_key", F.row_number().over(w).cast(IntegerType()))
        .withColumn("is_successful", F.col("payment_status") == "SUCCESS")
        .select("payment_status_key", "payment_status", "failure_reason", "is_successful")
    )

    unknown_schema = StructType([
        StructField("payment_status_key", IntegerType(), False),
        StructField("payment_status", StringType(), True),
        StructField("failure_reason", StringType(), True),
        StructField("is_successful", BooleanType(), True),
    ])
    unknown_row = spark.createDataFrame([(-1, "UNKNOWN", "NONE", False)], schema=unknown_schema)

    dim_payment_status = unknown_row.unionByName(status_df)
    write_gold_table(spark, dim_payment_status, gold_base, "dim_payment_status")
    return spark.read.format("delta").load(f"{gold_base}/dim_payment_status")


def build_dim_order_status(spark, stg_orders, gold_base):
    w = Window.orderBy("order_status")
    ord_status_df = (
        stg_orders
        .select(F.col("status").alias("order_status"))
        .distinct()
        .filter(F.col("order_status").isNotNull())
        .withColumn("order_status_key", F.row_number().over(w).cast(IntegerType()))
        .withColumn("is_final", F.col("order_status").isin("COMPLETED", "CANCELLED", "RETURNED"))
        .withColumn("counts_as_sale", ~F.col("order_status").isin("CANCELLED", "RETURNED"))
        .select("order_status_key", "order_status", "is_final", "counts_as_sale")
    )

    unknown_schema = StructType([
        StructField("order_status_key", IntegerType(), False),
        StructField("order_status", StringType(), True),
        StructField("is_final", BooleanType(), True),
        StructField("counts_as_sale", BooleanType(), True),
    ])
    unknown_row = spark.createDataFrame([(-1, "UNKNOWN", False, False)], schema=unknown_schema)

    dim_order_status = unknown_row.unionByName(ord_status_df)
    write_gold_table(spark, dim_order_status, gold_base, "dim_order_status")
    return spark.read.format("delta").load(f"{gold_base}/dim_order_status")


def build_dim_shipping_location(spark, stg_orders, gold_base):
    north_cities = ["Hanoi", "Ha Noi", "Hai Phong", "Quang Ninh", "Bac Ninh", "Hai Duong", "Nam Dinh", "Thai Nguyen"]
    central_cities = ["Da Nang", "Hue", "Nha Trang", "Khanh Hoa", "Quang Nam", "Binh Dinh", "Nghe An", "Thanh Hoa"]
    south_cities = ["Ho Chi Minh City", "Can Tho", "Binh Duong", "Dong Nai", "Vung Tau", "Ba Ria - Vung Tau", "Long An"]

    w = Window.orderBy("city")
    loc_df = (
        stg_orders
        .select(F.col("shipping_city").alias("city"))
        .distinct()
        .filter(F.col("city").isNotNull())
        .withColumn("ship_location_key", F.row_number().over(w).cast(IntegerType()))
        .withColumn(
            "region",
            F.when(F.col("city").isin(north_cities), F.lit("North"))
             .when(F.col("city").isin(central_cities), F.lit("Central"))
             .when(F.col("city").isin(south_cities), F.lit("South"))
             .otherwise(F.lit("Other"))
        )
        .select("ship_location_key", "city", "region")
    )

    unknown_schema = StructType([
        StructField("ship_location_key", IntegerType(), False),
        StructField("city", StringType(), True),
        StructField("region", StringType(), True),
    ])
    unknown_row = spark.createDataFrame([(-1, "Unknown", "Unknown")], schema=unknown_schema)

    dim_shipping_location = unknown_row.unionByName(loc_df)
    write_gold_table(spark, dim_shipping_location, gold_base, "dim_shipping_location")
    return spark.read.format("delta").load(f"{gold_base}/dim_shipping_location")

# 3. Fact & OBT Builders
def build_facts_and_obt(
    spark,
    stg_orders,
    stg_order_items,
    stg_payments,
    dim_date,
    dim_customer,
    dim_product,
    dim_payment_method,
    dim_payment_status,
    dim_order_status,
    dim_shipping_location,
    gold_base,
):
    print("\n--- Building Gold Fact Tables & OBT ---")

    # Step A: Resolve SCD2 customer_key per order_id at order_ts (with real fallback to earliest version)
    cust_versions = dim_customer.filter(F.col("customer_key") != -1).select(
        "customer_key",
        "customer_id",
        F.col("city").alias("customer_city"),
        F.col("segment").alias("segment_at_order"),
        "signup_device",
        "effective_from",
        "effective_to",
    )

    w_earliest = Window.partitionBy("customer_id").orderBy(F.asc("effective_from"))
    cust_fallback = (
        cust_versions
        .withColumn("_rn", F.row_number().over(w_earliest))
        .filter(F.col("_rn") == 1)
        .select(
            "customer_id",
            F.col("customer_key").alias("fb_customer_key"),
            F.col("customer_city").alias("fb_customer_city"),
            F.col("segment_at_order").alias("fb_segment_at_order"),
            F.col("signup_device").alias("fb_signup_device"),
        )
    )

    loc_lookup = dim_shipping_location.filter(F.col("ship_location_key") != -1)
    ord_status_lookup = dim_order_status.filter(F.col("order_status_key") != -1)
    date_lookup = dim_date.filter(F.col("date_key") != -1).select("date_key", "full_date")
    prod_lookup = dim_product.filter(F.col("product_key") != -1).select(
        "product_key", "product_id", "category", "brand", "base_price", "is_active"
    )

    orders_enriched = (
        stg_orders.alias("o")
        .join(
            cust_versions.alias("c"),
            (F.col("o.customer_id") == F.col("c.customer_id"))
            & (F.col("o.order_ts") >= F.col("c.effective_from"))
            & (F.col("o.order_ts") < F.col("c.effective_to")),
            how="left",
        )
        .join(cust_fallback.alias("fb"), F.col("o.customer_id") == F.col("fb.customer_id"), how="left")
        .join(loc_lookup.alias("loc"), F.col("o.shipping_city") == F.col("loc.city"), how="left")
        .join(ord_status_lookup.alias("os"), F.col("o.status") == F.col("os.order_status"), how="left")
        .select(
            F.col("o.order_id"),
            F.col("o.customer_id"),
            F.col("o.order_ts"),
            F.col("o.order_date"),
            F.col("o.status").alias("order_status"),
            F.col("o.shipping_city"),
            F.col("o.shipping_fee"),
            F.col("o.total_amount"),
            F.coalesce(F.col("c.customer_key"), F.col("fb.fb_customer_key"), F.lit(-1)).cast(LongType()).alias("customer_key"),
            F.coalesce(F.col("c.customer_city"), F.col("fb.fb_customer_city"), F.lit("Unknown")).alias("customer_city"),
            F.coalesce(F.col("c.segment_at_order"), F.col("fb.fb_segment_at_order"), F.lit("UNKNOWN")).alias("segment_at_order"),
            F.coalesce(F.col("c.signup_device"), F.col("fb.fb_signup_device"), F.lit("UNKNOWN")).alias("signup_device"),
            F.coalesce(F.col("loc.ship_location_key"), F.lit(-1)).cast(IntegerType()).alias("ship_location_key"),
            F.coalesce(F.col("loc.region"), F.lit("Unknown")).alias("region"),
            F.coalesce(F.col("os.order_status_key"), F.lit(-1)).cast(IntegerType()).alias("order_status_key"),
            F.coalesce(F.col("os.counts_as_sale"), F.lit(False)).alias("counts_as_sale"),
        )
        .cache()
    )

    # 1. Build fact_sales
    fact_sales = (
        stg_order_items.alias("oi")
        .join(
            orders_enriched.select(
                "order_id", "customer_key", "ship_location_key", "order_status_key", "order_date"
            ).alias("oe"),
            on="order_id",
            how="left",
        )
        .join(prod_lookup.alias("p"), on="product_id", how="left")
        .join(
            date_lookup.alias("d"),
            F.coalesce(F.col("oi.order_date"), F.col("oe.order_date")) == F.col("d.full_date"),
            how="left",
        )
        .withColumn("line_amount", (F.col("oi.quantity") * F.col("oi.unit_price")).cast(LongType()))
        .select(
            F.col("oi.order_item_id"),
            F.col("oi.order_id"),
            F.coalesce(F.col("d.date_key"), F.lit(-1)).cast(IntegerType()).alias("date_key"),
            F.coalesce(F.col("oe.customer_key"), F.lit(-1)).cast(LongType()).alias("customer_key"),
            F.coalesce(F.col("p.product_key"), F.lit(-1)).cast(LongType()).alias("product_key"),
            F.coalesce(F.col("oe.ship_location_key"), F.lit(-1)).cast(IntegerType()).alias("ship_location_key"),
            F.coalesce(F.col("oe.order_status_key"), F.lit(-1)).cast(IntegerType()).alias("order_status_key"),
            F.col("oi.quantity").cast(IntegerType()),
            F.col("oi.unit_price").cast(LongType()),
            F.col("line_amount"),
        )
    )
    write_gold_table(spark, fact_sales, gold_base, "fact_sales")

    # 2. Build fact_orders 
    item_agg = (
        stg_order_items
        .groupBy("order_id")
        .agg(F.count("order_item_id").cast(IntegerType()).alias("item_count"))
    )

    fact_orders = (
        orders_enriched.alias("oe")
        .join(item_agg.alias("ia"), on="order_id", how="left")
        .join(date_lookup.alias("d"), F.col("oe.order_date") == F.col("d.full_date"), how="left")
        .withColumn("subtotal_amount", (F.col("oe.total_amount") - F.col("oe.shipping_fee")).cast(LongType()))
        .select(
            F.col("oe.order_id"),
            F.coalesce(F.col("d.date_key"), F.lit(-1)).cast(IntegerType()).alias("date_key"),
            F.col("oe.customer_key"),
            F.col("oe.ship_location_key"),
            F.col("oe.order_status_key"),
            F.coalesce(F.col("ia.item_count"), F.lit(0)).cast(IntegerType()).alias("item_count"),
            F.col("subtotal_amount"),
            F.col("oe.shipping_fee").cast(LongType()),
            F.col("oe.total_amount").cast(LongType()),
        )
    )
    write_gold_table(spark, fact_orders, gold_base, "fact_orders")

    # 3. Build fact_payments
    method_lookup = dim_payment_method.filter(F.col("method_key") != -1)
    pay_status_lookup = dim_payment_status.filter(F.col("payment_status_key") != -1)

    fact_payments = (
        stg_payments.alias("p")
        .join(orders_enriched.select("order_id", "customer_key").alias("oe"), on="order_id", how="left")
        .join(date_lookup.alias("d"), F.col("p.payment_date") == F.col("d.full_date"), how="left")
        .join(method_lookup.alias("m"), F.col("p.method") == F.col("m.method"), how="left")
        .join(
            pay_status_lookup.alias("ps"),
            (F.col("p.status") == F.col("ps.payment_status"))
            & (F.coalesce(F.col("p.failure_reason"), F.lit("NONE")) == F.col("ps.failure_reason")),
            how="left",
        )
        .select(
            F.col("p.payment_id"),
            F.col("p.order_id"),
            F.coalesce(F.col("d.date_key"), F.lit(-1)).cast(IntegerType()).alias("date_key"),
            F.coalesce(F.col("oe.customer_key"), F.lit(-1)).cast(LongType()).alias("customer_key"),
            F.coalesce(F.col("m.method_key"), F.lit(-1)).cast(IntegerType()).alias("method_key"),
            F.coalesce(F.col("ps.payment_status_key"), F.lit(-1)).cast(IntegerType()).alias("payment_status_key"),
            F.col("p.amount").cast(LongType()),
        )
    )
    write_gold_table(spark, fact_payments, gold_base, "fact_payments")

    # 4. Build obt_sales 
    w_pay = Window.partitionBy("order_id").orderBy(
        F.desc(F.col("status") == "SUCCESS"),
        F.desc("updated_ts"),
        F.desc("payment_ts"),
        F.desc("payment_id"),
    )
    pay_latest_method = (
        stg_payments
        .withColumn("_rn", F.row_number().over(w_pay))
        .filter(F.col("_rn") == 1)
        .select("order_id", F.col("method").alias("final_payment_method"))
    )
    pay_counts = (
        stg_payments
        .groupBy("order_id")
        .agg(
            F.count("payment_id").cast(IntegerType()).alias("payment_attempts"),
            F.max(F.when(F.col("status") == "FAILED", True).otherwise(False)).alias("had_failed_payment"),
        )
    )
    pay_summary = pay_counts.join(pay_latest_method, on="order_id", how="left")

    obt_sales = (
        stg_order_items.alias("oi")
        .join(orders_enriched.alias("oe"), on="order_id", how="left")
        .join(
            dim_date.filter(F.col("date_key") != -1).alias("d"),
            F.coalesce(F.col("oi.order_date"), F.col("oe.order_date")) == F.col("d.full_date"),
            how="left",
        )
        .join(prod_lookup.alias("p"), on="product_id", how="left")
        .join(pay_summary.alias("ps"), on="order_id", how="left")
        .withColumn("line_amount", (F.col("oi.quantity") * F.col("oi.unit_price")).cast(LongType()))
        .select(
            # Keys & measures
            F.col("oi.order_item_id"),
            F.col("oi.order_id"),
            F.col("oi.quantity").cast(IntegerType()),
            F.col("oi.unit_price").cast(LongType()),
            F.col("line_amount"),
            # Date
            F.coalesce(F.col("oi.order_date"), F.col("oe.order_date")).alias("order_date"),
            F.col("d.year"),
            F.col("d.quarter"),
            F.col("d.month"),
            F.col("d.day_name"),
            F.col("d.is_weekend"),
            # Customer at order time
            F.col("oe.customer_id"),
            F.col("oe.customer_city"),
            F.col("oe.segment_at_order"),
            F.col("oe.signup_device"),
            # Product
            F.col("oi.product_id"),
            F.col("p.category"),
            F.col("p.brand"),
            F.col("p.base_price"),
            F.col("p.is_active"),
            # Order
            F.col("oe.order_status"),
            F.col("oe.counts_as_sale"),
            F.col("oe.shipping_city"),
            F.col("oe.region"),
            # Payment summary
            F.coalesce(F.col("ps.payment_attempts"), F.lit(0)).cast(IntegerType()).alias("payment_attempts"),
            F.coalesce(F.col("ps.final_payment_method"), F.lit("UNKNOWN")).alias("final_payment_method"),
            F.coalesce(F.col("ps.had_failed_payment"), F.lit(False)).alias("had_failed_payment"),
        )
    )
    write_gold_table(spark, obt_sales, gold_base, "obt_sales", partition_col="order_date")
    orders_enriched.unpersist()

# 4. Gold Quality Validation Gate
def validate_gold_layer(spark, gold_base):
    print("RUNNING GOLD QUALITY VALIDATION GATE")
    all_passed = True
    # 1. Check PK Uniqueness across all 11 Gold tables
    pk_map = {
        "dim_date": "date_key",
        "dim_customer": "customer_key",
        "dim_product": "product_key",
        "dim_payment_method": "method_key",
        "dim_payment_status": "payment_status_key",
        "dim_order_status": "order_status_key",
        "dim_shipping_location": "ship_location_key",
        "fact_sales": "order_item_id",
        "fact_orders": "order_id",
        "fact_payments": "payment_id",
        "obt_sales": "order_item_id",
    }

    loaded_dfs = {}
    for tbl, pk in pk_map.items():
        df = spark.read.format("delta").load(f"{gold_base}/{tbl}")
        loaded_dfs[tbl] = df
        total = df.count()
        uniq = df.select(pk).distinct().count()
        if total == 0 or total != uniq:
            print(f"FAILED PK Check: {tbl:<22} | Total: {total:,} vs Unique: {uniq:,}")
            all_passed = False
        else:
            print(f"PASSED PK Check: {tbl:<22} | Rows: {total:>8,} | PK Unique: 100%")

    # 2. Check SCD Type 2 Integrity on dim_customer (1 current row per customer, 0 gaps/overlaps)
    cust_real = loaded_dfs["dim_customer"].filter(F.col("customer_key") != -1)
    bad_current = (
        cust_real.groupBy("customer_id")
        .agg(F.sum(F.col("is_current").cast("int")).alias("curr_ct"))
        .filter(F.col("curr_ct") != 1)
        .count()
    )
    w_scd = Window.partitionBy("customer_id").orderBy("effective_from")
    broken_chain = (
        cust_real
        .withColumn("prev_to", F.lag("effective_to").over(w_scd))
        .filter(F.col("prev_to").isNotNull() & (F.col("effective_from") != F.col("prev_to")))
        .count()
    )
    if bad_current == 0 and broken_chain == 0:
        print("PASSED SCD2 Integrity: 1 current row per customer, 0 date gaps/overlaps.")
    else:
        print(f" FAILED SCD2 Integrity: bad_current={bad_current:,}, broken_chain={broken_chain:,}")
        all_passed = False

    # 3. Check Unknown (-1) Foreign Key Rates across Fact tables
    fk_cols = {
        "fact_sales": ["date_key", "customer_key", "product_key", "ship_location_key", "order_status_key"],
        "fact_orders": ["date_key", "customer_key", "ship_location_key", "order_status_key"],
        "fact_payments": ["date_key", "customer_key", "method_key", "payment_status_key"],
    }
    unknown_fk_total = 0
    for tbl, cols in fk_cols.items():
        df = loaded_dfs[tbl]
        agg_row = df.agg(
            F.count("*").alias("n"),
            *[F.sum((F.col(c) == -1).cast("int")).alias(c) for c in cols]
        ).collect()[0]
        for c in cols:
            unk_ct = agg_row[c] or 0
            if unk_ct > 0:
                pct = (unk_ct / agg_row["n"]) * 100
                print(f"FAILED FK Lookup: {tbl}.{c} has {unk_ct:,} (-1 Unknown) rows ({pct:.2f}%)")
                unknown_fk_total += unk_ct
                all_passed = False
    if unknown_fk_total == 0:
        print("PASSED FK Lookup Check: 0 Unknown (-1) foreign keys across all Fact tables.")

    # 4. Check OBT Row Count == fact_sales Row Count (Zero Fan-Out)
    fact_sales_ct = loaded_dfs["fact_sales"].count()
    obt_sales_ct = loaded_dfs["obt_sales"].count()
    if fact_sales_ct == obt_sales_ct:
        print(f"PASSED OBT Parity: obt_sales ({obt_sales_ct:,}) == fact_sales ({fact_sales_ct:,}) [Zero Fan-Out!]")
    else:
        print(f"FAILED OBT Parity: obt_sales ({obt_sales_ct:,}) != fact_sales ({fact_sales_ct:,})")
        all_passed = False

    # 5. Check Orphan Line Items & Order Subtotal Reconciliation
    fo = loaded_dfs["fact_orders"]
    fs = loaded_dfs["fact_sales"]
    sums = fs.groupBy("order_id").agg(F.sum("line_amount").cast(LongType()).alias("calc_subtotal"))

    orphan_items = sums.join(fo.select("order_id"), on="order_id", how="left_anti").count()
    no_items = fo.join(sums, on="order_id", how="left_anti").count()
    recon_mismatch = (
        fo.join(sums, on="order_id", how="inner")
        .filter(F.col("subtotal_amount") != F.col("calc_subtotal"))
        .count()
    )

    if orphan_items == 0:
        print(f"PASSED Order-Item Lineage: 0 orphan line items (Orders without items: {no_items:,}).")
    else:
        print(f"FAILED Order-Item Lineage: {orphan_items:,} line items reference non-existent orders!")
        all_passed = False

    if recon_mismatch == 0:
        print("PASSED Financial Reconciliation: SUM(line_amount) == subtotal_amount for 100% of orders.")
    else:
        print(f"INFO Financial Reconciliation: {recon_mismatch:,} orders have coupon/discount delta between line_amount sum and subtotal_amount.")

    print("==================================================")
    if all_passed:
        print("GOLD QUALITY GATE PASSED: Star Schema & OBT healthy.")
    else:
        print("GOLD QUALITY GATE FAILED.")
    print("==================================================")
    return all_passed

# 5. Main Execution Flow
def main():
    parser = argparse.ArgumentParser(description="DP2 Part 2: Silver to Gold Dimensional Pipeline")
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
        print(f"STARTING SILVER -> GOLD PIPELINE | STAGE: {args.stage.upper()}")
        print("==================================================")

        SILVER_BASE = "s3a://lakehouse-silver"
        GOLD_BASE = "s3a://lakehouse-gold"

        if args.stage in ("all", "ingest"):
            stg_customers = spark.read.format("delta").load(f"{SILVER_BASE}/stg_customers")
            stg_products = spark.read.format("delta").load(f"{SILVER_BASE}/stg_products")
            stg_orders = spark.read.format("delta").load(f"{SILVER_BASE}/stg_orders")
            stg_order_items = spark.read.format("delta").load(f"{SILVER_BASE}/stg_order_items")
            stg_payments = spark.read.format("delta").load(f"{SILVER_BASE}/stg_payments")

            dim_date = build_dim_date(spark, stg_orders, stg_payments, GOLD_BASE)
            dim_customer = build_dim_customer(spark, stg_customers, GOLD_BASE)
            dim_product = build_dim_product(spark, stg_products, GOLD_BASE)
            dim_payment_method = build_dim_payment_method(spark, stg_payments, GOLD_BASE)
            dim_payment_status = build_dim_payment_status(spark, stg_payments, GOLD_BASE)
            dim_order_status = build_dim_order_status(spark, stg_orders, GOLD_BASE)
            dim_shipping_location = build_dim_shipping_location(spark, stg_orders, GOLD_BASE)
            build_facts_and_obt(
                spark,
                stg_orders,
                stg_order_items,
                stg_payments,
                dim_date,
                dim_customer,
                dim_product,
                dim_payment_method,
                dim_payment_status,
                dim_order_status,
                dim_shipping_location,
                GOLD_BASE,
            )
        if args.stage in ("all", "validate"):
            validation_passed = validate_gold_layer(spark, GOLD_BASE)

        elapsed = round(time.time() - start_time, 2)
        print(f"\nGold processing ({args.stage}) finished in {elapsed}s.")
    except Exception as e:
        print(f"\nFATAL ERROR in Gold Pipeline: {type(e).__name__}: {e}")
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