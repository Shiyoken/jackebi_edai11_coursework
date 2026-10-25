"""
05_storage_optimization.py
--------------------------------------------------------------------------------
Rubric Section G: Data Storage Optimization

  Step 1. Sync Gold Delta tables -> PostgreSQL (and check the row counts).
  Step 2. Lakehouse: OPTIMIZE (compaction) + ZORDER BY on obt_sales,
          compare file counts and query time before vs after.
  Step 3. PostgreSQL: B-tree index on fact_orders,
          compare EXPLAIN ANALYZE time before vs after.
--------------------------------------------------------------------------------
"""

import os
import re
import statistics
import sys
import time
import traceback

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


# ==============================================================================
# CONFIG
# ==============================================================================
GOLD_BASE = "s3a://lakehouse-gold"

GOLD_TABLES = [
    "dim_date", "dim_customer", "dim_product", "dim_payment_method",
    "dim_payment_status", "dim_order_status", "dim_shipping_location",
    "fact_orders", "fact_sales", "fact_payments", "obt_sales",
]

INDEX_NAME = "idx_fact_orders_cust_date"
LAKEHOUSE_RUNS = 5     # measured runs (one extra warm-up run is not counted)
PG_RUNS = 5

PG_USER = os.getenv("PG_USER", "food_delivery_user")
PG_PASSWORD = os.getenv("PG_PASSWORD", "food_delivery_password")
JDBC_OPTS = {
    "url": os.getenv("PG_JDBC_URL", "jdbc:postgresql://postgres:5432/datawarehouse"),
    "user": PG_USER,
    "password": PG_PASSWORD,
    "driver": "org.postgresql.Driver",
}


# ==============================================================================
# SMALL HELPERS
# ==============================================================================
def create_spark_session():
    spark = (
        SparkSession.builder
        .appName("DP2-Storage-Optimization")
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


def median_ms(fn, runs):
    """Run fn once to warm up (not counted), then return the median time of `runs` runs in ms."""
    fn()
    times = []
    for _ in range(runs):
        t0 = time.perf_counter()
        fn()
        times.append((time.perf_counter() - t0) * 1000)
    return round(statistics.median(times), 2)


def ratio(before, after):
    return round(before / after, 2) if before and after else None


def fmt(value, unit=""):
    return "n/a" if value is None else f"{value:,.2f}{unit}"


# ==============================================================================
# STEP 1: SYNC GOLD -> POSTGRESQL
# ==============================================================================
def sync_gold_to_postgres(spark):
    print("\n==================================================")
    print("STEP 1: SYNC GOLD TABLES TO POSTGRESQL")
    print("==================================================")

    bad_tables = []
    for table in GOLD_TABLES:
        t0 = time.time()
        df = spark.read.format("delta").load(f"{GOLD_BASE}/{table}")
        delta_rows = df.count()

        (df.write.format("jdbc")
            .options(**JDBC_OPTS)
            .option("dbtable", table)
            .option("batchsize", 10000)
            .option("truncate", "true")
            .option("cascadeTruncate", "true")
            .mode("overwrite")
            .save())

        pg_rows = (spark.read.format("jdbc")
                   .options(**JDBC_OPTS)
                   .option("query", f"SELECT count(*) AS c FROM {table}")
                   .load().first()["c"])

        status = "OK  " if pg_rows == delta_rows else "FAIL"
        if pg_rows != delta_rows:
            bad_tables.append(table)
        print(f"  {status} {table:<22} Delta={delta_rows:>9,}  Postgres={pg_rows:>9,}  ({time.time() - t0:.1f}s)")

    if bad_tables:
        raise RuntimeError(f"Row counts do not match for: {bad_tables}")


# ==============================================================================
# STEP 2: LAKEHOUSE  (OPTIMIZE + ZORDER)
# ==============================================================================
def table_files(spark, path):
    """Return (number of files, size in MB) of a Delta table."""
    row = spark.sql(f"DESCRIBE DETAIL delta.`{path}`").first()
    return row["numFiles"], round(row["sizeInBytes"] / 1e6, 2)


def run_lookups(spark, path, pairs):
    """The benchmark workload: one selective lookup per (customer_id, product_id) pair."""
    for p in pairs:
        (spark.read.format("delta").load(path)
            .filter((F.col("customer_id") == p["customer_id"]) &
                    (F.col("product_id") == p["product_id"]))
            .agg(F.count("*"), F.sum("line_amount"))
            .collect())   # collect() is what actually runs the query


def benchmark_lakehouse(spark):
    print("\n==================================================")
    print("STEP 2: LAKEHOUSE OPTIMIZE + ZORDER BENCHMARK")
    print("==================================================")

    path = f"{GOLD_BASE}/obt_sales"

    # If the table was optimized already, "before" is not a real baseline
    last_op = spark.sql(f"DESCRIBE HISTORY delta.`{path}` LIMIT 1").first()["operation"]
    if last_op == "OPTIMIZE":
        print("  WARNING: obt_sales is already optimized. Re-run 04_silver_to_gold.py first.")

    # 5 fixed key pairs (same order every run), so before and after use the same queries
    pairs = (spark.read.format("delta").load(path)
             .select("customer_id", "product_id").distinct()
             .orderBy(F.xxhash64("customer_id", "product_id"))
             .limit(5).collect())

    # ---- BEFORE ----
    files_before, mb_before = table_files(spark, path)
    time_before = median_ms(lambda: run_lookups(spark, path, pairs), LAKEHOUSE_RUNS)
    print(f"\n[A] BEFORE: {files_before} files, {mb_before} MB, 5 lookups take {time_before} ms")

    # ---- OPTIMIZE ----
    print("\n[B] OPTIMIZE ... ZORDER BY (customer_id, product_id)")
    t0 = time.time()
    result = spark.sql(f"OPTIMIZE delta.`{path}` ZORDER BY (customer_id, product_id)")
    m = result.select("metrics.numFilesAdded", "metrics.numFilesRemoved").first()
    print(f"    Done in {time.time() - t0:.1f}s | files removed: {m['numFilesRemoved']} | files added: {m['numFilesAdded']}")

    # ---- AFTER ----
    files_after, mb_after = table_files(spark, path)
    time_after = median_ms(lambda: run_lookups(spark, path, pairs), LAKEHOUSE_RUNS)
    print(f"\n[C] AFTER: {files_after} files, {mb_after} MB, 5 lookups take {time_after} ms")

    print("\n--- Lakehouse Summary ---")
    print(f"  Files   : {files_before} -> {files_after}")
    print(f"  Latency : {time_before} ms -> {time_after} ms  (speedup {fmt(ratio(time_before, time_after), 'x')})")


# ==============================================================================
# STEP 3: POSTGRESQL  (B-tree index + EXPLAIN ANALYZE)
# ==============================================================================
def pg_query(stmt, sql):
    """Run a query and return the first column of every row as a list of strings."""
    rs = stmt.executeQuery(sql)
    rows = []
    while rs.next():
        rows.append(rs.getString(1))
    rs.close()
    return rows


def explain_once(stmt, sql):
    """Return (plan lines, execution time in ms) for one EXPLAIN ANALYZE."""
    plan = pg_query(stmt, "EXPLAIN (ANALYZE, BUFFERS) " + sql)
    ms = None
    for line in plan:
        match = re.search(r"Execution Time:\s*([\d.]+)\s*ms", line)
        if match:
            ms = float(match.group(1))
    return plan, ms


def explain_median(stmt, sql, runs):
    """One warm-up run, then the median execution time of `runs` runs, plus the last plan."""
    explain_once(stmt, sql)
    times, plan = [], []
    for _ in range(runs):
        plan, ms = explain_once(stmt, sql)
        if ms is not None:
            times.append(ms)
    return (round(statistics.median(times), 3) if times else None), plan


def scan_type(plan):
    for kind in ("Index Only Scan", "Bitmap Index Scan", "Index Scan", "Seq Scan"):
        if any(kind in line for line in plan):
            return kind
    return "unknown"


def benchmark_postgres(spark):
    print("\n==================================================")
    print("STEP 3: POSTGRESQL B-TREE INDEX BENCHMARK")
    print("==================================================")

    # Plain JDBC connection, used only to send SQL text to Postgres
    spark._jvm.java.lang.Class.forName("org.postgresql.Driver")
    conn = spark._jvm.java.sql.DriverManager.getConnection(
        JDBC_OPTS["url"], PG_USER, PG_PASSWORD)

    try:
        stmt = conn.createStatement()
        stmt.execute(f"DROP INDEX IF EXISTS {INDEX_NAME}")      # clean baseline

        # Use real values from the data: latest month, and the busiest customer in it
        date_hi = int(pg_query(stmt, "SELECT max(date_key) FROM fact_orders WHERE date_key <> -1")[0])
        date_lo = date_hi // 100 * 100 + 1                       # first day of that month
        cust_key = int(pg_query(stmt, f"""
            SELECT customer_key FROM fact_orders
            WHERE customer_key <> -1 AND date_key BETWEEN {date_lo} AND {date_hi}
            GROUP BY customer_key
            ORDER BY count(*) DESC, customer_key
            LIMIT 1""")[0])

        query = f"""
            SELECT customer_key, count(*) AS total_orders, sum(total_amount) AS spend
            FROM fact_orders
            WHERE customer_key = {cust_key}
              AND date_key BETWEEN {date_lo} AND {date_hi}
            GROUP BY customer_key"""
        print(f"  Query: customer_key={cust_key}, date_key {date_lo}..{date_hi}")

        # ---- BEFORE ----
        stmt.execute("ANALYZE fact_orders")                      # refresh planner statistics
        time_before, plan_before = explain_median(stmt, query, PG_RUNS)
        print("\n[A] BEFORE index:")
        for line in plan_before:
            print(f"    {line}")

        # ---- CREATE INDEX ----
        t0 = time.time()
        stmt.execute(f"CREATE INDEX {INDEX_NAME} ON fact_orders (customer_key, date_key)")
        build_seconds = round(time.time() - t0, 2)
        stmt.execute("ANALYZE fact_orders")
        print(f"\n[B] Index {INDEX_NAME} built in {build_seconds}s")

        # ---- AFTER ----
        time_after, plan_after = explain_median(stmt, query, PG_RUNS)
        print("\n[C] AFTER index:")
        for line in plan_after:
            print(f"    {line}")
    finally:
        conn.close()

    print("\n--- PostgreSQL Summary ---")
    print(f"  Plan type : {scan_type(plan_before)} -> {scan_type(plan_after)}")
    print(f"  Latency   : {fmt(time_before, ' ms')} -> {fmt(time_after, ' ms')}  (speedup {fmt(ratio(time_before, time_after), 'x')})")
    if "Index" not in scan_type(plan_after):
        print("  NOTE: Postgres chose not to use the index on a table this small. Report that honestly.")


# ==============================================================================
# MAIN
# ==============================================================================
def main():
    start = time.time()
    spark = None
    success = True
    try:
        spark = create_spark_session()
        sync_gold_to_postgres(spark)      # comment out to skip the sync when re-running benchmarks
        benchmark_lakehouse(spark)
        benchmark_postgres(spark)
        print(f"\nSTORAGE OPTIMIZATION COMPLETE in {time.time() - start:.1f}s")
    except Exception as e:
        print(f"\nFATAL ERROR: {type(e).__name__}: {e}")
        traceback.print_exc()
        success = False
    finally:
        if spark:
            spark.stop()
        sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()