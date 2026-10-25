from pyspark.sql import SparkSession

def main():
    print("Initializing Spark Session with Delta and S3A support...")
    spark = (
        SparkSession.builder
        .appName("MinIO-Connection-Test")
        # Configuration for Delta Lake
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        # Configuration for endpoint mapping
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
        .config("spark.hadoop.fs.s3a.access.key", "admin")
        .config("spark.hadoop.fs.s3a.secret.key", "password123")
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .getOrCreate()
    )
    print(">>> Reading customers from s3a://raw-landing/customers/...")
    try:
        df = spark.read.parquet("s3a://raw-landing/customers/part-00000.parquet")
        print("\n=== SUCCESS! SAMPLE DATA FROM MINIO ===")
        df.show(5)
        print(f"Total customers in file: {df.count()}")
    except Exception as e:
        print(f"\n[!] Error connecting to MinIO: {e}")
    finally:
        spark.stop()

if __name__ == "__main__":
    main()