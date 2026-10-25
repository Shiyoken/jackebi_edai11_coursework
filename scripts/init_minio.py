"""
Initialize MinIO S3 Buckets for Lakehouse Layers.

This script connects to the local MinIO container and ensures that 
the necessary landing and lakehouse tier buckets exist.
"""

from minio import Minio
from minio.error import S3Error

# MinIO Client Configuration
MINIO_ENDPOINT = "localhost:9000"
MINIO_ACCESS_KEY = "admin"
MINIO_SECRET_KEY = "password123"

# Required buckets for our Food Delivery Data Platform
BUCKETS = [
    "raw-landing",        # Initial ingestion from data generator
    "lakehouse-bronze",   # Raw ingested data with minimal cleaning
    "lakehouse-silver",   # Deduplicated, filtered, validated data
    "lakehouse-gold",     # Star schema / feature tables for analytics
]


def init_buckets():
    """Connect to MinIO and create buckets if they do not exist."""
    print(f"Connecting to MinIO at http://{MINIO_ENDPOINT}...")
    client = Minio(
        MINIO_ENDPOINT,
        access_key=MINIO_ACCESS_KEY,
        secret_key=MINIO_SECRET_KEY,
        secure=False  # Using HTTP locally
    )

    for bucket in BUCKETS:
        try:
            if not client.bucket_exists(bucket):
                client.make_bucket(bucket)
                print(f"  [+] Created bucket: '{bucket}'")
            else:
                print(f"  [i] Bucket already exists: '{bucket}'")
        except S3Error as err:
            print(f"  [!] Error handling bucket '{bucket}': {err}")

    print("\nMinIO initialization completed successfully!")


if __name__ == "__main__":
    init_buckets()