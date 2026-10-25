"""
E-Commerce Offline Data Generator — Direct-to-MinIO Streamer.
Follows the pattern from Lecture 8 using MinIO's put_object with in-memory BytesIO buffers.

Features:
- Pure pricing: NO discounts, NO coupon_code columns anywhere.
- Total amount: Subtotal + shipping_fee (strictly INT64 VND).
- Skew: 75% Electronics, 80% Ho Chi Minh City, Zipf product distribution.
- Schema Evolution: Payments before 2026-06-01 have 8 columns; on/after have 9 columns (failure_reason).
- Offline Duplicates: ~2% duplicate rows in order_items with drifted created_ts.
- SCD Type 2 Source: customers/snapshot/ (initial version) + customers/changes/ (tier upgrades by spend).
- Storage: Streamed directly to MinIO (s3://raw-landing/) with zero temporary local files.
"""

import os
import sys
import io
import json
import random
import datetime
import argparse
from zoneinfo import ZoneInfo
from typing import Dict, List, Tuple

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq
from minio import Minio

# ------------------------------------------------------------------------------
# CONSTANTS & TIMEZONES
# ------------------------------------------------------------------------------
TZ_VN = ZoneInfo("Asia/Ho_Chi_Minh")
TZ_UTC = datetime.timezone.utc

CITIES = ["Ho Chi Minh City", "Hanoi", "Da Nang", "Can Tho", "Hai Phong"]
CITY_WEIGHTS = [0.80, 0.10, 0.05, 0.03, 0.02]  # 80% Skew to HCMC

CATEGORIES = ["ELECTRONICS", "FASHION", "HOME_APPLIANCES", "BEAUTY", "SPORTS"]
CATEGORY_WEIGHTS = [0.75, 0.10, 0.07, 0.05, 0.03]  # 75% Skew to Electronics

BRANDS_BY_CAT = {
    "ELECTRONICS": ["Apple", "Samsung", "Sony", "Xiaomi", "Dell", "Asus"],
    "FASHION": ["Uniqlo", "Zara", "Coolmate", "An Phuoc", "Canifa"],
    "HOME_APPLIANCES": ["Lock&Lock", "Sunhouse", "Philips", "Panasonic"],
    "BEAUTY": ["La Roche-Posay", "L'Oreal", "Innisfree", "Cocoon"],
    "SPORTS": ["Nike", "Adidas", "Puma", "Decathlon"]
}

ORDER_STATUSES = ["COMPLETED", "COMPLETED", "COMPLETED", "SHIPPED", "PROCESSING", "PAID", "CANCELLED", "RETURNED"]
PAYMENT_METHODS = ["VNPAY", "MOMO", "ZALOPAY", "CREDIT_CARD", "COD"]
FAILURE_REASONS = ["INSUFFICIENT_FUNDS", "GATEWAY_TIMEOUT", "INVALID_CARD", "OTP_EXPIRED"]

# ------------------------------------------------------------------------------
# PYARROW SCHEMAS 
# ------------------------------------------------------------------------------
CUSTOMERS_SCHEMA = pa.schema([
    pa.field("customer_id", pa.string(), nullable=False),
    pa.field("signup_ts", pa.timestamp("ms", tz="UTC"), nullable=False),
    pa.field("updated_ts", pa.timestamp("ms", tz="UTC"), nullable=False),
    pa.field("city", pa.string(), nullable=False),
    pa.field("segment", pa.string(), nullable=False),
    pa.field("signup_device", pa.string(), nullable=False),
])

PRODUCTS_SCHEMA = pa.schema([
    pa.field("product_id", pa.string(), nullable=False),
    pa.field("category", pa.string(), nullable=False),
    pa.field("brand", pa.string(), nullable=False),
    pa.field("base_price", pa.int64(), nullable=False),
    pa.field("is_active", pa.bool_(), nullable=False),
    pa.field("created_ts", pa.timestamp("ms", tz="UTC"), nullable=False),
    pa.field("updated_ts", pa.timestamp("ms", tz="UTC"), nullable=False),
])

ORDERS_SCHEMA = pa.schema([
    pa.field("order_id", pa.string(), nullable=False),
    pa.field("customer_id", pa.string(), nullable=False),
    pa.field("order_ts", pa.timestamp("ms", tz="UTC"), nullable=False),
    pa.field("updated_ts", pa.timestamp("ms", tz="UTC"), nullable=False),
    pa.field("order_date", pa.date32(), nullable=False),
    pa.field("status", pa.string(), nullable=False),
    pa.field("shipping_city", pa.string(), nullable=False),
    pa.field("shipping_fee", pa.int64(), nullable=False),
    pa.field("total_amount", pa.int64(), nullable=False),
])

ORDER_ITEMS_SCHEMA = pa.schema([
    pa.field("order_item_id", pa.string(), nullable=False),
    pa.field("order_id", pa.string(), nullable=False),
    pa.field("product_id", pa.string(), nullable=False),
    pa.field("quantity", pa.int32(), nullable=False),
    pa.field("unit_price", pa.int64(), nullable=False),
    pa.field("order_date", pa.date32(), nullable=False),
    pa.field("created_ts", pa.timestamp("ms", tz="UTC"), nullable=False),
])

PAYMENTS_V1_SCHEMA = pa.schema([
    pa.field("payment_id", pa.string(), nullable=False),
    pa.field("order_id", pa.string(), nullable=False),
    pa.field("payment_ts", pa.timestamp("ms", tz="UTC"), nullable=False),
    pa.field("updated_ts", pa.timestamp("ms", tz="UTC"), nullable=False),
    pa.field("payment_date", pa.date32(), nullable=False),
    pa.field("method", pa.string(), nullable=False),
    pa.field("amount", pa.int64(), nullable=False),
    pa.field("status", pa.string(), nullable=False),
])

PAYMENTS_V2_SCHEMA = pa.schema([
    pa.field("payment_id", pa.string(), nullable=False),
    pa.field("order_id", pa.string(), nullable=False),
    pa.field("payment_ts", pa.timestamp("ms", tz="UTC"), nullable=False),
    pa.field("updated_ts", pa.timestamp("ms", tz="UTC"), nullable=False),
    pa.field("payment_date", pa.date32(), nullable=False),
    pa.field("method", pa.string(), nullable=False),
    pa.field("amount", pa.int64(), nullable=False),
    pa.field("status", pa.string(), nullable=False),
    pa.field("failure_reason", pa.string(), nullable=True),
])


# ------------------------------------------------------------------------------
# UPLOAD IN-MEMORY PARQUET TO MINIO
# ------------------------------------------------------------------------------
def upload_parquet_table(client: Minio, bucket: str, object_path: str, table: pa.Table):
    """Serialize PyArrow table to in-memory bytes and stream to MinIO."""
    buffer = io.BytesIO()
    pq.write_table(table, buffer, coerce_timestamps="ms")
    data = buffer.getvalue()

    client.put_object(
        bucket,
        object_path,
        io.BytesIO(data),
        len(data),
        content_type="application/octet-stream"
    )
    print(f"  Uploaded s3://{bucket}/{object_path} ({len(table)} rows)")


# ------------------------------------------------------------------------------
# GENERATION ENGINE
# ------------------------------------------------------------------------------
def run_generator(client: Minio, bucket: str, num_orders: int, num_customers: int, num_products: int, start_date: datetime.date, days: int, seed: int = 42):
    random.seed(seed)
    np.random.seed(seed)

    if not client.bucket_exists(bucket):
        client.make_bucket(bucket)
        print(f"Bucket '{bucket}' created.")
    else:
        print(f"Bucket '{bucket}' ready.")

    # 1. Customers Snapshot (Version 1 for everyone)
    print("\n--- Generating Customers Initial Snapshot ---")
    base_start = datetime.datetime(2025, 1, 1, tzinfo=TZ_UTC)
    cust_ids = [f"CUST-{i:010d}" for i in range(1, num_customers + 1)]
    signup_offsets = np.random.randint(0, 365 * 86400, size=num_customers)
    signup_ts = [base_start + datetime.timedelta(seconds=int(s)) for s in signup_offsets]
    
    # Initial updated_ts equals signup_ts
    cities = np.random.choice(CITIES, size=num_customers, p=[0.50, 0.25, 0.12, 0.08, 0.05])
    devices = np.random.choice(["MOBILE", "DESKTOP", "TABLET"], size=num_customers, p=[0.75, 0.20, 0.05])
    initial_segments = ["BRONZE"] * num_customers  # Everyone starts at BRONZE

    cust_snapshot_tbl = pa.Table.from_arrays([
        pa.array(cust_ids, type=pa.string()),
        pa.array(signup_ts, type=pa.timestamp("ms", tz="UTC")),
        pa.array(signup_ts, type=pa.timestamp("ms", tz="UTC")), # updated_ts = signup_ts
        pa.array(cities, type=pa.string()),
        pa.array(initial_segments, type=pa.string()),
        pa.array(devices, type=pa.string()),
    ], schema=CUSTOMERS_SCHEMA)
    upload_parquet_table(client, bucket, "customers/snapshot/part-00000.parquet", cust_snapshot_tbl)

    # State tracking for customer SCD2 spend upgrades
    cust_metadata = {
        c_id: {"signup_ts": s_ts, "city": c_city, "device": c_dev, "segment": "BRONZE", "spend": 0}
        for c_id, s_ts, c_city, c_dev in zip(cust_ids, signup_ts, cities, devices)
    }
    customer_changes_by_date = {}

    # 2. Products (75% Electronics Skew)
    print("\n--- Generating Products (75% Skew) ---")
    prod_ids = [f"PROD-{i:010d}" for i in range(1, num_products + 1)]
    categories = np.random.choice(CATEGORIES, size=num_products, p=CATEGORY_WEIGHTS)
    brands = [random.choice(BRANDS_BY_CAT[cat]) for cat in categories]
    prices = [
        int(round(random.uniform(500_000, 45_000_000), -4)) if cat == "ELECTRONICS"
        else int(round(random.uniform(50_000, 3_000_000), -3))
        for cat in categories
    ]
    base_time = datetime.datetime(2024, 6, 1, tzinfo=TZ_UTC)
    created_ts = [base_time + datetime.timedelta(days=random.randint(0, 300)) for _ in range(num_products)]
    prod_update_ts = [c_ts + datetime.timedelta(days=random.randint(0, 30)) for c_ts in created_ts]
    is_active = np.random.choice([True, False], size=num_products, p=[0.95, 0.05])

    prod_tbl = pa.Table.from_arrays([
        pa.array(prod_ids, type=pa.string()),
        pa.array(categories, type=pa.string()),
        pa.array(brands, type=pa.string()),
        pa.array(prices, type=pa.int64()),
        pa.array(is_active, type=pa.bool_()),
        pa.array(created_ts, type=pa.timestamp("ms", tz="UTC")),
        pa.array(prod_update_ts, type=pa.timestamp("ms", tz="UTC")),
    ], schema=PRODUCTS_SCHEMA)
    upload_parquet_table(client, bucket, "products/part-00000.parquet", prod_tbl)

    # 3. Orders, Order Items, Payments & Customer Change Events
    print("\n--- Generating Orders, Items, Payments & SCD2 Customer Upgrades ---")
    prod_prices = dict(zip(prod_ids, prices))
    zipf_probs = 1.0 / np.power(np.arange(1, num_products + 1), 1.15)
    zipf_probs /= zipf_probs.sum()

    start_dt = datetime.datetime.combine(start_date, datetime.time(0, 0, 0), tzinfo=TZ_UTC)
    total_seconds = days * 86400
    random_offsets = np.sort(np.random.randint(0, total_seconds, size=num_orders))
    evolution_cutoff_date = datetime.date(2026, 6, 1)

    orders_by_date = {}
    items_by_date = {}
    payments_by_date = {}

    total_item_count = 0
    injected_duplicate_count = 0
    item_counter = 1
    total_customer_upgrades = 0

    for idx, sec_offset in enumerate(random_offsets):
        order_id = f"ORD-{idx + 1:010d}"
        cust_id = random.choice(cust_ids)
        o_ts = start_dt + datetime.timedelta(seconds=int(sec_offset))
        u_ts = o_ts + datetime.timedelta(minutes=random.randint(5, 120))

        o_local = o_ts.astimezone(TZ_VN)
        o_date = o_local.date()
        date_str = o_date.isoformat()

        status = random.choice(ORDER_STATUSES)
        shipping_city = np.random.choice(CITIES, p=CITY_WEIGHTS)
        shipping_fee = 0 if (shipping_city == "Ho Chi Minh City" and random.random() < 0.40) else 30_000

        # Items
        num_items = random.choices([1, 2, 3, 4, 5], weights=[0.55, 0.25, 0.12, 0.05, 0.03])[0]
        selected_indices = np.random.choice(num_products, size=num_items, p=zipf_probs, replace=False)

        subtotal = 0
        current_items = []
        for p_idx in selected_indices:
            p_id = prod_ids[p_idx]
            qty = random.randint(1, 4)
            unit_price = prod_prices[p_id]
            subtotal += (qty * unit_price)

            item_row = {
                "order_item_id": f"ITEM-{item_counter:010d}",
                "order_id": order_id,
                "product_id": p_id,
                "quantity": qty,
                "unit_price": unit_price,
                "order_date": o_date,
                "created_ts": o_ts
            }
            current_items.append(item_row)
            total_item_count += 1
            item_counter += 1

            # Injected Duplicates (~2%)
            if random.random() < 0.02:
                dupe = item_row.copy()
                dupe["created_ts"] = o_ts + datetime.timedelta(seconds=random.uniform(2.0, 5.0))
                current_items.append(dupe)
                total_item_count += 1
                injected_duplicate_count += 1

        total_amount = subtotal + shipping_fee

        orders_by_date.setdefault(date_str, []).append({
            "order_id": order_id,
            "customer_id": cust_id,
            "order_ts": o_ts,
            "updated_ts": u_ts,
            "order_date": o_date,
            "status": status,
            "shipping_city": shipping_city,
            "shipping_fee": shipping_fee,
            "total_amount": total_amount
        })
        items_by_date.setdefault(date_str, []).extend(current_items)

        # Customer SCD Type 2 Upgrade Logic based on cumulative COMPLETED spend
        if status in ["COMPLETED", "SHIPPED", "PAID"]:
            cm = cust_metadata[cust_id]
            cm["spend"] += total_amount
            new_seg = None
            if cm["spend"] >= 50_000_000 and cm["segment"] != "DIAMOND":
                new_seg = "DIAMOND"
            elif cm["spend"] >= 20_000_000 and cm["segment"] not in ["GOLD", "DIAMOND"]:
                new_seg = "GOLD"
            elif cm["spend"] >= 5_000_000 and cm["segment"] not in ["SILVER", "GOLD", "DIAMOND"]:
                new_seg = "SILVER"

            if new_seg:
                cm["segment"] = new_seg
                customer_changes_by_date.setdefault(date_str, []).append({
                    "customer_id": cust_id,
                    "signup_ts": cm["signup_ts"],
                    "updated_ts": o_ts, # Upgrade happened at order completion time
                    "city": cm["city"],
                    "segment": new_seg,
                    "signup_device": cm["device"]
                })
                total_customer_upgrades += 1

        # Payments
        p_status = "SUCCESS" if status not in ["CANCELLED", "RETURNED"] else random.choice(["FAILED", "REFUNDED"])
        p_ts = o_ts + datetime.timedelta(seconds=random.randint(15, 180))
        p_up_ts = p_ts + datetime.timedelta(seconds=random.randint(2, 60))
        p_date = p_ts.astimezone(TZ_VN).date()

        p_row = {
            "payment_id": f"PAY-{idx + 1:010d}-01",
            "order_id": order_id,
            "payment_ts": p_ts,
            "updated_ts": p_up_ts,
            "payment_date": p_date,
            "method": random.choice(PAYMENT_METHODS),
            "amount": total_amount,
            "status": p_status,
        }
        if p_date >= evolution_cutoff_date:
            p_row["failure_reason"] = random.choice(FAILURE_REASONS) if p_status == "FAILED" else None

        payments_by_date.setdefault(p_date.isoformat(), []).append(p_row)

    # Stream date partitions into MinIO
    print("\n--- Streaming Partitions into MinIO ---")
    for d_str, rows in orders_by_date.items():
        tbl = pa.Table.from_pylist(rows, schema=ORDERS_SCHEMA)
        upload_parquet_table(client, bucket, f"orders/order_date={d_str}/part-00000.parquet", tbl)

    for d_str, rows in items_by_date.items():
        tbl = pa.Table.from_pylist(rows, schema=ORDER_ITEMS_SCHEMA)
        upload_parquet_table(client, bucket, f"order_items/order_date={d_str}/part-00000.parquet", tbl)

    for d_str, rows in payments_by_date.items():
        dt_obj = datetime.date.fromisoformat(d_str)
        schema_to_use = PAYMENTS_V2_SCHEMA if dt_obj >= evolution_cutoff_date else PAYMENTS_V1_SCHEMA
        tbl = pa.Table.from_pylist(rows, schema=schema_to_use)
        upload_parquet_table(client, bucket, f"payments/payment_date={d_str}/part-00000.parquet", tbl)

    # Stream Customer Changes (SCD Type 2 Feed)
    print("\n--- Streaming Customer SCD2 Changes into MinIO ---")
    for d_str, rows in customer_changes_by_date.items():
        tbl = pa.Table.from_pylist(rows, schema=CUSTOMERS_SCHEMA)
        upload_parquet_table(client, bucket, f"customers/changes/change_date={d_str}/part-00000.parquet", tbl)

    # Manifest file for rubric verification
    customers_with_changes = len(set(
        row["customer_id"] for rows in customer_changes_by_date.values() for row in rows
    ))
    manifest = {
        "unique_orders_count": num_orders,
        "total_order_items_generated": total_item_count,
        "injected_duplicates_count": injected_duplicate_count,
        "duplicate_rate_pct": round((injected_duplicate_count / total_item_count) * 100, 3),
        "schema_evolution_cutoff": evolution_cutoff_date.isoformat(),
        "total_customer_snapshots": num_customers,
        "total_customer_upgrade_events": total_customer_upgrades,
        "customers_with_upgrades_count": customers_with_changes,
        "pct_customers_upgraded": round((customers_with_changes / num_customers) * 100, 2),
        "date_partitions_count": len(orders_by_date),
        "target_bucket": bucket
    }
    manifest_bytes = json.dumps(manifest, indent=2).encode("utf-8")
    client.put_object(
        bucket,
        "generation_manifest.json",
        io.BytesIO(manifest_bytes),
        len(manifest_bytes),
        content_type="application/json"
    )
    print(f"\n  Uploaded s3://{bucket}/generation_manifest.json")
    print(f"  -> Customers Upgraded (SCD2 History): {customers_with_changes:,} / {num_customers:,} ({manifest['pct_customers_upgraded']}%)")

    print("\nGeneration completed successfully!")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="E-Commerce Direct MinIO Stream Generator")
    parser.add_argument("--orders", type=int, default=100_000, help="Number of unique orders")
    parser.add_argument("--customers", type=int, default=20_000, help="Number of customers")
    parser.add_argument("--products", type=int, default=5_000, help="Number of products")
    parser.add_argument("--start-date", type=str, default="2026-05-15", help="Start date (YYYY-MM-DD)")
    parser.add_argument("--days", type=int, default=30, help="Span of days")
    parser.add_argument("--bucket", type=str, default="raw-landing", help="MinIO target bucket")
    parser.add_argument("--endpoint", type=str, default="localhost:9000", help="MinIO endpoint")
    parser.add_argument("--access-key", type=str, default="admin", help="MinIO user")
    parser.add_argument("--secret-key", type=str, default="password123", help="MinIO password")
    args = parser.parse_args()

    client = Minio(
        args.endpoint,
        access_key=args.access_key,
        secret_key=args.secret_key,
        secure=False
    )
    s_date = datetime.date.fromisoformat(args.start_date)

    run_generator(
        client=client,
        bucket=args.bucket,
        num_orders=args.orders,
        num_customers=args.customers,
        num_products=args.products,
        start_date=s_date,
        days=args.days
    )