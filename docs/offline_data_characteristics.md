# Offline Data Simulation & Characteristics
**Vietnam E-Commerce Food & Retail Platform**

---

## 1. Data Skew Analysis

The generator simulates real-world distribution imbalances common in Vietnamese e-commerce to benchmark Apache Spark optimizations (AQE, Skew Join handling, and Z-Ordering):

| Table | Skewed Column | Distribution Breakdown | Technical Impact & Mitigation |
| :--- | :--- | :--- | :--- |
| **`orders`** | `shipping_city` | • **Ho Chi Minh City**: **80.0%**<br>• **Hanoi**: **10.0%**<br>• **Da Nang**: **5.0%**<br>• **Can Tho**: **3.0%**<br>• **Hai Phong**: **2.0%** | Causes severe **Partition Skew** and **Join Skew** when grouping by geography or joining `fact_orders` with `dim_shipping_location`. Handled via Spark AQE (`spark.sql.adaptive.skewJoin.enabled = true`) to dynamically split skewed partitions. |
| **`products`** | `category` | • **ELECTRONICS**: **75.0%**<br>• **FASHION**: **10.0%**<br>• **HOME_APPLIANCES**: **7.0%**<br>• **BEAUTY**: **5.0%**<br>• **SPORTS**: **3.0%** | Electronics dominates catalog volume and total revenue, creating skew during category-level analytical aggregations. |
| **`order_items`** | `product_id` | **Zipfian Distribution ($s = 1.15$)**:<br>• Top 20% "bestseller" products account for **>75%** of items ordered.<br>• Remaining 80% products form a long tail. | Simulates product popularity curves, causing data imbalance when joining `order_items` with `dim_product`. |
| **`customers`** | `city` | • **Ho Chi Minh City**: **50.0%**<br>• **Hanoi**: **25.0%**<br>• Others: **25.0%** | Reflects demographic concentration in Vietnam's two primary economic centers. |

---

## 2. Cardinality Analysis

$$\text{Cardinality Ratio} = \frac{\text{Unique Values}}{\text{Total Rows}}$$

| Table | Column | Unique Values | Total Rows | Cardinality Ratio | Cardinality Level | Engineering Significance / Handling |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| `orders` | `order_id` | 100,000 | 100,000 | **1.0 (100%)** | Very High | Primary Key. Ideal for B-Tree indexing in PostgreSQL DWH. |
| `order_items` | `order_item_id` | 176,187 | 179,707 | **0.9804 (98.0%)** | Very High | Primary Key (ratio < 1.0 due to ~2% duplicate retry bug). |
| `payments` | `payment_id` | 100,000 | 100,000 | **1.0 (100%)** | Very High | Primary Key for financial transactions. |
| `customers` | `customer_id` | 20,000 | 20,000 | **1.0 (100%)** | Very High | Natural Business Key, entity key for Feature Store lookups. |
| `products` | `product_id` | 5,000 | 5,000 | **1.0 (100%)** | Very High | Natural product SKU identifier. |
| `order_items` | `order_id` | 100,000 | 179,707 | **0.556 (55.6%)** | High | Foreign Key (averages ~1.8 items per order). |
| `orders` | `customer_id` | 20,000 | 100,000 | **0.200 (20.0%)** | Medium | Foreign Key (averages ~5 orders per customer). |
| `order_items` | `product_id` | 5,000 | 179,707 | **0.0278 (2.78%)** | Low-Medium | Foreign Key influenced by Zipfian skew. |
| `orders` | `status` | 8 | 100,000 | **0.00008 (<0.01%)** | Very Low | Low-cardinality enum. Optimized via Parquet dictionary encoding. |
| `orders` | `shipping_city` | 5 | 100,000 | **0.00005 (<0.01%)** | Very Low | Ideal candidate for Z-Ordering and directory partitioning. |
| `products` | `category` | 5 | 5,000 | **0.001 (0.1%)** | Very Low | Category taxonomy lookup. |
| `payments` | `method` | 5 | 100,000 | **0.00005 (<0.01%)** | Very Low | Fixed payment gateways (`MOMO`, `VNPAY`, `COD`, etc.). |

---

## 3. Schema Evolution (Pre vs. Post Cutoff: 2026-06-01)

To test the Lakehouse's resilience to upstream schema drifts via Delta Lake's `mergeSchema = true`, the `payments` dataset introduces an evolved schema midway through the generation timeline:

- **Evolution Cutoff Date**: `2026-06-01`
- **Business Rationale**: The payment gateway upgraded its webhook payload to supply granular error codes on transaction failures.

### Side-by-Side Schema Comparison:

| Attribute | Pre-Evolution (`< 2026-06-01`) | Post-Evolution (`>= 2026-06-01`) |
| :--- | :--- | :--- |
| **Schema Identifier** | `PAYMENTS_V1` | `PAYMENTS_V2` |
| **Column Count** | **8 columns** | **9 columns** (+1 evolved column) |
| **Column Manifest** | 1. `payment_id` (`STRING`)<br>2. `order_id` (`STRING`)<br>3. `payment_ts` (`TIMESTAMP[ms]`)<br>4. `updated_ts` (`TIMESTAMP[ms]`)<br>5. `payment_date` (`DATE`)<br>6. `method` (`STRING`)<br>7. `amount` (`INT64`)<br>8. `status` (`STRING`) | 1. `payment_id` (`STRING`)<br>2. `order_id` (`STRING`)<br>3. `payment_ts` (`TIMESTAMP[ms]`)<br>4. `updated_ts` (`TIMESTAMP[ms]`)<br>5. `payment_date` (`DATE`)<br>6. `method` (`STRING`)<br>7. `amount` (`INT64`)<br>8. `status` (`STRING`)<br>9. **`failure_reason`** (`STRING`, nullable) |
| **`failure_reason` Values** | *Column does not exist* | • When `status = 'FAILED'`: Randomly assigned (`INSUFFICIENT_FUNDS`, `GATEWAY_TIMEOUT`, `INVALID_CARD`, `OTP_EXPIRED`).<br>• When `status = 'SUCCESS'`: Populated as `NULL`. |
| **Bronze Handling** | Standard batch read | Delta Lake schema merging seamlessly appends V2 records and backfills `failure_reason = NULL` for historical V1 records without breaking ingestion pipelines. |

---

## 4. Duplicate Rate Analysis

Simulates a common e-commerce checkout glitch where network timeouts cause client-side auto-retries, resulting in duplicate line items with a slight timestamp drift (+2.0s to 5.0s on `created_ts`).

| Metric | Measured Value | Percentage (%) | Engineering Note |
| :--- | :--- | :--- | :--- |
| **Affected Table** | `order_items` | — | Isolated specifically to line items |
| **Total Order Items Generated** | `179,707` rows | 100% | Total raw landing volume |
| **Injected Duplicate Records** | `3,520` rows | **1.959% (~2.0%)** | Generated via `random.random() < 0.02` |
| **Other Tables** (`orders`, `customers`, `products`, `payments`) | 0 rows | **0.0%** | Strict Primary Key integrity maintained |

*Silver Layer Deduplication*: The Silver pipeline sanitizes this table using Spark's deduplication API:  
`stg_order_items = raw_order_items.dropDuplicates(["order_item_id"])`, eliminating all 3,520 duplicate rows before downstream star-schema enrichment.

---

## 5. Data Volume

The offline generator synthesized continuous transactions spanning **31 consecutive days** (`2026-05-15` through `2026-06-14`):

### 5.1 Record Count Breakdown
| Entity | Record Count | Partitioning Strategy |
| :--- | :--- | :--- |
| **`orders`** | **100,000** rows | 31 date partitions (`order_date=YYYY-MM-DD`) |
| **`order_items`** | **179,707** rows | 31 date partitions (averages ~1.8 items per order) |
| **`customers`** | **20,000** snapshots + updates | Partitioned into `snapshot/` (V1) and `changes/` (SCD2 feeds) |
| **`products`** | **5,000** rows | Conformed catalog dimension |
| **`payments`** | **100,000** rows | 31 date partitions (1:1 with order checkout events) |
| **TOTAL SYSTEM VOLUME** | **~404,707+ rows** | Complete raw landing ingest |

### 5.2 Physical Storage Footprint
- **File Format**: Apache Parquet with Snappy compression and dictionary encoding.
- **MinIO Storage (`s3a://raw-landing/`)**: **~11.05 MB** (96 Parquet files across all partition subdirectories).
- **Uncompressed In-Memory Footprint**: Estimated **~55 MB – 65 MB** when loaded into Spark DataFrames or PostgreSQL DWH tables.
