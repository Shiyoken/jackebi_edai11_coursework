# Online Streaming Data Simulation & Characteristics
**Vietnam E-Commerce Food & Retail Platform**

---

## 1. Overview of Streaming Architecture

The online streaming generator (`data_generator/streaming/stream_producer.py`) continuously publishes real-time event pairs into **Apache Kafka** across two decoupled topics:

1. **`clickstream`**: High-velocity user browsing and funnel events (`event_id`, `session_id`, `customer_id`, `product_id`, `page_type`, `event_timestamp`, `is_late`).
2. **`payment_callbacks`**: Asynchronous payment gateway settlement webhooks (`callback_id`, `payment_id`, `order_id`, `method`, `amount`, `status`, `failure_reason`, `callback_timestamp`).

The producer simulates real-world distributed streaming anomalies—specifically **Out-of-Order / Late Data Arrival** and **At-Least-Once Duplicate Deliveries**—to benchmark Apache Flink streaming operators.

---

## 2. Late Data Characteristics (Out-of-Order Events)

Simulates mobile app disconnections (e.g., poor 4G connectivity, tunnels, offline caches) where user interaction events are queued on client devices and flushed in batches minutes after occurring.

| Attribute | Specification | Technical Details |
| :--- | :--- | :--- |
| **Target Topic** | **`clickstream`** | Only user behavioral click events experience delayed arrival. |
| **Late Event Ratio (`--late-pct`)** | **15.0%** of total click events | Injected via `random.random() < 0.15` (Event #1 is kept strictly on-time to anchor Flink's initial watermark). |
| **Lateness Duration (How Late?)** | **300 to 600 seconds**<br>(**5 to 10 minutes late**) | `delay_seconds = random.uniform(300, 600)`<br>`event_timestamp = current_ts - timedelta(seconds=delay_seconds)` |
| **Audit Flag** | `is_late: BOOLEAN` | Explicitly tagged in raw Kafka payload for automated watermark verification. |
| **Flink Processing Impact** | **Watermark Strategy** | • **Baseline (`INTERVAL '5' SECOND`)**: The watermark advances too aggressively. All 15% late events arrive after the window closes and are dropped silently.<br>• **Optimized (`INTERVAL '15' MINUTE`)**: A 15-minute watermark tolerance window absorbs **100% of late events** into correct Tumbling Windows without data loss. |

---

## 3. Duplicate Rate & Gateway Retries

Simulates payment gateway retry policies where an asynchronous webhook fails to receive an immediate HTTP 200 ACK from the checkout service, triggering an automated retry of the callback payload.

| Attribute | Specification | Technical Details |
| :--- | :--- | :--- |
| **Target Topic** | **`payment_callbacks`** | Isolated to payment settlement webhooks. |
| **Duplicate Ratio (`--dup-pct`)** | **1.5%** of callback events | Injected via `random.random() < 0.015` using a 100-event FIFO cache. |
| **Payload Behavior** | Identical transaction IDs | `callback_id`, `payment_id`, and `order_id` remain identical to the original attempt, while `callback_timestamp` reflects the delayed retry arrival time. |
| **Kafka Partition Key** | Keyed by `callback_id` | Guarantees that identical callback retries route to the **same Kafka partition**, enabling deterministic, stateful deduplication. |
| **Flink Deduplication Handling** | **First-Row Deduplication** | PyFlink SQL uses stateful first-row deduplication to filter duplicate retries:<br>`ROW_NUMBER() OVER (PARTITION BY callback_id ORDER BY proc_time ASC) WHERE row_num = 1`. |

---

## 4. Kafka Partition Keying & Stream Balancing

Unlike batch workloads where static column skew causes join bottlenecks, streaming engines require partition-safe keying to prevent worker thread hotspotting:

| Stream / Topic | Partition Key | Balancing Behavior |
| :--- | :--- | :--- |
| **`clickstream`** | `customer_id` | Distributed across Kafka partitions via customer ID hash, preventing any single Kafka partition or Flink task slot from overloading while preserving session locality. |
| **`payment_callbacks`** | `callback_id` | Evenly distributes payments across partitions while ensuring duplicate retries for the same transaction always hash to the same partition for deduplication. |

---

## 5. Throughput, Velocity & Execution Modes

| Parameter | Configuration | Description |
| :--- | :--- | :--- |
| **Production Rate (`--rate`)** | **5.0 pairs/sec** | Emits **10.0 events/sec total** (5 clickstream events + 5 payment callbacks). |
| **Kafka Broker** | `localhost:9092` | Configured to Kafka cluster (`kafka:29092` within Docker network). |
| **Batch Limit (`--limit`)** | `100` pairs (or `0`) | Total event pairs to generate before graceful exit (`0` runs indefinitely). |
| **Clock Modes (`--sim-step`)** | • **`0.0` (Real-Time)**<br>• **`> 0.0` (Accelerated)** | In accelerated mode (e.g. `1.0s/pair`), event timestamps advance faster than wall-clock time for rapid end-to-end integration testing of long time windows. |
| **Delivery Guarantee** | `acks="all"`, `retries=3` | Enforces at-least-once delivery semantics across all Kafka broker replicas. |
