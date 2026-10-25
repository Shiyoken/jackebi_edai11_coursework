import argparse
import json
import logging
import random
import sys
import time
from datetime import datetime, timezone, timedelta
from kafka import KafkaProducer

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("stream_producer")

# ------------------------------------------------------------------------------
# Aligned Catalogs & Skew Distributions
# ------------------------------------------------------------------------------
CITIES = ["Ho Chi Minh City", "Hanoi", "Da Nang", "Can Tho", "Hai Phong"]
CITY_WEIGHTS = [0.80, 0.08, 0.05, 0.04, 0.03]  # 80% HCMC skew

CATEGORIES = ["ELECTRONICS", "FASHION", "HOME", "BEAUTY", "SPORTS"]
CATEGORY_WEIGHTS = [0.75, 0.10, 0.05, 0.05, 0.05]  # 75% ELECTRONICS skew

PAGE_TYPES = ["HOME", "CATEGORY", "PRODUCT_DETAIL", "CART", "CHECKOUT"]
PAGE_WEIGHTS = [0.35, 0.25, 0.25, 0.10, 0.05]

DEVICE_TYPES = ["MOBILE", "DESKTOP", "TABLET"]
DEVICE_WEIGHTS = [0.75, 0.20, 0.05]

PAYMENT_METHODS = ["MOMO", "ZALOPAY", "VNPAY", "CREDIT_CARD", "COD"]
STATUSES = ["SUCCESS", "FAILED", "REFUNDED"]
STATUS_WEIGHTS = [0.85, 0.10, 0.05]
FAILURE_REASONS = ["INSUFFICIENT_FUNDS", "GATEWAY_TIMEOUT", "OTP_EXPIRED"]

RUN_ID = datetime.now(timezone.utc).strftime("%Y%m%d%H%M")
BASE_TIME = datetime.now(timezone.utc)

# Top-Level Helper Functions (Zero Nested Defs)
def on_send_error(excp: Exception) -> None:
    logger.error("Failed to deliver message to Kafka broker: %s", excp)

def fmt_ts(dt: datetime) -> str:
    return dt.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]

def calculate_stream_time(base_time: datetime, step_count: int, sim_step: float) -> datetime:
    if sim_step > 0:
        return base_time + timedelta(seconds=step_count * sim_step)
    return datetime.now(timezone.utc)

def category_for(product_num: int) -> str:
    return random.Random(f"cat-{product_num}").choices(CATEGORIES, weights=CATEGORY_WEIGHTS)[0]

def city_for(cust_num: int) -> str:
    return random.Random(f"city-{cust_num}").choices(CITIES, weights=CITY_WEIGHTS)[0]

def device_for(cust_num: int) -> str:
    return random.Random(f"dev-{cust_num}").choices(DEVICE_TYPES, weights=DEVICE_WEIGHTS)[0]


def create_clickstream_event(event_id: int, cust_num: int, current_ts: datetime, late: bool = False) -> dict:
    if late:
        delay_seconds = random.uniform(300, 600)  # 5 to 10 minutes late
        event_time = fmt_ts(current_ts - timedelta(seconds=delay_seconds))
    else:
        event_time = fmt_ts(current_ts)

    page_type = random.choices(PAGE_TYPES, weights=PAGE_WEIGHTS, k=1)[0]
    product_num = random.randint(1, 5000)
    category = category_for(product_num)

    return {
        "event_id": f"EVT-{RUN_ID}-{event_id:06d}",
        "session_id": f"SES-{RUN_ID}-{cust_num:05d}",
        "customer_id": f"CUST-{cust_num:010d}",
        "product_id": f"PROD-{product_num:010d}",
        "category": category,
        "page_type": page_type,
        "device_type": device_for(cust_num),
        "city": city_for(cust_num),
        "event_timestamp": event_time,
        "is_late": late,
        "produced_at": fmt_ts(current_ts),
    }

def create_payment_callback(callback_id: int, current_ts: datetime) -> dict:
    order_num = 100000 + callback_id
    status = random.choices(STATUSES, weights=STATUS_WEIGHTS, k=1)[0]
    failure_reason = random.choice(FAILURE_REASONS) if status == "FAILED" else None

    return {
        "callback_id": f"CB-{RUN_ID}-{callback_id:06d}",
        "payment_id": f"PAY-{order_num:010d}-01",
        "order_id": f"ORD-{order_num:010d}",
        "method": random.choice(PAYMENT_METHODS),
        "amount": random.randint(50, 5000) * 1000,
        "status": status,
        "failure_reason": failure_reason,
        "callback_timestamp": fmt_ts(current_ts),
    }

def main():
    parser = argparse.ArgumentParser(description="Streaming Data Generator for E-Commerce Platform")
    parser.add_argument("--broker", default="localhost:9092", help="Kafka bootstrap broker")
    parser.add_argument("--rate", type=float, default=5.0, help="Target event pairs emitted per second")
    parser.add_argument("--limit", type=int, default=100, help="Total event pairs to emit (0 = infinite)")
    parser.add_argument("--late-pct", type=float, default=0.15, help="Percentage of late clickstreams (default: 0.15)")
    parser.add_argument("--dup-pct", type=float, default=0.015, help="Percentage of duplicate payments (default: 0.015)")
    parser.add_argument("--sim-step", type=float, default=0.0, help="Simulated seconds per pair (0.0 = real-time, 1.0 = fast demo)")
    parser.add_argument("--seed", type=int, default=None, help="Random seed for reproducible benchmark runs")
    args = parser.parse_args()

    if args.seed is not None:
        random.seed(args.seed)
    active_customers = random.sample(range(1, 15001), 200)
    try:
        producer = KafkaProducer(
            bootstrap_servers=args.broker,
            key_serializer=lambda k: k.encode("utf-8") if k else None,
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            acks="all",
            retries=3,
        )
    except Exception as e:
        logger.error("Failed to connect to Kafka broker at %s: %s", args.broker, e)
        sys.exit(1)

    print("=================================================================")
    print(f"STREAM PRODUCER STARTED -> Kafka Broker: {args.broker}")
    print(f"Rate: {args.rate} pairs/s | Late Clicks: {args.late_pct*100:.1f}% | Duplicate Payments: {args.dup_pct*100:.1f}%")
    print(f"Clock Mode: {'ACCELERATED (' + str(args.sim_step) + 's/pair)' if args.sim_step > 0 else 'REAL-TIME'}")
    print(f"Active Pool: 200 customers | Run ID: {RUN_ID}")
    print("=================================================================")

    delay = 1.0 / args.rate if args.rate > 0 else 0
    count = 0
    cached_payment_callbacks = []
    try:
        while True:
            count += 1
            now = calculate_stream_time(BASE_TIME, count, args.sim_step)
            cust_num = random.choice(active_customers)

            # 1. Emit Clickstream (Event #1 is always on-time to anchor Flink's initial watermark)
            is_late = (count > 1) and (random.random() < args.late_pct)
            click_event = create_clickstream_event(count, cust_num=cust_num, current_ts=now, late=is_late)
            producer.send(
                "clickstream",
                key=click_event["customer_id"],
                value=click_event,
            ).add_errback(on_send_error)

            if count % 100 == 0 or count <= 10:
                tag = "LATE  " if is_late else "ONTIME"
                print(f"[{tag}] Clickstream #{count}: {click_event['event_id']} | User: {click_event['customer_id']} | EventTime: {click_event['event_timestamp']}")

            # 2. Emit Payment Callback (Keyed by callback_id so duplicate retries share partition)
            is_duplicate = (random.random() < args.dup_pct) and len(cached_payment_callbacks) > 0
            if is_duplicate:
                cached_event = random.choice(cached_payment_callbacks)
                pay_event = dict(cached_event)
                pay_event["callback_timestamp"] = fmt_ts(now)

                producer.send(
                    "payment_callbacks",
                    key=pay_event["callback_id"],
                    value=pay_event,
                ).add_errback(on_send_error)

                if count % 100 == 0 or count <= 10:
                    print(f" [DUPLICATE RETRY] Callback: {pay_event['callback_id']} (Retry arrived: {pay_event['callback_timestamp']})")
            else:
                pay_event = create_payment_callback(count, current_ts=now)
                producer.send(
                    "payment_callbacks",
                    key=pay_event["callback_id"],
                    value=pay_event,
                ).add_errback(on_send_error)

                cached_payment_callbacks.append(pay_event)
                if len(cached_payment_callbacks) > 100:
                    cached_payment_callbacks.pop(0)

            if delay > 0:
                time.sleep(delay)
            if args.limit and count >= args.limit:
                break

    except KeyboardInterrupt:
        logger.info("Stream generation interrupted by user.")
    finally:
        producer.flush()
        producer.close()
        print(f"\nStream production finished: {count} pairs ({count * 2} total events) emitted.")

if __name__ == "__main__":
    main()