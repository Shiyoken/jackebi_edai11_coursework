import argparse 
import os 
import sys 
from pathlib import Path 
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import StreamTableEnvironment, EnvironmentSettings 

# Find the Kafka connector JAR -> Flink talk to Kafka
def resolve_kafka_jar() -> str:
    env_jar = os.getenv("FLINK_KAFKA_JAR")
    if env_jar and Path(env_jar).is_file():
        return str(Path(env_jar).resolve())
    # 2. Compute project root relative to this script
    script_dir = Path(__file__).resolve().parent      # .../jackebi_coursework/flink_jobs
    project_root = script_dir.parent                  # .../jackebi_coursework
    candidate_paths = [
        project_root / "jars" / "flink-sql-connector-kafka-4.0.1-2.0.jar",
        script_dir / "jars" / "flink-sql-connector-kafka-4.0.1-2.0.jar",
        Path("/opt/flink/lib/flink-sql-connector-kafka-4.0.1-2.0.jar"),
        Path("/opt/flink/usrlib/flink-sql-connector-kafka-4.0.1-2.0.jar"),
    ]
    for path in candidate_paths:
        if path.is_file():
            return str(path.resolve())

    raise FileNotFoundError(
        "Could not find flink-sql-connector-kafka JAR. Please ensure it exists in your jars/ directory."
    )

# ClickStream Flink SQL table
def run_clickstream_pipeline(t_env: StreamTableEnvironment, broker: str, window_min: int, baseline: bool, bounded: bool):
    watermark_interval = "INTERVAL '5' SECOND" if baseline else "INTERVAL '15' MINUTE"
    group_id = f"flink-clickstream-{'baseline' if baseline else 'optimized'}-{os.getpid()}"
    bounded_clause = "'scan.bounded.mode' = 'latest-offset'," if bounded else ""

    print("=================================================================")
    print("Starting Flink Clickstream Window Processor")
    print(f"Mode: {'BASELINE (5-sec Watermark)' if baseline else 'OPTIMIZED (15-min Watermark)'}")
    print(f"Window Size: {window_min} minute(s) | Bounded Mode: {bounded} | Broker: {broker}")
    print("=================================================================")

    t_env.execute_sql(f"""
        CREATE TABLE clickstream(
            event_id        STRING,
            session_id      STRING,
            customer_id     STRING,
            product_id      STRING,
            category        STRING,
            page_type       STRING,
            device_type     STRING,
            city            STRING,
            event_timestamp TIMESTAMP(3),
            is_late         BOOLEAN,
            produced_at     TIMESTAMP(3),
            WATERMARK FOR event_timestamp AS event_timestamp - {watermark_interval}
        ) WITH (
            'connector'                    = 'kafka',
            'topic'                        = 'clickstream',
            'properties.bootstrap.servers' = '{broker}',
            'properties.group.id'          = '{group_id}',
            'scan.startup.mode'            = 'earliest-offset',
            {bounded_clause}
            'format'                       = 'json'
        )
    """)
    # Modern Window Table-Valued Function (TVF) Query
    query = f"""
        SELECT
            city,
            category,
            DATE_FORMAT(window_start, 'yyyy-MM-dd HH:mm:ss') AS window_start,
            DATE_FORMAT(window_end,   'yyyy-MM-dd HH:mm:ss') AS window_end,
            COUNT(event_id)                                  AS total_clicks,
            COUNT(DISTINCT customer_id)                      AS unique_customers,
            COUNT(DISTINCT session_id)                       AS unique_sessions,
            SUM(CASE WHEN is_late THEN 1 ELSE 0 END)         AS late_events_absorbed
        FROM TABLE(
            TUMBLE(TABLE clickstream, DESCRIPTOR(event_timestamp), INTERVAL '{window_min}' MINUTE)
        )
        GROUP BY
            city,
            category,
            window_start,
            window_end
    """
    t_env.sql_query(query).execute().print()

# Payment Callback Flink SQL Table
def run_payment_callback_pipeline(t_env: StreamTableEnvironment, broker: str, baseline: bool, bounded: bool):
    group_id = f"flink-payment-{'baseline' if baseline else 'dedup'}-{os.getpid()}"
    bounded_clause = "'scan.bounded.mode' = 'latest-offset'," if bounded else ""

    print("=================================================================")
    print("Starting Flink Payment Callback Pipeline")
    print(f"Mode: {'BASELINE (Duplicate Audit)' if baseline else 'OPTIMIZED (Append-Only Deduplication)'}")
    print(f"Bounded Mode: {bounded} | Broker: {broker}")
    print("=================================================================")

    t_env.execute_sql(f"""
        CREATE TABLE payment_callbacks (
            callback_id        STRING,
            payment_id         STRING,
            order_id           STRING,
            `method`            STRING,
            amount             BIGINT,
            status             STRING,
            failure_reason     STRING,
            callback_timestamp TIMESTAMP(3),
            proc_time          AS PROCTIME()
        ) WITH (
            'connector'                    = 'kafka',
            'topic'                        = 'payment_callbacks',
            'properties.bootstrap.servers' = '{broker}',
            'properties.group.id'          = '{group_id}',
            'scan.startup.mode'            = 'earliest-offset',
            {bounded_clause}
            'format'                       = 'json'
        )
    """)

    if baseline:
        # Baseline Mode: Proves the raw Kafka stream contains ~1.5% duplicate retries
        query = """
            SELECT
                COUNT(*)                                AS total_rows,
                COUNT(DISTINCT callback_id)             AS unique_callbacks,
                COUNT(*) - COUNT(DISTINCT callback_id)  AS duplicate_retries_detected
            FROM payment_callbacks
        """
    else:
        # Optimized Mode: First-Row Deduplication Operator (Ordered by proc_time ASC)
        query = """
            SELECT
                callback_id,
                payment_id,
                order_id,
                `method`,
                amount,
                status,
                failure_reason,
                DATE_FORMAT(callback_timestamp, 'yyyy-MM-dd HH:mm:ss.SSS') AS callback_timestamp
            FROM (
                SELECT
                    *,
                    ROW_NUMBER() OVER (
                        PARTITION BY callback_id
                        ORDER BY proc_time ASC
                    ) AS row_num
                FROM payment_callbacks
            )
            WHERE row_num = 1
        """
    t_env.sql_query(query).execute().print()

def main():
    parser = argparse.ArgumentParser(description="Flink Streaming Processor")
    parser.add_argument(
        "--job",
        choices=["clickstream", "payments"],
        default = "clickstream",
        help="Streaming pipeline to execute ('clickstream' or 'payment') ", 
    )
    parser.add_argument(
        "--broker",
        default="localhost:9092",
        help="Kafka bootstrap broker (default: localhost:9092, or kafka:29092 in Docker)",
    )
    parser.add_argument(
        "--window-min",
        type=int,
        default=1,
        help="Tumbling window duration in minutes (default 1)",
    )
    parser.add_argument(
        "--baseline",
        action="store_true",
        help="Run in baseline comparison mode",
    )
    parser.add_argument(
        "--bounded",
        action="store_true",
        help="Bounded mode: stop and flush open windows at end of topic",
    )
    args = parser.parse_args()

    # Initialize Flink Streaming Environment
    from pyflink.common import Configuration
    conf = Configuration()
    conf.set_string('rest.bind-port', '8088')
    env = StreamExecutionEnvironment.get_execution_environment(conf)
    env.set_parallelism(1)

    # Attach Kafka connection 
    jar_path = resolve_kafka_jar()
    print(f"[INFO] Using Kafka connector JAR: {jar_path}")
    env.add_jars(Path(jar_path).as_uri())

    # Create Table Environment 
    settings = EnvironmentSettings.new_instance().in_streaming_mode().build()
    t_env = StreamTableEnvironment.create(env, environment_settings=settings)
    t_env.get_config().set("pipeline.auto-watermark-interval", "100 ms")
    t_env.get_config().set("table.exec.state.ttl", "1 h")
    app_name = "Flink-Clickstream-Window-Processor" if args.job == "clickstream" else "Flink-Payment-Deduplication-Processor"
    t_env.get_config().set("pipeline.name", app_name)
    if args.job == "clickstream":
        run_clickstream_pipeline(t_env, args.broker, args.window_min, args.baseline, args.bounded)
    elif args.job == "payments":
        run_payment_callback_pipeline(t_env, args.broker, args.baseline, args.bounded)

if __name__ == "__main__":
    main()
