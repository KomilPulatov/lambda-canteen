import glob
import os
import sys

import clickhouse_connect
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, substring, sum as _sum

RAW_BASE = os.environ.get("RAW_BASE", "/data/raw")
CH_HOST = os.environ.get("CH_HOST", "clickhouse")
CH_PORT = int(os.environ.get("CH_PORT", "8123"))

input_glob = f"{RAW_BASE}/orders/dt=*/hour=*/*.jsonl.gz"

files = glob.glob(input_glob)
print(f"[batch] searching raw files at: {input_glob}", flush=True)
print(f"[batch] raw files found: {len(files)}", flush=True)

if not files:
    print("[batch] no raw files found; wait for writer to flush first", flush=True)
    sys.exit(0)

spark = (
    SparkSession.builder
    .appName("batch_views")
    .master("local[*]")
    .config("spark.sql.shuffle.partitions", "4")
    .getOrCreate()
)

try:
    raw = (
        spark.read
        .json(f"{RAW_BASE}/orders/*/*/*.jsonl.gz")
        .withColumn("day", substring(col("event_time"), 1, 10).cast("date"))
        .withColumn("hour", substring(col("event_time"), 12, 2).cast("int"))
        .withColumn("amount", col("quantity") * col("unit_price"))
    )

    total = raw.count()
    print(f"[batch] total raw rows read: {total}", flush=True)

    revenue_rows = (
        raw.groupBy("day", "category")
        .agg(
            _sum("amount").alias("revenue"),
            count("order_id").alias("orders"),
        )
        .collect()
    )

    top_item_rows = (
        raw.groupBy("day", "item_id")
        .agg(_sum("quantity").alias("quantity"))
        .collect()
    )

    orders_per_hour_rows = (
        raw.groupBy("day", "hour")
        .agg(count("order_id").alias("orders"))
        .collect()
    )

    client = clickhouse_connect.get_client(host=CH_HOST, port=CH_PORT)

    if revenue_rows:
        client.insert(
            "batch_revenue_by_category",
            [
                [
                    row["day"],
                    row["category"],
                    float(row["revenue"]),
                    int(row["orders"]),
                ]
                for row in revenue_rows
            ],
            column_names=["day", "category", "revenue", "orders"],
        )
        print(f"[batch] inserted {len(revenue_rows)} revenue rows", flush=True)

    if top_item_rows:
        client.insert(
            "batch_top_items",
            [
                [
                    row["day"],
                    row["item_id"],
                    int(row["quantity"]),
                ]
                for row in top_item_rows
            ],
            column_names=["day", "item_id", "quantity"],
        )
        print(f"[batch] inserted {len(top_item_rows)} item rows", flush=True)

    if orders_per_hour_rows:
        client.insert(
            "batch_orders_per_hour",
            [
                [
                    row["day"],
                    int(row["hour"]),
                    int(row["orders"]),
                ]
                for row in orders_per_hour_rows
            ],
            column_names=["day", "hour", "orders"],
        )
        print(f"[batch] inserted {len(orders_per_hour_rows)} hourly rows", flush=True)

finally:
    spark.stop()
