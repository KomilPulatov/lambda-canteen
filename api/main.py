import os

import clickhouse_connect
import redis
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles

CH_HOST = os.environ.get("CH_HOST", "clickhouse")
CH_PORT = int(os.environ.get("CH_PORT", "8123"))
REDIS_HOST = os.environ.get("REDIS_HOST", "redis")
REDIS_PORT = int(os.environ.get("REDIS_PORT", "6379"))

app = FastAPI(
    title="Lambda Canteen Query API",
    description="Merges ClickHouse batch views with Redis speed views.",
    version="1.0.0",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)


def clickhouse_rows(query: str, parameters: dict) -> list:
    client = clickhouse_connect.get_client(host=CH_HOST, port=CH_PORT)
    try:
        return client.query(query, parameters=parameters).result_rows
    finally:
        client.close()


@app.get("/health")
def health():
    return {"ok": True}


@app.get("/metrics/revenue-by-category")
def revenue_by_category(day: str):
    rows = clickhouse_rows(
        """
        SELECT category, sum(revenue)
        FROM batch_revenue_by_category FINAL
        WHERE day = %(day)s
        GROUP BY category
        """,
        {"day": day},
    )

    merged = {category: float(revenue) for category, revenue in rows}

    for category in ("meal", "snack", "drink"):
        delta = float(
            r.get(f"speed:revenue:category:{day}:{category}") or 0
        )
        merged[category] = merged.get(category, 0.0) + delta

    return {
        "day": day,
        "data": [
            {"category": category, "revenue": revenue}
            for category, revenue in sorted(merged.items())
        ],
    }


@app.get("/metrics/top-items")
def top_items(day: str, limit: int = 5):
    rows = clickhouse_rows(
        """
        SELECT item_id, sum(quantity)
        FROM batch_top_items FINAL
        WHERE day = %(day)s
        GROUP BY item_id
        """,
        {"day": day},
    )

    merged = {item_id: int(quantity) for item_id, quantity in rows}

    for key in r.scan_iter(f"speed:quantity:item:{day}:*"):
        item_id = key.split(":")[-1]
        delta = int(float(r.get(key) or 0))
        merged[item_id] = merged.get(item_id, 0) + delta

    top = sorted(merged.items(), key=lambda x: x[1], reverse=True)[:limit]

    return {
        "day": day,
        "data": [
            {"item_id": item_id, "quantity": quantity}
            for item_id, quantity in top
        ],
    }


@app.get("/metrics/orders-per-hour")
def orders_per_hour(day: str):
    rows = clickhouse_rows(
        """
        SELECT hour, sum(orders)
        FROM batch_orders_per_hour FINAL
        WHERE day = %(day)s
        GROUP BY hour
        """,
        {"day": day},
    )

    merged = {hour: int(orders) for hour, orders in rows}

    for hour in range(24):
        key = f"speed:orders:hour:{day}:{hour:02d}"
        delta = int(float(r.get(key) or 0))
        merged[hour] = merged.get(hour, 0) + delta

    return {
        "day": day,
        "data": [
            {"hour": f"{hour:02d}:00", "orders": merged.get(hour, 0)}
            for hour in range(24)
        ],
    }


app.mount("/", StaticFiles(directory="static", html=True), name="static")
