import json
import os
import random
import time
import uuid
from datetime import datetime, timezone

from kafka import KafkaProducer

BROKER = os.environ.get("KAFKA_BROKER", "kafka:9092")
TOPIC = os.environ.get("KAFKA_TOPIC", "canteen_orders")

producer = KafkaProducer(
    bootstrap_servers=BROKER,
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
)

CATEGORIES = ["meal", "snack", "drink"]
ITEMS = ["i-1", "i-2", "i-3", "i-4", "i-5", "i-6", "i-7", "i-8"]
PRICES = [8000, 10000, 12000, 15000, 18000, 22000]

print(f"[producer] sending to {BROKER} topic={TOPIC}", flush=True)

while True:
    event = {
        "order_id": f"o-{uuid.uuid4().hex[:12]}",
        "user_id": f"u-{random.randint(1, 200)}",
        "item_id": random.choice(ITEMS),
        "category": random.choice(CATEGORIES),
        "quantity": random.randint(1, 3),
        "unit_price": random.choice(PRICES),
        "event_time": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
    }

    producer.send(TOPIC, event)
    producer.flush()
    print(f"[producer] {event}", flush=True)
    time.sleep(0.5)