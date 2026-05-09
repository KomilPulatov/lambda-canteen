import gzip
import json
import os
import time
import uuid
from datetime import datetime, timezone

from confluent_kafka import Consumer, KafkaError

BROKER = os.environ.get("KAFKA_BROKER", "kafka:9092")
TOPIC = os.environ.get("KAFKA_TOPIC", "canteen_orders")
GROUP = os.environ.get("KAFKA_GROUP", "raw-writer-v1")
FLUSH_INTERVAL = int(os.environ.get("FLUSH_INTERVAL", "60"))
RAW_BASE = os.environ.get("RAW_BASE", "/data/raw")

consumer = Consumer({
    "bootstrap.servers": BROKER,
    "group.id": GROUP,
    "auto.offset.reset": "earliest",
    "enable.auto.commit": False,
})

consumer.subscribe([TOPIC])


def partition_path(dt: datetime) -> str:
    return (
        f"{RAW_BASE}/orders/"
        f"dt={dt.strftime('%Y-%m-%d')}/"
        f"hour={dt.strftime('%H')}"
    )


def flush(records: list, dt: datetime) -> None:
    if not records:
        return

    directory = partition_path(dt)
    os.makedirs(directory, exist_ok=True)

    filename = f"{directory}/part-{uuid.uuid4().hex[:8]}.jsonl.gz"

    with gzip.open(filename, "wb") as gz:
        for record in records:
            gz.write((json.dumps(record) + "\n").encode("utf-8"))

    print(f"[writer] wrote {len(records)} records -> {filename}", flush=True)


print(f"[writer] consuming {TOPIC} from {BROKER}", flush=True)

buffer = []
window_start = time.monotonic()

try:
    while True:
        msg = consumer.poll(timeout=1.0)

        if msg is None:
            pass
        elif msg.error():
            if msg.error().code() != KafkaError._PARTITION_EOF:
                raise RuntimeError(msg.error())
        else:
            buffer.append(json.loads(msg.value()))

        if time.monotonic() - window_start >= FLUSH_INTERVAL:
            if buffer:
                now = datetime.now(timezone.utc)
                flush(buffer, now)
                consumer.commit(asynchronous=False)
                buffer.clear()

            window_start = time.monotonic()

except KeyboardInterrupt:
    pass
finally:
    consumer.close()