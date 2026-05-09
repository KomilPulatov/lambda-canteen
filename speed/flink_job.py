import json
import os

import redis as redis_lib
from pyflink.common.serialization import SimpleStringSchema
from pyflink.common.watermark_strategy import WatermarkStrategy
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import KafkaOffsetsInitializer, KafkaSource
from pyflink.datastream.functions import MapFunction

KAFKA_BROKER = os.environ.get("KAFKA_BROKER", "kafka:9092")
KAFKA_TOPIC = os.environ.get("KAFKA_TOPIC", "canteen_orders")
REDIS_HOST = os.environ.get("REDIS_HOST", "redis")
REDIS_PORT = int(os.environ.get("REDIS_PORT", "6379"))
SPEED_TTL = int(os.environ.get("SPEED_TTL", "7200"))


class SpeedAggregator(MapFunction):
    _r = None

    def _client(self):
        if SpeedAggregator._r is None:
            SpeedAggregator._r = redis_lib.Redis(
                host=REDIS_HOST,
                port=REDIS_PORT,
                decode_responses=True,
            )
        return SpeedAggregator._r

    def map(self, raw: str) -> str:
        event = json.loads(raw)

        day = event["event_time"][:10]
        hour = event["event_time"][11:13]
        category = event["category"]
        item_id = event["item_id"]

        amount = event["quantity"] * event["unit_price"]

        keys_and_values = {
            f"speed:revenue:category:{day}:{category}": amount,
            f"speed:quantity:item:{day}:{item_id}": event["quantity"],
            f"speed:orders:hour:{day}:{hour}": 1,
        }

        r = self._client()

        for key, value in keys_and_values.items():
            r.incrbyfloat(key, value)
            if r.ttl(key) == -1:
                r.expire(key, SPEED_TTL)

        return raw


def main():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)

    source = (
        KafkaSource.builder()
        .set_bootstrap_servers(KAFKA_BROKER)
        .set_topics(KAFKA_TOPIC)
        .set_group_id("flink-speed-v1")
        .set_starting_offsets(KafkaOffsetsInitializer.latest())
        .set_value_only_deserializer(SimpleStringSchema())
        .build()
    )

    stream = env.from_source(
        source,
        WatermarkStrategy.no_watermarks(),
        "KafkaSource",
    )

    stream.map(SpeedAggregator()).print()

    env.execute("canteen-speed-job")


if __name__ == "__main__":
    main()