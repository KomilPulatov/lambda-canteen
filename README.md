# Lambda Canteen Analytics

Implementation of a Lambda Architecture analytics system.

## Stack

- Kafka: event ingestion
- Local gzip JSONL archive: immutable raw storage
- Prefect + PySpark: batch recomputation
- ClickHouse: batch serving store
- Flink + Redis: speed layer
- FastAPI: query API
- Chart.js: dashboard

## Run commands

Copy `.env.example` to `.env` if you need to change host ports on the droplet.

```bash
docker compose up -d --build zookeeper kafka clickhouse redis jobmanager taskmanager

docker compose exec kafka kafka-topics \
  --create --if-not-exists \
  --topic canteen_orders \
  --bootstrap-server kafka:9092 \
  --partitions 3 \
  --replication-factor 1

docker compose exec -T clickhouse clickhouse-client \
  --multiquery < batch/sql/init.sql

docker compose up -d --build producer writer api

docker compose run --rm speed

docker compose run --rm batch
```

PowerShell equivalent for loading ClickHouse SQL:

```powershell
Get-Content batch/sql/init.sql | docker compose exec -T clickhouse clickhouse-client --multiquery
```

Useful URLs:

- Dashboard: http://localhost:8000/
- Swagger UI: http://localhost:8000/docs
- Flink UI: http://localhost:8081/ by default, or your `.env` `FLINK_UI_PORT` value
