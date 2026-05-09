import os
import subprocess

import clickhouse_connect
from prefect import flow, task

CH_HOST = os.environ.get("CH_HOST", "clickhouse")
CH_PORT = int(os.environ.get("CH_PORT", "8123"))


@task(name="run-spark-job")
def run_spark_job():
    subprocess.run(
        ["spark-submit", "/app/spark_jobs/build_batch_views.py"],
        check=True,
    )


@task(name="dedupe-and-verify")
def dedupe_and_verify():
    client = clickhouse_connect.get_client(host=CH_HOST, port=CH_PORT)

    tables = (
        "batch_revenue_by_category",
        "batch_top_items",
        "batch_orders_per_hour",
    )

    for table in tables:
        client.command(f"OPTIMIZE TABLE {table} FINAL DEDUPLICATE")
        n = client.query(f"SELECT count() FROM {table}").result_rows[0][0]
        print(f"[prefect] {table} now contains {n} rows", flush=True)


@flow(name="batch-recompute-flow")
def batch_recompute_flow():
    run_spark_job()
    dedupe_and_verify()


if __name__ == "__main__":
    batch_recompute_flow()
