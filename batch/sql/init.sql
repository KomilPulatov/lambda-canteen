CREATE TABLE IF NOT EXISTS batch_revenue_by_category (
    day Date,
    category LowCardinality(String),
    revenue Float64,
    orders UInt64,
    updated_at DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree(updated_at)
ORDER BY (day, category);

CREATE TABLE IF NOT EXISTS batch_top_items (
    day Date,
    item_id LowCardinality(String),
    quantity UInt64,
    updated_at DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree(updated_at)
ORDER BY (day, item_id);

CREATE TABLE IF NOT EXISTS batch_orders_per_hour (
    day Date,
    hour UInt8,
    orders UInt64,
    updated_at DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree(updated_at)
ORDER BY (day, hour);
