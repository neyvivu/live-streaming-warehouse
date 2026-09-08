-- OLAP serving layer in ClickHouse.
--
-- The DuckDB star schema is the modelling layer. This is the serving layer:
-- the same facts laid out for fast slice-and-dice at scale, plus pre-aggregated
-- rollups that dashboards read instead of scanning the detail every time.
--
-- Three ClickHouse features carry the weight here:
--   MergeTree           columnar storage with a sorting key, so range scans on
--                       the sorting prefix skip most granules
--   PARTITION BY month  whole partitions are pruned when the query filters on date
--   LowCardinality      dictionary encoding for repeated strings, big win on
--                       room_id, country and gift columns

CREATE DATABASE IF NOT EXISTS live;

-- ---------- detail layer (DWD) ----------

DROP TABLE IF EXISTS live.fact_live_engagement;

CREATE TABLE live.fact_live_engagement
(
    event_date   Date,
    room_id      LowCardinality(String),
    user_id      String,
    gift_name    LowCardinality(String),
    gift_tier    LowCardinality(String),
    country      LowCardinality(String),
    event_count  UInt32,
    joins        UInt32,
    comments     UInt32,
    gifts        UInt32,
    coins        UInt64
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(event_date)
ORDER BY (event_date, room_id, user_id);

-- ORDER BY is the sorting key, not a uniqueness constraint. Queries that filter
-- on a prefix of it (event_date, then room_id) skip granules instead of scanning.

-- ---------- rollup layer (DWS), maintained automatically ----------

DROP TABLE IF EXISTS live.agg_room_daily;

CREATE TABLE live.agg_room_daily
(
    event_date  Date,
    room_id     LowCardinality(String),
    coins       AggregateFunction(sum, UInt64),
    gifts       AggregateFunction(sum, UInt32),
    comments    AggregateFunction(sum, UInt32),
    uniq_users  AggregateFunction(uniq, String)
)
ENGINE = AggregatingMergeTree
PARTITION BY toYYYYMM(event_date)
ORDER BY (event_date, room_id);

-- The materialized view is an insert trigger: every batch written to the fact
-- table is aggregated and merged into the rollup. No scheduled job to run, and
-- no window where the rollup is stale relative to the detail.

DROP VIEW IF EXISTS live.mv_room_daily;

CREATE MATERIALIZED VIEW live.mv_room_daily TO live.agg_room_daily AS
SELECT
    event_date,
    room_id,
    sumState(coins)     AS coins,
    sumState(gifts)     AS gifts,
    sumState(comments)  AS comments,
    uniqState(user_id)  AS uniq_users
FROM live.fact_live_engagement
GROUP BY event_date, room_id;

-- uniq uses HyperLogLog, so distinct user counts stay cheap at scale. It is
-- approximate; uniqExact is available when the number has to be exact.
