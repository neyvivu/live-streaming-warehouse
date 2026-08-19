-- Offline warehouse: dimensional (star schema) model over the raw event lake.
--
--   dim_room ─┐
--   dim_user ─┼──< fact_live_engagement >── dim_date
--   dim_gift ─┘
--
-- Grain of the fact table: one row per (room, user, date, gift) engagement
-- rollup, the lowest level the serving queries actually need.

CREATE OR REPLACE TABLE stg_events AS
SELECT
    event_id,
    event_type,
    CAST(event_ts   AS TIMESTAMP) AS event_ts,
    CAST(ingest_ts  AS TIMESTAMP) AS ingest_ts,
    room_id,
    user_id,
    country,
    COALESCE(gift_name, 'none')   AS gift_name,
    COALESCE(coins, 0)            AS coins,
    COALESCE(comment_len, 0)      AS comment_len
FROM read_parquet($lake_events)
WHERE event_id IS NOT NULL
  AND event_ts IS NOT NULL;

-- ---------- dimensions ----------

CREATE OR REPLACE TABLE dim_date AS
SELECT
    CAST(strftime(d, '%Y%m%d') AS INTEGER) AS date_key,
    d                                       AS full_date,
    EXTRACT(year    FROM d)                 AS year,
    EXTRACT(month   FROM d)                 AS month,
    EXTRACT(day     FROM d)                 AS day,
    EXTRACT(dow     FROM d)                 AS day_of_week,
    CASE WHEN EXTRACT(dow FROM d) IN (0, 6) THEN TRUE ELSE FALSE END AS is_weekend
FROM (SELECT DISTINCT CAST(event_ts AS DATE) AS d FROM stg_events);

CREATE OR REPLACE TABLE dim_room AS
SELECT
    ROW_NUMBER() OVER (ORDER BY room_id) AS room_key,
    room_id,
    MIN(event_ts)                        AS first_seen_ts,
    MAX(event_ts)                        AS last_seen_ts
FROM stg_events
GROUP BY room_id;

CREATE OR REPLACE TABLE dim_user AS
SELECT
    ROW_NUMBER() OVER (ORDER BY user_id) AS user_key,
    user_id,
    -- country of the user's first observed event: a simple type-1 attribute
    ARG_MIN(country, event_ts)           AS country,
    MIN(event_ts)                        AS first_seen_ts
FROM stg_events
GROUP BY user_id;

CREATE OR REPLACE TABLE dim_gift AS
SELECT
    ROW_NUMBER() OVER (ORDER BY gift_name) AS gift_key,
    gift_name,
    MAX(coins)                             AS coin_value,
    CASE
        WHEN MAX(coins) >= 1000 THEN 'premium'
        WHEN MAX(coins) >= 100  THEN 'high'
        WHEN MAX(coins) >= 10   THEN 'mid'
        WHEN MAX(coins) >= 1    THEN 'low'
        ELSE 'none'
    END                                    AS gift_tier
FROM stg_events
GROUP BY gift_name;

-- ---------- fact ----------

CREATE OR REPLACE TABLE fact_live_engagement AS
SELECT
    d.date_key,
    r.room_key,
    u.user_key,
    g.gift_key,
    COUNT(*)                                                         AS event_count,
    SUM(CASE WHEN s.event_type = 'join'    THEN 1 ELSE 0 END)        AS joins,
    SUM(CASE WHEN s.event_type = 'comment' THEN 1 ELSE 0 END)        AS comments,
    SUM(CASE WHEN s.event_type = 'gift'    THEN 1 ELSE 0 END)        AS gifts,
    SUM(s.coins)                                                     AS coins,
    SUM(s.comment_len)                                               AS comment_chars,
    -- pipeline latency: how long ingestion trailed the event itself
    AVG(EXTRACT(epoch FROM (s.ingest_ts - s.event_ts)))              AS avg_ingest_lag_s
FROM stg_events s
JOIN dim_date d ON d.full_date = CAST(s.event_ts AS DATE)
JOIN dim_room r ON r.room_id   = s.room_id
JOIN dim_user u ON u.user_id   = s.user_id
JOIN dim_gift g ON g.gift_name = s.gift_name
GROUP BY d.date_key, r.room_key, u.user_key, g.gift_key;
