# Live-Streaming Data Warehouse (batch + real-time)

An end-to-end data pipeline for live-streaming engagement events: a **Kafka →
Spark Structured Streaming** real-time layer, an **offline star-schema
warehouse**, and **data quality checks** that gate the load.

```
                    ┌──────────────┐
  producer.py  ───▶ │    Kafka     │ ───┐
  (join/comment/    │  (Redpanda)  │    │
   gift/leave)      └──────────────┘    │
                                        ▼
                            ┌───────────────────────┐
                            │  Spark Structured     │
                            │  Streaming            │
                            │  • watermark (late)   │
                            │  • tumbling windows   │
                            └───────┬───────────────┘
                        ┌───────────┴───────────┐
                        ▼                       ▼
              data/lake/events         data/lake/room_metrics
              (raw, append)            (real-time serving)
                        │
                        ▼
              ┌──────────────────────┐
              │  DuckDB warehouse    │   dim_room ─┐
              │  star schema         │   dim_user ─┼──< fact_live_engagement
              │  + quality checks    │   dim_gift ─┤
              └──────────────────────┘   dim_date ─┘
```

## What each piece demonstrates

| File | Concern |
|---|---|
| `src/producer.py` | Event generation; deliberately emits ~3% **late events** |
| `src/streaming_job.py` | **Watermarking**, **tumbling-window** aggregation, dual sinks, checkpointing |
| `sql/warehouse.sql` | **Dimensional modeling** — conformed dimensions, declared fact grain |
| `src/warehouse.py` | **Data quality gates** — referential integrity, dedupe, reconciliation |

## Run it

```bash
pip install -r requirements.txt
```

**Local (no broker needed)** — the file source stands in for Kafka:

```bash
python -m src.producer --sink file --rate 300 --seconds 20 --outdir data/raw
python -m src.streaming_job --source file --indir data/raw --seconds 100
python -m src.warehouse --lake data/lake/events --db data/warehouse.duckdb
```

**With Kafka:**

```bash
docker compose up -d
python -m src.producer --sink kafka --rate 300 --seconds 60
python -m src.streaming_job --source kafka --bootstrap localhost:9092 --seconds 120
python -m src.warehouse
```

## Verified output

```
model built:
  stg_events                    6,000 rows
  dim_date                          1 rows
  dim_room                         50 rows
  dim_user                      3,498 rows
  dim_gift                          5 rows
  fact_live_engagement          5,942 rows

data quality checks:
  [PASS] no null keys in fact
  [PASS] no duplicate event_id in staging
  [PASS] referential integrity: fact.room_key -> dim_room
  [PASS] referential integrity: fact.user_key -> dim_user
  [PASS] coins never negative
  [PASS] fact row count reconciles with staging

average ingest lag: 3.18s
ALL CHECKS PASSED
```

## Design notes

- **Why a watermark?** The producer emits events up to 90s late on purpose.
  A 20s watermark bounds streaming state while still admitting most late
  arrivals; anything later is dropped rather than growing state forever.
- **Fact grain** is one row per (date, room, user, gift) rollup — the lowest
  level the serving queries need, which keeps the fact table narrow without
  losing the ability to re-aggregate.
- **Append-mode windows** only emit once the watermark passes the window end,
  so the defaults (30s window / 20s watermark) are tuned for a short local run.
  Production values would be minutes.
- **Reconciliation check** (`SUM(fact.event_count) == COUNT(stg_events)`) is
  the one that catches real bugs — a bad join silently drops or fans out rows.
