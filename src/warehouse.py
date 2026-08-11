"""Batch layer: build the star schema, then assert data quality.

    python -m src.warehouse --lake data/lake/events --db data/warehouse.duckdb

Quality checks are the point, not an afterthought: a warehouse nobody trusts
gets bypassed. Every check prints PASS/FAIL and the process exits non-zero on
failure, so it can gate a scheduled run.
"""
import argparse
import glob
import os
import sys

import duckdb

CHECKS = [
    (
        "no null keys in fact",
        """SELECT COUNT(*) FROM fact_live_engagement
           WHERE date_key IS NULL OR room_key IS NULL
              OR user_key IS NULL OR gift_key IS NULL""",
        0,
    ),
    (
        "no duplicate event_id in staging",
        "SELECT COUNT(*) - COUNT(DISTINCT event_id) FROM stg_events",
        0,
    ),
    (
        "referential integrity: fact.room_key -> dim_room",
        """SELECT COUNT(*) FROM fact_live_engagement f
           LEFT JOIN dim_room r USING (room_key) WHERE r.room_key IS NULL""",
        0,
    ),
    (
        "referential integrity: fact.user_key -> dim_user",
        """SELECT COUNT(*) FROM fact_live_engagement f
           LEFT JOIN dim_user u USING (user_key) WHERE u.user_key IS NULL""",
        0,
    ),
    (
        "coins never negative",
        "SELECT COUNT(*) FROM fact_live_engagement WHERE coins < 0",
        0,
    ),
    (
        "fact row count reconciles with staging",
        """SELECT ABS(
               (SELECT COALESCE(SUM(event_count), 0) FROM fact_live_engagement)
             - (SELECT COUNT(*) FROM stg_events))""",
        0,
    ),
]


def build(lake, db, sql_path):
    files = glob.glob(os.path.join(lake, "**", "*.parquet"), recursive=True)
    if not files:
        sys.exit(f"no parquet found under {lake} — run the streaming job first")
    print(f"loading {len(files)} parquet file(s) from {lake}")

    con = duckdb.connect(db)
    with open(sql_path, encoding="utf-8") as f:
        script = f.read()
    # DuckDB's parameter binding does not span multiple statements; inline the
    # validated glob instead and run the script as one batch.
    script = script.replace("$lake_events", repr(os.path.join(lake, "**", "*.parquet")))
    con.execute(script)

    print("\nmodel built:")
    for t in ["stg_events", "dim_date", "dim_room", "dim_user", "dim_gift",
              "fact_live_engagement"]:
        n = con.execute(f"SELECT COUNT(*) FROM {t}").fetchone()[0]
        print(f"  {t:<24} {n:>10,} rows")
    return con


def quality(con):
    print("\ndata quality checks:")
    failures = 0
    for name, sql, expected in CHECKS:
        got = con.execute(sql).fetchone()[0]
        ok = got == expected
        failures += 0 if ok else 1
        print(f"  [{'PASS' if ok else 'FAIL'}] {name} (got {got}, expected {expected})")
    return failures


def sample_queries(con):
    print("\ntop rooms by coins:")
    rows = con.execute(
        """SELECT r.room_id, SUM(f.coins) AS coins, SUM(f.gifts) AS gifts,
                  COUNT(DISTINCT f.user_key) AS unique_users
           FROM fact_live_engagement f JOIN dim_room r USING (room_key)
           GROUP BY r.room_id ORDER BY coins DESC LIMIT 5"""
    ).fetchall()
    for r in rows:
        print(f"  {r[0]}  coins={r[1]:>8,}  gifts={r[2]:>5,}  users={r[3]:>5,}")

    lag = con.execute(
        "SELECT ROUND(AVG(avg_ingest_lag_s), 2) FROM fact_live_engagement"
    ).fetchone()[0]
    print(f"\naverage ingest lag: {lag}s")


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--lake", default="data/lake/events")
    p.add_argument("--db", default="data/warehouse.duckdb")
    p.add_argument("--sql", default="sql/warehouse.sql")
    a = p.parse_args()

    con = build(a.lake, a.db, a.sql)
    failed = quality(con)
    sample_queries(con)
    con.close()
    print(f"\n{'ALL CHECKS PASSED' if not failed else f'{failed} CHECK(S) FAILED'}")
    sys.exit(1 if failed else 0)
