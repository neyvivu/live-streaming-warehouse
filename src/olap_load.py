"""OLAP serving layer: load the star schema into ClickHouse and benchmark it.

The DuckDB warehouse is where the model lives. This pushes the same facts into
ClickHouse, which is where a dashboard would actually query them at scale.

    python -m src.olap_load                      # embedded engine, no server
    python -m src.olap_load --server localhost   # a real ClickHouse instance

Embedded mode uses chdb, which is ClickHouse compiled as a library, so the SQL
and the storage engines are identical to a server. That keeps the project
runnable without Docker while still being real ClickHouse.
"""
import argparse
import os
import sys
import time

FACT_QUERY = """
SELECT
    d.full_date                    AS event_date,
    r.room_id                      AS room_id,
    u.user_id                      AS user_id,
    g.gift_name                    AS gift_name,
    g.gift_tier                    AS gift_tier,
    u.country                      AS country,
    f.event_count, f.joins, f.comments, f.gifts, f.coins
FROM fact_live_engagement f
JOIN dim_date d USING (date_key)
JOIN dim_room r USING (room_key)
JOIN dim_user u USING (user_key)
JOIN dim_gift g USING (gift_key)
"""


def read_star_schema(db_path):
    """Flatten the star schema into the wide row ClickHouse stores."""
    import duckdb
    if not os.path.exists(db_path):
        sys.exit(f"no warehouse at {db_path}. Run the pipeline first.")
    con = duckdb.connect(db_path, read_only=True)
    rows = con.execute(FACT_QUERY).fetchall()
    con.close()
    return rows


def sql_values(rows, batch=5000):
    """ClickHouse ingests best in large batches, not row by row."""
    for i in range(0, len(rows), batch):
        chunk = rows[i:i + batch]
        yield ",".join(
            "('{}','{}','{}','{}','{}','{}',{},{},{},{},{})".format(
                r[0], r[1], r[2], r[3], r[4], r[5],
                int(r[6]), int(r[7]), int(r[8]), int(r[9]), int(r[10])
            )
            for r in chunk
        )


class Embedded:
    """chdb: ClickHouse as an in-process library."""

    def __init__(self):
        import chdb.session
        self.s = chdb.session.Session()

    def exec(self, sql):
        return self.s.query(sql)

    def show(self, sql):
        return self.s.query(sql, "PrettyCompact")


class Server:
    """A real ClickHouse server over the native protocol."""

    def __init__(self, host):
        import clickhouse_connect
        self.c = clickhouse_connect.get_client(host=host)

    def exec(self, sql):
        return self.c.command(sql)

    def show(self, sql):
        rows = self.c.query(sql)
        return "\n".join(str(r) for r in rows.result_rows)


def run_ddl(engine, path):
    with open(path, encoding="utf-8") as f:
        script = f.read()
    # Strip comment lines first, otherwise they attach to the next statement.
    body = "\n".join(
        line for line in script.splitlines()
        if not line.strip().startswith("--")
    )
    # ClickHouse takes one statement per call.
    for stmt in (s.strip() for s in body.split(";")):
        if stmt:
            engine.exec(stmt)


BENCHMARKS = [
    ("Revenue by gift tier", """
        SELECT gift_tier, sum(coins) AS coins, sum(gifts) AS gifts
        FROM live.fact_live_engagement
        GROUP BY gift_tier ORDER BY coins DESC
    """),
    ("Top rooms by coins", """
        SELECT room_id, sum(coins) AS coins, uniq(user_id) AS users
        FROM live.fact_live_engagement
        GROUP BY room_id ORDER BY coins DESC LIMIT 10
    """),
    ("Country breakdown", """
        SELECT country, uniq(user_id) AS users, sum(coins) AS coins
        FROM live.fact_live_engagement
        GROUP BY country ORDER BY coins DESC
    """),
    ("Daily rollup, read from the materialized view", """
        SELECT event_date, room_id,
               sumMerge(coins) AS coins,
               uniqMerge(uniq_users) AS users
        FROM live.agg_room_daily
        GROUP BY event_date, room_id
        ORDER BY coins DESC LIMIT 10
    """),
    ("Running total per room (window function)", """
        SELECT room_id, event_date, coins,
               sum(coins) OVER (PARTITION BY room_id ORDER BY event_date
                                ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running
        FROM (
            SELECT room_id, event_date, sum(coins) AS coins
            FROM live.fact_live_engagement GROUP BY room_id, event_date
        )
        ORDER BY room_id, event_date LIMIT 10
    """),
]


def main(a):
    engine = Server(a.server) if a.server else Embedded()
    mode = f"server {a.server}" if a.server else "embedded (chdb)"
    print(f"ClickHouse mode: {mode}\n")

    print("creating schema")
    run_ddl(engine, a.ddl)

    print(f"reading star schema from {a.db}")
    rows = read_star_schema(a.db)
    print(f"  {len(rows):,} fact rows to load")

    t0 = time.time()
    for values in sql_values(rows):
        engine.exec(f"INSERT INTO live.fact_live_engagement VALUES {values}")
    print(f"  loaded in {time.time() - t0:.2f}s\n")

    print("partitions on disk:")
    print(engine.show("""
        SELECT partition, sum(rows) AS rows, formatReadableSize(sum(bytes_on_disk)) AS size
        FROM system.parts
        WHERE database='live' AND table='fact_live_engagement' AND active
        GROUP BY partition ORDER BY partition
    """))

    print("\nqueries:")
    for name, sql in BENCHMARKS:
        t0 = time.time()
        out = engine.show(sql)
        ms = (time.time() - t0) * 1000
        print(f"\n--- {name}  ({ms:.1f} ms) ---")
        print(out)


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--db", default="data/warehouse.duckdb")
    p.add_argument("--ddl", default="sql/clickhouse_schema.sql")
    p.add_argument("--server", default=None,
                   help="host of a running ClickHouse; omit to use the embedded engine")
    main(p.parse_args())
