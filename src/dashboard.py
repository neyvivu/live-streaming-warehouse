"""BI layer: a Streamlit dashboard served from the star-schema warehouse.

Every metric is a SQL query against the fact table joined to its dimensions, so the
dashboard is a thin serving layer rather than a second copy of the business logic.

    streamlit run src/dashboard.py
"""
import os

import duckdb
import pandas as pd
import streamlit as st

DB = os.environ.get("WAREHOUSE_DB", "data/warehouse.duckdb")
RT_GLOB = "data/lake/room_metrics/**/*.parquet"

st.set_page_config(page_title="Live Engagement", layout="wide")


@st.cache_resource
def connect(path):
    if not os.path.exists(path):
        return None
    return duckdb.connect(path, read_only=True)


def q(con, sql):
    return con.execute(sql).fetchdf()


# ---------- queries: one per tile, all against the star schema ----------

HEADLINE = """
SELECT
    SUM(coins)                   AS coins,
    SUM(gifts)                   AS gifts,
    SUM(comments)                AS comments,
    COUNT(DISTINCT user_key)     AS users,
    COUNT(DISTINCT room_key)     AS rooms,
    ROUND(AVG(avg_ingest_lag_s), 2) AS ingest_lag_s
FROM fact_live_engagement
"""

TOP_ROOMS = """
SELECT r.room_id                       AS room,
       SUM(f.coins)                    AS coins,
       SUM(f.gifts)                    AS gifts,
       COUNT(DISTINCT f.user_key)      AS users,
       ROUND(SUM(f.coins) * 1.0 / NULLIF(COUNT(DISTINCT f.user_key), 0), 1) AS coins_per_user
FROM fact_live_engagement f
JOIN dim_room r USING (room_key)
GROUP BY r.room_id
ORDER BY coins DESC
LIMIT 15
"""

GIFT_MIX = """
SELECT g.gift_tier                AS tier,
       g.gift_name                AS gift,
       SUM(f.gifts)               AS sent,
       SUM(f.coins)               AS coins
FROM fact_live_engagement f
JOIN dim_gift g USING (gift_key)
WHERE g.gift_name <> 'none'
GROUP BY g.gift_tier, g.gift_name
ORDER BY coins DESC
"""

BY_COUNTRY = """
SELECT u.country                  AS country,
       COUNT(DISTINCT f.user_key) AS users,
       SUM(f.coins)               AS coins,
       SUM(f.comments)            AS comments
FROM fact_live_engagement f
JOIN dim_user u USING (user_key)
GROUP BY u.country
ORDER BY coins DESC
"""

ENGAGEMENT = """
SELECT SUM(joins) AS joins, SUM(comments) AS comments, SUM(gifts) AS gifts
FROM fact_live_engagement
"""

# The same six assertions the batch load enforces, surfaced for the business user.
CHECKS = [
    ("No null keys in fact",
     "SELECT COUNT(*) FROM fact_live_engagement WHERE date_key IS NULL OR room_key IS NULL OR user_key IS NULL OR gift_key IS NULL"),
    ("No duplicate event_id",
     "SELECT COUNT(*) - COUNT(DISTINCT event_id) FROM stg_events"),
    ("Fact -> dim_room integrity",
     "SELECT COUNT(*) FROM fact_live_engagement f LEFT JOIN dim_room r USING (room_key) WHERE r.room_key IS NULL"),
    ("Fact -> dim_user integrity",
     "SELECT COUNT(*) FROM fact_live_engagement f LEFT JOIN dim_user u USING (user_key) WHERE u.user_key IS NULL"),
    ("Coins never negative",
     "SELECT COUNT(*) FROM fact_live_engagement WHERE coins < 0"),
    ("Fact reconciles with staging",
     "SELECT ABS((SELECT COALESCE(SUM(event_count),0) FROM fact_live_engagement) - (SELECT COUNT(*) FROM stg_events))"),
]


def main():
    st.title("Live Engagement")
    st.caption("Served from the star-schema warehouse (fact_live_engagement + conformed dimensions)")

    con = connect(DB)
    if con is None:
        st.error(f"No warehouse found at {DB}. Run the pipeline first, then reload.")
        st.code("python -m src.producer --sink file --rate 300 --seconds 20 --outdir data/raw\n"
                "python -m src.streaming_job --source file --indir data/raw --seconds 100\n"
                "python -m src.warehouse --lake data/lake/events --db data/warehouse.duckdb")
        return

    h = q(con, HEADLINE).iloc[0]
    c = st.columns(6)
    c[0].metric("Coins", f"{int(h.coins):,}")
    c[1].metric("Gifts", f"{int(h.gifts):,}")
    c[2].metric("Comments", f"{int(h.comments):,}")
    c[3].metric("Viewers", f"{int(h.users):,}")
    c[4].metric("Rooms", f"{int(h.rooms):,}")
    c[5].metric("Ingest lag", f"{h.ingest_lag_s}s")

    left, right = st.columns([3, 2])

    with left:
        st.subheader("Top rooms by coins")
        rooms = q(con, TOP_ROOMS)
        st.bar_chart(rooms.set_index("room")["coins"])
        st.dataframe(rooms, use_container_width=True, hide_index=True)

    with right:
        st.subheader("Engagement mix")
        eng = q(con, ENGAGEMENT).iloc[0]
        st.bar_chart(pd.DataFrame(
            {"events": [int(eng.joins), int(eng.comments), int(eng.gifts)]},
            index=["joins", "comments", "gifts"],
        ))

        st.subheader("By country")
        st.dataframe(q(con, BY_COUNTRY), use_container_width=True, hide_index=True)

    st.subheader("Gift revenue mix")
    st.dataframe(q(con, GIFT_MIX), use_container_width=True, hide_index=True)

    # Real-time layer, read straight from the streaming sink
    st.subheader("Real-time windows (streaming layer)")
    try:
        rt = con.execute(f"""
            SELECT window_start, window_end, room_id, unique_viewers, gifts, coins
            FROM read_parquet('{RT_GLOB}')
            ORDER BY window_start DESC, coins DESC
            LIMIT 20
        """).fetchdf()
        st.dataframe(rt, use_container_width=True, hide_index=True)
    except Exception:
        st.info("No windowed metrics yet — the streaming job emits them once the watermark "
                "passes each window end.")

    st.subheader("Data quality")
    rows = []
    for name, sql in CHECKS:
        got = con.execute(sql).fetchone()[0]
        rows.append({"check": name, "violations": got, "status": "PASS" if got == 0 else "FAIL"})
    dq = pd.DataFrame(rows)
    failed = (dq.status == "FAIL").sum()
    (st.success if failed == 0 else st.error)(
        f"{len(dq) - failed}/{len(dq)} checks passing"
    )
    st.dataframe(dq, use_container_width=True, hide_index=True)


if __name__ == "__main__":
    main()
