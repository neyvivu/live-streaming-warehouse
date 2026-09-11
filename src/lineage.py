"""
Governance layer for the live-streaming warehouse.

Three things, all derived from sql/warehouse.sql rather than declared by hand:

  1. column-level lineage  - where does a column actually come from
  2. impact analysis       - if an upstream column changes, what breaks
  3. catalog checks        - which tables are missing owner / grain /
                             freshness target / data classification

The point of parsing the SQL is that a hand-written lineage document is wrong
the moment somebody edits a query. A parsed one cannot drift.

Usage:
    python -m src.lineage                 # full report
    python -m src.lineage --column coins  # impact of one upstream column
    python -m src.lineage --json          # machine-readable output
"""

from __future__ import annotations

import argparse
import json
import re
from dataclasses import dataclass, field, asdict
from pathlib import Path

import sqlglot
from sqlglot import exp
from sqlglot.lineage import lineage as sqlglot_lineage

DIALECT = "duckdb"
DEFAULT_SQL = Path(__file__).resolve().parent.parent / "sql" / "warehouse.sql"


# --------------------------------------------------------------------------
# catalog: the metadata a table is expected to carry
# --------------------------------------------------------------------------

@dataclass
class TableMeta:
    owner: str | None = None
    grain: str | None = None
    freshness_target: str | None = None
    classification: str | None = None  # public | internal | restricted

    def gaps(self) -> list[str]:
        return [k for k, v in asdict(self).items() if not v]


CATALOG: dict[str, TableMeta] = {
    "stg_events": TableMeta(
        owner="data-platform",
        grain="one row per raw engagement event",
        freshness_target="15 minutes",
        classification="internal",
    ),
    "dim_date": TableMeta(
        owner="data-platform",
        grain="one row per calendar date",
        freshness_target="daily",
        classification="public",
    ),
    "dim_room": TableMeta(
        owner="data-platform",
        grain="one row per room_id",
        freshness_target="daily",
        classification="internal",
    ),
    "dim_user": TableMeta(
        owner="data-platform",
        grain="one row per user_id",
        freshness_target="daily",
        # deliberately left unset: user country is personal data and the
        # classification has not been signed off, so the check should flag it
        classification=None,
    ),
    "dim_gift": TableMeta(
        owner="data-platform",
        grain="one row per gift_name",
        freshness_target="daily",
        classification="public",
    ),
    "fact_live_engagement": TableMeta(
        owner="analytics-engineering",
        grain="one row per (date, room, user, gift)",
        freshness_target="hourly",
        classification="internal",
    ),
}


# --------------------------------------------------------------------------
# parsing
# --------------------------------------------------------------------------

@dataclass
class Model:
    """One CREATE TABLE ... AS SELECT statement from warehouse.sql."""
    name: str
    select_sql: str
    columns: list[str] = field(default_factory=list)
    sources: list[str] = field(default_factory=list)


def _strip_params(sql: str) -> str:
    """DuckDB prepared parameters ($lake_events) are not valid to parse."""
    return re.sub(r"\$[A-Za-z_][A-Za-z0-9_]*", "'__param__'", sql)


def parse_models(sql_path: Path = DEFAULT_SQL) -> dict[str, Model]:
    raw = _strip_params(sql_path.read_text(encoding="utf-8"))
    models: dict[str, Model] = {}

    for statement in sqlglot.parse(raw, read=DIALECT):
        if not isinstance(statement, exp.Create):
            continue
        target = statement.this
        if isinstance(target, exp.Schema):
            target = target.this
        name = target.name
        select = statement.expression
        if select is None:
            continue

        cols = [
            (p.alias_or_name or f"col_{i}")
            for i, p in enumerate(select.selects)
        ] if isinstance(select, exp.Select) else []

        sources = sorted({
            t.name for t in select.find_all(exp.Table)
            if t.name and not t.name.startswith("read_")
        })

        models[name] = Model(
            name=name,
            select_sql=select.sql(dialect=DIALECT),
            columns=cols,
            sources=sources,
        )
    return models


def build_schema(models: dict[str, Model]) -> dict[str, dict[str, str]]:
    """sqlglot needs a schema to resolve unqualified columns."""
    return {name: {c: "UNKNOWN" for c in m.columns} for name, m in models.items()}


# --------------------------------------------------------------------------
# column-level lineage
# --------------------------------------------------------------------------

def column_lineage(
    model: str,
    column: str,
    models: dict[str, Model],
    schema: dict[str, dict[str, str]],
) -> list[str]:
    """Return the upstream 'table.column' leaves feeding one output column."""
    node = sqlglot_lineage(
        column,
        models[model].select_sql,
        schema=schema,
        dialect=DIALECT,
    )
    leaves: list[str] = []

    def walk(n) -> None:
        if not n.downstream:
            src = n.source
            table = None
            if isinstance(src, exp.Table):
                table = src.name
            else:
                found = src.find(exp.Table) if src is not None else None
                table = found.name if found is not None else None
            col = n.name.split(".")[-1]
            if table is None:
                leaves.append(col)
            elif table in models and col not in models[table].columns:
                # COUNT(*) and friends depend on the row, not on any one column
                leaves.append(f"{table}.*")
            else:
                leaves.append(f"{table}.{col}")
            return
        for d in n.downstream:
            walk(d)

    walk(node)
    return sorted(set(leaves))


def full_lineage(models: dict[str, Model]) -> dict[str, dict[str, list[str]]]:
    schema = build_schema(models)
    out: dict[str, dict[str, list[str]]] = {}
    for name, m in models.items():
        if not m.sources:            # staging reads the lake, nothing upstream
            continue
        out[name] = {}
        for col in m.columns:
            try:
                out[name][col] = column_lineage(name, col, models, schema)
            except Exception as err:                  # noqa: BLE001
                out[name][col] = [f"<unresolved: {type(err).__name__}>"]
    return out


# --------------------------------------------------------------------------
# impact analysis
# --------------------------------------------------------------------------

def impact_of(upstream: str, lineage_map: dict[str, dict[str, list[str]]]
              ) -> dict[str, list[str]]:
    """
    Which downstream columns actually carry `upstream` (e.g. 'stg_events.coins').

    This is column-aware on purpose. Table-level lineage would claim every
    dimension is affected by every staging column, which is useless for
    deciding what to re-test.
    """
    hits: dict[str, list[str]] = {}
    for model, cols in lineage_map.items():
        affected = sorted(c for c, leaves in cols.items() if upstream in leaves)
        if affected:
            hits[model] = affected
    return hits


# --------------------------------------------------------------------------
# catalog checks
# --------------------------------------------------------------------------

def catalog_report(models: dict[str, Model]) -> list[tuple[str, list[str]]]:
    rows: list[tuple[str, list[str]]] = []
    for name in models:
        meta = CATALOG.get(name)
        rows.append((name, ["not in catalog"] if meta is None else meta.gaps()))
    return rows


# --------------------------------------------------------------------------
# reporting
# --------------------------------------------------------------------------

def print_report(models: dict[str, Model],
                 lineage_map: dict[str, dict[str, list[str]]]) -> int:
    print("=" * 74)
    print("MODELS PARSED FROM sql/warehouse.sql")
    print("=" * 74)
    for name, m in models.items():
        print(f"  {name:<24} {len(m.columns):>2} cols   sources: "
              f"{', '.join(m.sources) or '(lake)'}")

    print()
    print("=" * 74)
    print("COLUMN-LEVEL LINEAGE")
    print("=" * 74)
    for model, cols in lineage_map.items():
        print(f"\n  {model}")
        for col, leaves in cols.items():
            print(f"    {col:<22} <- {', '.join(leaves)}")

    print()
    print("=" * 74)
    print("IMPACT ANALYSIS (column-aware)")
    print("=" * 74)
    for probe in ("stg_events.coins", "stg_events.country", "stg_events.event_ts"):
        hits = impact_of(probe, lineage_map)
        print(f"\n  if {probe} changes:")
        if not hits:
            print("    nothing downstream carries it")
        for model, cols in hits.items():
            print(f"    {model:<24} {', '.join(cols)}")

    print()
    print("=" * 74)
    print("CATALOG GOVERNANCE CHECK")
    print("=" * 74)
    failures = 0
    for name, gaps in catalog_report(models):
        if gaps:
            failures += 1
            print(f"  [GAP ] {name:<24} missing: {', '.join(gaps)}")
        else:
            print(f"  [ OK ] {name}")

    print()
    if failures:
        print(f"{failures} table(s) missing required metadata.")
    else:
        print("All tables carry owner, grain, freshness target and classification.")
    return failures


def main() -> None:
    ap = argparse.ArgumentParser(description="Lineage and governance report.")
    ap.add_argument("--sql", type=Path, default=DEFAULT_SQL)
    ap.add_argument("--column", help="upstream column, e.g. stg_events.coins")
    ap.add_argument("--json", action="store_true", help="machine-readable output")
    args = ap.parse_args()

    models = parse_models(args.sql)
    lineage_map = full_lineage(models)

    if args.column:
        probe = args.column if "." in args.column else f"stg_events.{args.column}"
        hits = impact_of(probe, lineage_map)
        if args.json:
            print(json.dumps({"column": probe, "impact": hits}, indent=2))
        else:
            print(f"impact of {probe}:")
            if not hits:
                print("  nothing downstream carries it")
            for model, cols in hits.items():
                print(f"  {model:<24} {', '.join(cols)}")
        return

    if args.json:
        print(json.dumps({
            "models": {k: asdict(v) for k, v in models.items()},
            "lineage": lineage_map,
            "catalog_gaps": {n: g for n, g in catalog_report(models) if g},
        }, indent=2))
        return

    print_report(models, lineage_map)


if __name__ == "__main__":
    main()
