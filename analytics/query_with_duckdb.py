"""
query_with_duckdb.py
--------------------
Query Iceberg tables locally using DuckDB + PyIceberg.

DuckDB reads Iceberg metadata via PyIceberg, then queries the Parquet files
directly from MinIO — no Flink JVM required.

Features demonstrated:
  - Basic SELECT queries
  - Time travel (query a previous snapshot)
  - Read from a Nessie branch

Usage:
    python analytics/query_with_duckdb.py
    python analytics/query_with_duckdb.py --branch dev
    python analytics/query_with_duckdb.py --snapshot 1234567890
"""

import argparse
import sys
from pathlib import Path
import yaml
import duckdb

CONFIG_PATH = Path(__file__).parent.parent / "catalog" / "catalog_config.yaml"


def load_catalog(cfg, ref: str = None):
    from pyiceberg.catalog import load_catalog

    cat_cfg = cfg["catalog"]
    s3_cfg = cfg["storage"]["s3"]

    return load_catalog(
        name=cat_cfg["name"],
        **{
            "type": "nessie",
            "uri": cat_cfg["uri"],
            "ref": ref or cat_cfg["ref"],
            "warehouse": cat_cfg["warehouse"],
            "s3.endpoint": s3_cfg["endpoint"],
            "s3.access-key-id": s3_cfg["access_key_id"],
            "s3.secret-access-key": s3_cfg["secret_access_key"],
            "s3.region": s3_cfg["region"],
            "s3.path-style-access": str(s3_cfg["path_style_access"]).lower(),
        },
    )


def setup_duckdb_s3(cfg):
    """Configure DuckDB httpfs for MinIO (path-style S3)."""
    s3_cfg = cfg["storage"]["s3"]
    con = duckdb.connect()
    con.execute("INSTALL httpfs; LOAD httpfs;")
    con.execute(f"""
        SET s3_endpoint = '{s3_cfg["endpoint"].replace("http://", "")}';
        SET s3_access_key_id = '{s3_cfg["access_key_id"]}';
        SET s3_secret_access_key = '{s3_cfg["secret_access_key"]}';
        SET s3_region = '{s3_cfg["region"]}';
        SET s3_use_ssl = false;
        SET s3_url_style = 'path';
    """)
    return con


def query_latest(catalog, con, table_id: str, limit: int = 20):
    """Query the latest snapshot of a table."""
    print(f"\n=== Latest snapshot: {table_id} ===")
    table = catalog.load_table(table_id)
    arrow = table.scan(limit=limit).to_arrow()
    result = con.execute("SELECT * FROM arrow").fetchdf()
    print(result.to_string())
    print(f"\n{len(result)} rows | {len(result.columns)} columns")
    return result


def query_snapshot(catalog, con, table_id: str, snapshot_id: int, limit: int = 20):
    """Time travel: query a specific snapshot."""
    print(f"\n=== Snapshot {snapshot_id}: {table_id} ===")
    table = catalog.load_table(table_id)
    arrow = table.scan(snapshot_id=snapshot_id, limit=limit).to_arrow()
    result = con.execute("SELECT * FROM arrow").fetchdf()
    print(result.to_string())
    return result


def show_snapshots(catalog, table_id: str):
    """List all snapshots for a table (for time travel exploration)."""
    table = catalog.load_table(table_id)
    print(f"\n=== Snapshots for {table_id} ===")
    for snap in table.history():
        print(f"  Snapshot {snap.snapshot_id} | {snap.timestamp_ms} ms | parent={snap.parent_snapshot_id}")


def run_sql(catalog, con, table_id: str, sql: str):
    """Run an arbitrary SQL query against the table."""
    print(f"\n=== Custom SQL ===")
    table = catalog.load_table(table_id)
    arrow = table.scan().to_arrow()
    result = con.execute(f"SELECT * FROM arrow").fetchdf()
    # Register as a view, then run the user SQL
    con.register("tbl", arrow)
    result = con.execute(sql.replace(table_id.split(".")[-1], "tbl")).fetchdf()
    print(result.to_string())
    return result


def main():
    parser = argparse.ArgumentParser(description="Query Iceberg tables with DuckDB")
    parser.add_argument("--table", default="lakehouse_db.benchmark_events", help="Table identifier")
    parser.add_argument("--branch", default=None, help="Nessie branch to read from (default: main)")
    parser.add_argument("--snapshot", type=int, default=None, help="Snapshot ID for time travel")
    parser.add_argument("--snapshots", action="store_true", help="List all snapshots")
    parser.add_argument("--limit", type=int, default=20, help="Row limit for queries")
    parser.add_argument("--sql", default=None, help="Custom SQL query (use table name in FROM)")
    args = parser.parse_args()

    with open(CONFIG_PATH) as f:
        cfg = yaml.safe_load(f)

    catalog = load_catalog(cfg, ref=args.branch)
    con = setup_duckdb_s3(cfg)

    if args.snapshots:
        show_snapshots(catalog, args.table)
    elif args.snapshot:
        query_snapshot(catalog, con, args.table, args.snapshot, limit=args.limit)
    elif args.sql:
        run_sql(catalog, con, args.table, args.sql)
    else:
        query_latest(catalog, con, args.table, limit=args.limit)


if __name__ == "__main__":
    main()
