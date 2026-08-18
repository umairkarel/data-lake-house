"""
compaction.py
-------------
Runs Iceberg small-file compaction on the benchmark_events table using PyIceberg.

This replaces CompactionService.java (RewriteDataFilesAction) from the original project.
No Flink JVM needed — PyIceberg talks directly to Nessie + MinIO over HTTP/S3.

Usage:
    python jobs/compaction.py
    python jobs/compaction.py --table lakehouse_db.benchmark_events --strategy binpack
"""

import argparse
import sys
from pathlib import Path
import yaml

# Resolve catalog config
CONFIG_PATH = Path(__file__).parent.parent / "catalog" / "catalog_config.yaml"


def load_catalog():
    with open(CONFIG_PATH) as f:
        cfg = yaml.safe_load(f)

    from pyiceberg.catalog import load_catalog

    cat_cfg = cfg["catalog"]
    s3_cfg = cfg["storage"]["s3"]

    return load_catalog(
        name=cat_cfg["name"],
        **{
            "type": "nessie",
            "uri": cat_cfg["uri"],
            "ref": cat_cfg["ref"],
            "warehouse": cat_cfg["warehouse"],
            "s3.endpoint": s3_cfg["endpoint"],
            "s3.access-key-id": s3_cfg["access_key_id"],
            "s3.secret-access-key": s3_cfg["secret_access_key"],
            "s3.region": s3_cfg["region"],
            "s3.path-style-access": str(s3_cfg["path_style_access"]).lower(),
        },
    )


def run_compaction(table_identifier: str, strategy: str = "binpack", target_mb: int = 128):
    """
    Compact small Parquet files in an Iceberg table.

    Args:
        table_identifier: e.g. 'lakehouse_db.benchmark_events'
        strategy: 'binpack' (default) or 'sort'
        target_mb: target file size in MB after compaction
    """
    catalog = load_catalog()
    table = catalog.load_table(table_identifier)

    print(f"[INFO] Loaded table: {table_identifier}")
    print(f"[INFO] Strategy: {strategy}, Target file size: {target_mb} MB")

    # List current snapshot info before compaction
    current_snapshot = table.current_snapshot()
    if current_snapshot:
        print(f"[INFO] Current snapshot ID: {current_snapshot.snapshot_id}")
        summary = current_snapshot.summary
        print(f"[INFO] Total data files: {summary.get('total-data-files', 'unknown')}")

    # Run compaction
    from pyiceberg.table.rewrite import RewriteDataFiles

    rewrite = RewriteDataFiles(table=table)
    result = rewrite.execute(
        options={
            "rewrite-job-order": strategy,
            "target-file-size-bytes": str(target_mb * 1024 * 1024),
            "min-file-size-bytes": str(int(target_mb * 1024 * 1024 * 0.25)),  # compact if < 25% of target
            "max-concurrent-file-group-rewrites": "5",
        }
    )

    print(f"\n[DONE] Compaction complete.")
    print(f"  Rewritten data files: {result.rewritten_data_files_count}")
    print(f"  Added data files:     {result.added_data_files_count}")
    print(f"  Rewritten bytes:      {result.rewritten_bytes_count:,}")


def main():
    parser = argparse.ArgumentParser(description="Iceberg table compaction via PyIceberg")
    parser.add_argument(
        "--table",
        default="lakehouse_db.benchmark_events",
        help="Table identifier in format namespace.table_name",
    )
    parser.add_argument(
        "--strategy",
        default="binpack",
        choices=["binpack", "sort"],
        help="Rewrite strategy: binpack (default) or sort",
    )
    parser.add_argument(
        "--target-mb",
        type=int,
        default=128,
        help="Target output file size in MB (default: 128)",
    )
    args = parser.parse_args()

    run_compaction(
        table_identifier=args.table,
        strategy=args.strategy,
        target_mb=args.target_mb,
    )


if __name__ == "__main__":
    main()
