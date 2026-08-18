"""
setup_catalog.py
----------------
Bootstrap script — run once to:
  1. Connect to the Nessie catalog via PyIceberg
  2. Create the default namespace (database)
  3. Create all Iceberg tables

Usage (from repo root, after `docker compose up`):
    python catalog/setup_catalog.py

Or inside a running container:
    docker exec lakehouse-jobmanager python /opt/flink/catalog/setup_catalog.py
"""

import yaml
import os
import sys
from pathlib import Path

from pyiceberg.catalog import load_catalog
from pyiceberg.exceptions import NamespaceAlreadyExistsError, TableAlreadyExistsError

# Resolve config path relative to this file
CONFIG_PATH = Path(__file__).parent / "catalog_config.yaml"


def load_config():
    with open(CONFIG_PATH) as f:
        return yaml.safe_load(f)


def build_catalog(cfg):
    """
    Build a PyIceberg catalog instance pointing to Nessie + MinIO.
    """
    cat_cfg = cfg["catalog"]
    s3_cfg = cfg["storage"]["s3"]

    catalog = load_catalog(
        name=cat_cfg["name"],
        **{
            "type": "nessie",
            "uri": cat_cfg["uri"],
            "ref": cat_cfg["ref"],
            "warehouse": cat_cfg["warehouse"],
            # S3 / MinIO credentials
            "s3.endpoint": s3_cfg["endpoint"],
            "s3.access-key-id": s3_cfg["access_key_id"],
            "s3.secret-access-key": s3_cfg["secret_access_key"],
            "s3.region": s3_cfg["region"],
            "s3.path-style-access": str(s3_cfg["path_style_access"]).lower(),
        },
    )
    return catalog


def setup(catalog, namespace: str):
    # -------------------------------------------------------------------------
    # 1. Create namespace
    # -------------------------------------------------------------------------
    try:
        catalog.create_namespace(namespace)
        print(f"[OK] Namespace '{namespace}' created.")
    except NamespaceAlreadyExistsError:
        print(f"[SKIP] Namespace '{namespace}' already exists.")

    # -------------------------------------------------------------------------
    # 2. Import schemas from schema.py
    # -------------------------------------------------------------------------
    sys.path.insert(0, str(Path(__file__).parent))
    from schema import BENCHMARK_EVENTS_SCHEMA, BENCHMARK_EVENTS_PARTITION_SPEC

    # -------------------------------------------------------------------------
    # 3. Create benchmark_events table
    # -------------------------------------------------------------------------
    table_id = f"{namespace}.benchmark_events"
    try:
        table = catalog.create_table(
            identifier=table_id,
            schema=BENCHMARK_EVENTS_SCHEMA,
            partition_spec=BENCHMARK_EVENTS_PARTITION_SPEC,
            location=f"s3://warehouse/{namespace}/benchmark_events",
            properties={
                "write.format.default": "parquet",
                "write.parquet.compression-codec": "snappy",
                # Enable small-file compaction (Iceberg v2)
                "write.target-file-size-bytes": str(128 * 1024 * 1024),  # 128 MB
            },
        )
        print(f"[OK] Table '{table_id}' created: {table.location()}")
    except TableAlreadyExistsError:
        print(f"[SKIP] Table '{table_id}' already exists.")


def main():
    cfg = load_config()
    namespace = cfg["defaults"]["namespace"]

    print(f"Connecting to Nessie at: {cfg['catalog']['uri']}")
    catalog = build_catalog(cfg)
    print("[OK] Catalog connected.")

    setup(catalog, namespace)
    print("\nSetup complete. Tables:")
    for tbl in catalog.list_tables(namespace):
        print(f"  - {tbl}")


if __name__ == "__main__":
    main()
