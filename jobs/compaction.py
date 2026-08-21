"""
compaction.py
-------------
Runs Iceberg small-file compaction on the benchmark_events table using PyFlink + Py4J.

This replaces CompactionService.java (RewriteDataFilesAction) from the original project.
It uses Flink's Java `RewriteDataFilesAction` under the hood via the Py4J gateway,
which natively supports compacting V2 tables containing equality deletes!

Usage:
    python jobs/compaction.py
    python jobs/compaction.py --table lakehouse.benchmark_events
"""

import argparse
from pathlib import Path
import yaml
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.java_gateway import get_gateway

CONFIG_PATH = Path(__file__).parent.parent / "catalog" / "catalog_config.yaml"

def run_compaction(table_identifier: str):
    """
    Compact small Parquet files in an Iceberg table via Flink's RewriteDataFilesAction.
    """
    print(f"[INFO] Initializing compaction for table: {table_identifier} using PyFlink's Java Gateway...")
    
    with open(CONFIG_PATH) as f:
        cfg = yaml.safe_load(f)
        
    cat_cfg = cfg["catalog"]
    s3_cfg = cfg["storage"]["s3"]

    env = StreamExecutionEnvironment.get_execution_environment()
    gateway = get_gateway()
    
    try:
        # Load Java classes via Py4J Gateway
        Actions = gateway.jvm.org.apache.iceberg.flink.actions.Actions
        CatalogLoader = gateway.jvm.org.apache.iceberg.flink.CatalogLoader
        
        # Build Catalog Properties Map
        HashMap = gateway.jvm.java.util.HashMap
        props = HashMap()
        props.put("type", "rest")
        props.put("uri", cat_cfg["uri"])
        props.put("ref", cat_cfg.get("ref", "main"))
        props.put("warehouse", cat_cfg["warehouse"])
        props.put("s3.endpoint", s3_cfg["endpoint"])
        props.put("s3.access-key-id", s3_cfg["access_key_id"])
        props.put("s3.secret-access-key", s3_cfg["secret_access_key"])
        props.put("s3.region", s3_cfg["region"])
        props.put("s3.path-style-access", str(s3_cfg["path_style_access"]).lower())
        props.put("s3.remote-signing-enabled", "false")
        
        # Instantiate Java Catalog
        catalog_loader = CatalogLoader.custom(
            cat_cfg["name"], 
            props, 
            gateway.jvm.org.apache.hadoop.conf.Configuration(), 
            "org.apache.iceberg.rest.RESTCatalog"
        )
        catalog = catalog_loader.loadCatalog()
        
        # Parse table identifier (namespace.table)
        parts = table_identifier.split(".")
        namespace_name = parts[0]
        table_name = parts[1]
        
        # Create TableIdentifier Java Object
        Namespace = gateway.jvm.org.apache.iceberg.catalog.Namespace
        namespace_array = gateway.new_array(gateway.jvm.java.lang.String, 1)
        namespace_array[0] = namespace_name
        namespace = Namespace.of(namespace_array)
        
        TableIdentifier = gateway.jvm.org.apache.iceberg.catalog.TableIdentifier
        table_id = TableIdentifier.of(namespace, table_name)
        
        # Load Table in Java
        table = catalog.loadTable(table_id)
        
        print(f"[INFO] Loaded Java Table: {table_identifier}")
        print("[INFO] Executing RewriteDataFilesAction (Compaction)...")
        
        # Execute Action
        result = Actions.forTable(env._j_stream_execution_environment, table).rewriteDataFiles().execute()
        
        print("\n[DONE] Compaction successfully executed using Flink JVM!")
        
    except Exception as e:
        print(f"\n[ERROR] Compaction failed:")
        import traceback
        traceback.print_exc()


def main():
    parser = argparse.ArgumentParser(description="Iceberg table compaction via PyFlink Java Gateway")
    parser.add_argument(
        "--table",
        default="lakehouse.benchmark_events",
        help="Table identifier in format namespace.table_name",
    )
    args = parser.parse_args()

    run_compaction(table_identifier=args.table)


if __name__ == "__main__":
    main()
