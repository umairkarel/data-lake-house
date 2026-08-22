import os
from pyflink.table import EnvironmentSettings, TableEnvironment


def main():
    print("Initializing PyFlink Table Environment...")
    env_settings = EnvironmentSettings.in_batch_mode()
    t_env = TableEnvironment.create(env_settings)

    catalog_name  = "nessie_catalog"
    nessie_uri    = "http://lakehouse-nessie:19120/api/v1"
    warehouse     = "s3://warehouse/"
    ref           = "main"
    s3_endpoint   = os.environ.get("S3_ENDPOINT",   "http://minio:9000")
    s3_access_key = os.environ.get("S3_ACCESS_KEY", "minioadmin")
    s3_secret_key = os.environ.get("S3_SECRET_KEY", "minioadmin")

    print(f"Creating Iceberg Catalog '{catalog_name}' using Nessie...")

    t_env.execute_sql(f"""
        CREATE CATALOG {catalog_name} WITH (
            'type'                 = 'iceberg',
            'catalog-impl'         = 'org.apache.iceberg.nessie.NessieCatalog',
            'uri'                  = '{nessie_uri}',
            'ref'                  = '{ref}',
            'warehouse'            = '{warehouse}',
            'io-impl'              = 'org.apache.iceberg.aws.s3.S3FileIO',
            's3.endpoint'          = '{s3_endpoint}',
            's3.access-key-id'     = '{s3_access_key}',
            's3.secret-access-key' = '{s3_secret_key}',
            's3.path-style-access' = 'true',
            'client.region'        = 'us-east-1'
        )
    """)
    t_env.use_catalog(catalog_name)

    print("Creating namespace 'lakehouse'...")
    t_env.execute_sql("CREATE DATABASE IF NOT EXISTS lakehouse")
    t_env.use_database("lakehouse")

    # Drop old table if it exists from previous schema iteration
    # print("Dropping old 'benchmark_events' table if present...")
    # t_env.execute_sql("DROP TABLE IF EXISTS benchmark_events")

    print("Creating table 'order_events'...")
    # t_env.execute_sql("DROP TABLE IF EXISTS order_events")
    t_env.execute_sql("""
        CREATE TABLE IF NOT EXISTS order_events (
            event_id        STRING,
            order_id        STRING,
            user_id         STRING,
            product_id      STRING,
            category        STRING,
            status          STRING,
            quantity        INT,
            unit_price      DOUBLE,
            total_amount    DOUBLE,
            discount_pct    DOUBLE,
            region          STRING,
            platform        STRING,
            event_time      TIMESTAMP(6),
            ingestion_time  TIMESTAMP(3),
            PRIMARY KEY (event_id) NOT ENFORCED
        ) WITH (
            'format-version'        = '2',
            'write.upsert.enabled'  = 'true'
        )
    """)

    print("✅ Catalog and table 'order_events' successfully initialized!")


if __name__ == "__main__":
    main()
