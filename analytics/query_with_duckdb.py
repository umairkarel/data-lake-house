import boto3
import duckdb

def get_latest_metadata_path(bucket, prefix):
    # Connect to MinIO
    s3 = boto3.client('s3',
                      endpoint_url='http://localhost:9000',
                      aws_access_key_id='minioadmin',
                      aws_secret_access_key='minioadmin')
    
    # List all objects under the warehouse/lakehouse prefix
    response = s3.list_objects_v2(Bucket=bucket, Prefix=prefix)
    
    # Find all metadata.json files and group them by table folder UUID
    metadata_files = {}
    for obj in response.get('Contents', []):
        key = obj['Key']
        if key.endswith('.metadata.json'):
            # Path looks like: lakehouse/benchmark_events_UUID/metadata/0000x-uuid.metadata.json
            parts = key.split('/')
            folder_name = parts[1] # benchmark_events_UUID
            metadata_files.setdefault(folder_name, []).append(obj)
            
    # Find the active folder (the one that actually has data in it)
    active_folder = None
    latest_metadata_key = None
    
    for folder, files in metadata_files.items():
        # Check if this folder has a data/ directory
        data_prefix = f"lakehouse/{folder}/data/"
        data_response = s3.list_objects_v2(Bucket=bucket, Prefix=data_prefix, MaxKeys=1)
        if 'Contents' in data_response:
            active_folder = folder
            # Sort metadata files by LastModified to get the latest snapshot
            latest_file = sorted(files, key=lambda x: x['LastModified'])[-1]
            latest_metadata_key = latest_file['Key']
            break
            
    if not latest_metadata_key:
        raise Exception("Could not find an active Iceberg table with data!")
        
    return f"s3://{bucket}/{latest_metadata_key}"


print("Searching MinIO for the latest Iceberg metadata...")
metadata_path = get_latest_metadata_path('warehouse', 'lakehouse/benchmark_events_')
print(f"Found active Iceberg metadata: {metadata_path}")

print("Starting DuckDB and installing Iceberg extensions...")
# Initialize DuckDB connection
con = duckdb.connect()

# Install and load required extensions for S3 and Iceberg
con.execute("INSTALL httpfs;")
con.execute("LOAD httpfs;")
con.execute("INSTALL iceberg;")
con.execute("LOAD iceberg;")

# Configure S3 credentials for MinIO
con.execute("""
    CREATE SECRET minio_secret (
        TYPE S3,
        KEY_ID 'minioadmin',
        SECRET 'minioadmin',
        ENDPOINT 'localhost:9000',
        URL_STYLE 'path',
        USE_SSL false
    );
""")

print("Querying Iceberg Table directly from MinIO...\n")

# Run a query using the iceberg_scan function
query = f"""
    SELECT * 
    FROM iceberg_scan('{metadata_path}')
    LIMIT 10;
"""

result = con.execute(query).df()
print(result)

# Let's also get a total count
count_result = con.execute(f"SELECT COUNT(*) as total_events FROM iceberg_scan('{metadata_path}')").df()
print(f"\nTotal Events in Iceberg Table: {count_result['total_events'][0]}")
