import os
import json
import psycopg2
import boto3
import pyarrow.parquet as pq

from config import DB_CONFIG, MINIO_CONFIG

"""
Ingest parquet data into the DuckLake lakehouse
- Uploads parquet file to MinIO
- Registers metadata in PostgreSQL catalog
"""

# Configuration
BUCKET_NAME = 'lakehouse'
NAMESPACE = 'default'
LOCAL_PARQUET_FILE = "data/popolazione_residente.parquet"
TABLE_NAME = "popolazione_residente"

def get_parquet_schema(file_path: str):
    """Extract schema from parquet file"""
    parquet_file = pq.read_table(file_path)
    schema = parquet_file.schema
    
    # Convert to a JSON-serializable format
    schema_dict = {
        'fields': [
            {
                'name': field.name,
                'type': str(field.type),
                'nullable': field.nullable
            }
            for field in schema
        ]
    }
    return schema_dict, parquet_file.num_rows

def upload_to_minio(local_path, s3_path):
    """Upload file to MinIO"""
    s3_client = boto3.client('s3', **MINIO_CONFIG)
    
    # Ensure bucket exists
    try:
        s3_client.head_bucket(Bucket=BUCKET_NAME)
    except:
        s3_client.create_bucket(Bucket=BUCKET_NAME)
        print(f"✓ Created bucket: {BUCKET_NAME}")
    
    # Upload file
    s3_client.upload_file(local_path, BUCKET_NAME, s3_path)
    print(f"✓ Uploaded to MinIO: s3://{BUCKET_NAME}/{s3_path}")
    
    return f"s3://{BUCKET_NAME}/{s3_path}"

def register_table_in_catalog(table_name, location, schema_dict, record_count):
    """Register table metadata in PostgreSQL catalog"""
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()
    
    try:
        # Get namespace_id
        cur.execute(
            "SELECT namespace_id FROM namespaces WHERE namespace_name = %s",
            (NAMESPACE,)
        )
        namespace_id = cur.fetchone()[0]
        
        # Insert or update table metadata
        cur.execute("""
            INSERT INTO tables (
                namespace_id, table_name, location, schema_json, properties
            ) VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (namespace_id, table_name) 
            DO UPDATE SET 
                location = EXCLUDED.location,
                schema_json = EXCLUDED.schema_json,
                updated_at = CURRENT_TIMESTAMP
            RETURNING table_id
        """, (
            namespace_id,
            table_name,
            location,
            json.dumps(schema_dict),
            json.dumps({'record_count': record_count})
        ))
        
        table_id = cur.fetchone()[0]
        
        # Register partition/file
        cur.execute("""
            INSERT INTO partitions (
                table_id, partition_spec, file_path, file_format, record_count
            ) VALUES (%s, %s, %s, %s, %s)
        """, (
            table_id,
            json.dumps({}),  # No partitioning for now
            location,
            'parquet',
            record_count
        ))
        
        conn.commit()
        print(f"✓ Registered table '{NAMESPACE}.{table_name}' in catalog")
        
    except Exception as e:
        conn.rollback()
        raise e
    finally:
        cur.close()
        conn.close()

def ingest_parquet(local_file_path: str, table_name: str) -> None:
    """Main ingestion function"""
    print(f"\n📥 Starting ingestion for table: {table_name}")
    print(f"📄 Source file: {local_file_path}")
    
    if not os.path.exists(local_file_path):
        raise FileNotFoundError(f"File not found: {local_file_path}")
    
    # Get schema and stats
    print("\n1️⃣  Reading parquet file...")
    schema_dict, record_count = get_parquet_schema(local_file_path)
    print(f"   Schema: {len(schema_dict['fields'])} columns")
    print(f"   Records: {record_count:,}")
    
    # Upload to MinIO
    print("\n2️⃣  Uploading to MinIO...")
    s3_path = f"tables/{NAMESPACE}/{table_name}/data.parquet"
    location = upload_to_minio(local_file_path, s3_path)
    
    # Register in catalog
    print("\n3️⃣  Registering in catalog...")
    register_table_in_catalog(table_name, location, schema_dict, record_count)
    
    print(f"\n✅ Ingestion complete!")
    print(f"📊 Table available as: {NAMESPACE}.{table_name}")
    print(f"🔗 Location: {location}")


if __name__ == "__main__":    
    try:
        ingest_parquet(LOCAL_PARQUET_FILE, TABLE_NAME)
    except FileNotFoundError:
        print(f"\n⚠️  Please update the LOCAL_PARQUET_FILE path in the script")
        print(f"   Current path: {LOCAL_PARQUET_FILE}")
        print(f"   Place your parquet file in the data/ directory and update the path")
    except Exception as e:
        print(f"\n❌ Error during ingestion: {e}")