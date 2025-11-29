import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT

from config import DB_CONFIG

"""
Initialize the catalog database with necessary tables for DuckLake.
This creates a simple catalog structure inspired by Iceberg patterns.
"""

def create_catalog_schema() -> None:
    """Create the catalog schema and tables"""
    
    conn = psycopg2.connect(**DB_CONFIG)
    conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
    cur = conn.cursor()
    
    print("🗄️  Initializing DuckLake Catalog...")
    
    # Create namespaces table (like databases/schemas)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS namespaces (
            namespace_id SERIAL PRIMARY KEY,
            namespace_name VARCHAR(255) UNIQUE NOT NULL,
            properties JSONB,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    print("✓ Created namespaces table")
    
    # Create tables metadata table
    cur.execute("""
        CREATE TABLE IF NOT EXISTS tables (
            table_id SERIAL PRIMARY KEY,
            namespace_id INTEGER REFERENCES namespaces(namespace_id),
            table_name VARCHAR(255) NOT NULL,
            location VARCHAR(1000) NOT NULL,
            schema_json JSONB NOT NULL,
            properties JSONB,
            current_snapshot_id BIGINT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(namespace_id, table_name)
        )
    """)
    print("✓ Created tables metadata table")
    
    # Create snapshots table (for versioning)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS snapshots (
            snapshot_id BIGSERIAL PRIMARY KEY,
            table_id INTEGER REFERENCES tables(table_id),
            parent_snapshot_id BIGINT,
            manifest_list VARCHAR(1000) NOT NULL,
            summary JSONB,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    print("✓ Created snapshots table")
    
    # Create partitions table
    cur.execute("""
        CREATE TABLE IF NOT EXISTS partitions (
            partition_id SERIAL PRIMARY KEY,
            table_id INTEGER REFERENCES tables(table_id),
            partition_spec JSONB NOT NULL,
            file_path VARCHAR(1000) NOT NULL,
            file_format VARCHAR(50) DEFAULT 'parquet',
            record_count BIGINT,
            file_size_bytes BIGINT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    print("✓ Created partitions table")
    
    # Insert default namespace
    cur.execute("""
        INSERT INTO namespaces (namespace_name, properties)
        VALUES ('default', '{"description": "Default namespace"}')
        ON CONFLICT (namespace_name) DO NOTHING
    """)
    print("✓ Created default namespace")
    
    cur.close()
    conn.close()
    
    print("\n✅ Catalog initialization complete!")
    print("📊 Catalog database ready at: postgresql://catalog_user@localhost:5432/catalog_db")

if __name__ == "__main__":
    try:
        create_catalog_schema()
    except psycopg2.Error as e:
        print(f"❌ Database error: {e}")
    except Exception as e:
        print(f"❌ Error: {e}")