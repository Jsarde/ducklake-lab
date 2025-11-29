#!/usr/bin/env python3

import json
import psycopg2
import duckdb

from config import DB_CONFIG

"""
Interactive CLI for querying DuckLake using DuckDB
Provides a SQL shell to query tables registered in the catalog
"""

class DuckLakeCLI:
    def __init__(self):
        self.pg_conn = None
        self.duck_conn = None
        self.current_namespace = 'default'
        self.table_registry = {}
        
    def connect(self):
        """Initialize connections"""
        try:
            self.pg_conn = psycopg2.connect(**DB_CONFIG)
            self.duck_conn = duckdb.connect()
            
            # Setup DuckDB for S3/MinIO
            self.duck_conn.execute("INSTALL httpfs")
            self.duck_conn.execute("LOAD httpfs")
            self.duck_conn.execute("""
                SET s3_endpoint='localhost:9000';
                SET s3_access_key_id='minioadmin';
                SET s3_secret_access_key='minioadmin';
                SET s3_use_ssl=false;
                SET s3_url_style='path';
            """)
            
            self._load_tables()
            return True
        except Exception as e:
            print(f"❌ Connection error: {e}")
            return False
    
    def _load_tables(self):
        """Load table metadata from catalog"""
        cur = self.pg_conn.cursor()
        cur.execute("""
            SELECT n.namespace_name, t.table_name, t.location, t.schema_json
            FROM tables t
            JOIN namespaces n ON t.namespace_id = n.namespace_id
        """)
        
        for namespace, table_name, location, schema_json in cur.fetchall():
            full_name = f"{namespace}.{table_name}"
            # Handle both dict and string JSON
            if isinstance(schema_json, dict):
                schema = schema_json
            else:
                schema = json.loads(schema_json)
            
            self.table_registry[full_name] = {
                'location': location,
                'schema': schema
            }
        
        cur.close()
    
    def list_tables(self):
        """Display available tables"""
        if not self.table_registry:
            print("No tables found in catalog")
            return
        
        print("\n📚 Available Tables:")
        print("-" * 80)
        for table_name, metadata in self.table_registry.items():
            schema = metadata['schema']
            columns = [f['name'] for f in schema['fields']]
            print(f"  • {table_name}")
            print(f"    Columns: {', '.join(columns[:5])}{', ...' if len(columns) > 5 else ''}")
        print("-" * 80)
    
    def show_schema(self, table_name):
        """Show table schema"""
        if table_name not in self.table_registry:
            print(f"❌ Table '{table_name}' not found")
            return
        
        schema = self.table_registry[table_name]['schema']
        print(f"\n📋 Schema for {table_name}:")
        print("-" * 80)
        for field in schema['fields']:
            nullable = "NULL" if field['nullable'] else "NOT NULL"
            print(f"  {field['name']:<30} {field['type']:<20} {nullable}")
        print("-" * 80)
    
    def execute_query(self, sql):
        """Execute SQL query"""
        try:
            # Replace table references with their S3 locations
            modified_sql = sql
            for table_name, metadata in self.table_registry.items():
                if table_name in sql:
                    modified_sql = modified_sql.replace(
                        table_name,
                        f"'{metadata['location']}'"
                    )
            
            result = self.duck_conn.execute(modified_sql).fetchdf()
            
            if len(result) == 0:
                print("Query returned no results")
                return
            
            print("\n" + "=" * 80)
            print(result.to_string())
            print("=" * 80)
            print(f"\n{len(result)} row(s) returned")
            
        except Exception as e:
            print(f"❌ Query error: {e}")
    
    def show_help(self):
        """Display help message"""
        help_text = """
╔════════════════════════════════════════════════════════════════════════════╗
║                         DuckLake CLI - Help                                ║
╚════════════════════════════════════════════════════════════════════════════╝

Commands:
  \\l, \\list              List all available tables
  \\d <table>             Describe table schema
  \\h, \\help              Show this help message
  \\q, quit, exit         Exit the CLI

SQL Queries:
  Write any SQL query and press Enter. Table names will be automatically
  resolved to their S3 locations in MinIO.

Examples:
  SELECT * FROM default.popolazione_residente LIMIT 10;
  
  SELECT nome_comune, residenti 
  FROM default.popolazione_residente 
  ORDER BY residenti DESC 
  LIMIT 5;

Tips:
  - Use arrow keys to navigate command history
  - Queries are case-insensitive
  - Always reference tables as: namespace.table_name
        """
        print(help_text)
    
    def run(self):
        """Main CLI loop"""
        if not self.connect():
            return
        
        print("╔════════════════════════════════════════════════════════════════════════════╗")
        print("║                         DuckLake SQL CLI                                   ║")
        print("║                    Powered by DuckDB + MinIO                               ║")
        print("╚════════════════════════════════════════════════════════════════════════════╝")
        print("\nType \\h for help, \\q to quit\n")
        
        self.list_tables()
        
        while True:
            try:
                query = input(f"\nducklake> ").strip()
                
                if not query:
                    continue
                
                # Handle meta-commands
                if query in ['\\q', 'quit', 'exit']:
                    print("Goodbye! 👋")
                    break
                elif query in ['\\h', '\\help']:
                    self.show_help()
                elif query in ['\\l', '\\list']:
                    self.list_tables()
                elif query.startswith('\\d '):
                    table_name = query[3:].strip()
                    self.show_schema(table_name)
                else:
                    # Execute SQL query
                    self.execute_query(query)
                    
            except KeyboardInterrupt:
                print("\n\nUse \\q to quit")
            except EOFError:
                print("\nGoodbye! 👋")
                break
        
        # Cleanup
        if self.pg_conn:
            self.pg_conn.close()
        if self.duck_conn:
            self.duck_conn.close()

if __name__ == "__main__":
    cli = DuckLakeCLI()
    cli.run()
