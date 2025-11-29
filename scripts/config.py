from botocore.client import Config

# Database configuration
DB_CONFIG = {
    'host': 'localhost',
    'port': 5432,
    'database': 'catalog_db',
    'user': 'catalog_user',
    'password': 'catalog_psw'
}

# Object storage configuration
MINIO_CONFIG = {
    'endpoint_url': 'http://localhost:9000',
    'aws_access_key_id': 'minioadmin',
    'aws_secret_access_key': 'minioadmin',
    'config': Config(signature_version='s3v4'),
    'region_name': 'us-east-1'
}
