"""
BQ2PG API Server - Flask backend for web-based migration tool
"""

from flask import Flask, request, jsonify, send_from_directory
from flask_cors import CORS
from flask_limiter import Limiter
from flask_limiter.util import get_remote_address
import json
import uuid
import threading
import time
import sys
import re
from datetime import datetime
from functools import wraps
from pathlib import Path

# Add project root to path so we can import from src/
project_root = Path(__file__).parent.parent
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

from src.app_config import config

try:
    from src.extract import BigQueryExtractor
    from src.load import PostgresLoader
    from src.transform import DataTransformer
except ImportError:
    # If imports fail, we'll handle it gracefully
    BigQueryExtractor = None
    PostgresLoader = None
    DataTransformer = None

from google.cloud import bigquery
import psycopg2

app = Flask(__name__, static_folder='../frontend', static_url_path='')
CORS(app)

# Security: Limit request body size to 5MB (protects against DoS)
app.config['MAX_CONTENT_LENGTH'] = 5 * 1024 * 1024

# Security: Rate Limiting
limiter = Limiter(
    app=app,
    key_func=get_remote_address,
    default_limits=["200 per day", "50 per hour"],
    storage_uri="memory://",
)

# In-memory storage for migration jobs
migration_jobs = {}
migration_logs = {}

@app.after_request
def add_security_headers(response):
    """Inject security headers into every response."""
    response.headers['X-Frame-Options'] = 'DENY'
    response.headers['X-Content-Type-Options'] = 'nosniff'
    response.headers['X-XSS-Protection'] = '1; mode=block'
    response.headers['Content-Security-Policy'] = "default-src 'self'; script-src 'self' 'unsafe-inline'; style-src 'self' 'unsafe-inline' https://fonts.googleapis.com; font-src 'self' https://fonts.gstatic.com; img-src 'self' data:;"
    response.headers['Strict-Transport-Security'] = 'max-age=31536000; includeSubDomains'
    response.headers['Referrer-Policy'] = 'strict-origin-when-cross-origin'
    return response

def require_auth(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        token = config.API_SECURITY_TOKEN
        if not token:
            return f(*args, **kwargs)
        
        user_token = request.headers.get('X-API-Token')
        if user_token != token:
            return jsonify({'error': 'Unauthorized: Invalid or missing security token'}), 401
        return f(*args, **kwargs)
    return decorated

def validate_db_identifier(name):
    """Ensure identifier consists only of alphanumeric characters and underscores."""
    if not name:
        return False
    # Only allow letters, numbers, underscores, dashes, and single dots
    return bool(re.match(r'^[a-zA-Z0-9_\-\.]+$', name))

def scrub_sensitive_data(data):
    """Recursively remove sensitive keys from dictionaries for safe logging/returning."""
    if not isinstance(data, dict):
        return data
    
    sensitive_keys = {'credentials', 'password', 'private_key', 'client_email', 'token_uri', 'private_key_id'}
    scrubbed = {}
    
    for k, v in data.items():
        if k.lower() in sensitive_keys:
            scrubbed[k] = "********"
        elif isinstance(v, dict):
            scrubbed[k] = scrub_sensitive_data(v)
        elif isinstance(v, list):
            scrubbed[k] = [scrub_sensitive_data(i) if isinstance(i, dict) else i for i in v]
        else:
            scrubbed[k] = v
    return scrubbed


@app.route('/')
def index():
    """Serve the main web UI"""
    return send_from_directory(app.static_folder, 'index.html')


@app.route('/api/validate-credentials', methods=['POST'])
@require_auth
@limiter.limit("10 per minute")
def validate_credentials():
    """Validate Google Cloud credentials"""
    try:
        data = request.json
        credentials = data.get('credentials')
        
        if not credentials:
            return jsonify({'valid': False, 'error': 'No credentials provided'}), 400
        
        # Check required fields
        required_fields = ['type', 'project_id', 'private_key', 'client_email']
        missing = [f for f in required_fields if f not in credentials]
        
        if missing:
            return jsonify({
                'valid': False,
                'error': f'Missing required fields: {", ".join(missing)}'
            }), 400
        
        # Try to create a BigQuery client
        from google.oauth2 import service_account
        creds = service_account.Credentials.from_service_account_info(credentials)
        client = bigquery.Client(credentials=creds, project=credentials['project_id'])
        
        # Test with a simple query
        list(client.list_datasets(max_results=1))
        
        return jsonify({'valid': True, 'project_id': credentials['project_id']})
        
    except Exception as e:
        return jsonify({'valid': False, 'error': str(e)}), 400


@app.route('/api/test-bigquery', methods=['POST'])
@require_auth
@limiter.limit("20 per minute")
def test_bigquery():
    """Test BigQuery connection and get row count"""
    try:
        data = request.json
        dataset = data.get('dataset')
        table = data.get('table')
        project_id = data.get('project_id')
        credentials = data.get('credentials')

        # 1. Validate table and dataset names first
        if not validate_db_identifier(dataset) or not validate_db_identifier(table):
            return jsonify({'success': False, 'error': 'Invalid dataset or table name format'}), 400

        # 2. Extract and check credentials
        from google.oauth2 import service_account
        creds = service_account.Credentials.from_service_account_info(credentials)
        client = bigquery.Client(credentials=creds, project=project_id)
        
        # Handle cross-project dataset references
        # If dataset contains a dot, it's already a full reference (project.dataset)
        if '.' in dataset:
            table_ref = f"{dataset}.{table}"
        else:
            table_ref = f"{project_id}.{dataset}.{table}"
        
        query = f"SELECT COUNT(*) as count FROM `{table_ref}`"
        
        result = client.query(query).result()
        row_count = list(result)[0]['count']
        
        return jsonify({
            'success': True,
            'row_count': row_count,
            'table': table_ref
        })
        
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 400


@app.route('/api/test-postgres', methods=['POST'])
@require_auth
@limiter.limit("20 per minute")
def test_postgres():
    """Test PostgreSQL connection"""
    try:
        data = request.json
        
        # Try to connect
        conn = psycopg2.connect(
            host=data.get('host'),
            port=data.get('port'),
            database=data.get('database'),
            user=data.get('user'),
            password=data.get('password')
        )
        
        # Test query
        cursor = conn.cursor()
        cursor.execute('SELECT version();')
        version = cursor.fetchone()[0]
        
        cursor.close()
        conn.close()
        
        return jsonify({
            'success': True,
            'version': version
        })
        
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 400


@app.route('/api/start-migration', methods=['POST'])
@require_auth
@limiter.limit("5 per minute")
def start_migration():
    """Start a migration job"""
    try:
        data = request.json
        job_id = str(uuid.uuid4())
        
        # Initialize job status
        migration_jobs[job_id] = {
            'status': 'running',
            'progress': 0,
            'rows_extracted': 0,
            'rows_loaded': 0,
            'rows_failed': 0,
            'message': 'Starting migration...',
            'started_at': datetime.now().isoformat()
        }
        
        migration_logs[job_id] = []
        
        # Start migration in background thread
        thread = threading.Thread(
            target=run_migration,
            args=(job_id, data)
        )
        thread.daemon = True
        thread.start()
        
        return jsonify({'job_id': job_id})
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/api/migration-status/<job_id>', methods=['GET'])
def migration_status(job_id):
    """Get migration job status"""
    if job_id not in migration_jobs:
        return jsonify({'error': 'Job not found'}), 404
    
    return jsonify(scrub_sensitive_data(migration_jobs[job_id]))


@app.route('/api/migration-logs/<job_id>', methods=['GET'])
def get_migration_logs(job_id):
    """Get migration job logs"""
    if job_id not in migration_logs:
        return jsonify({'error': 'Job not found'}), 404
    
    logs = '\n'.join(migration_logs[job_id])
    return logs, 200, {'Content-Type': 'text/plain'}


def run_migration(job_id, config):
    """Run the actual migration (background task)"""
    try:
        log(job_id, f"Starting migration job {job_id}")
        update_status(job_id, message="Initializing components...")
        
        # Extract configuration
        credentials = config['credentials']
        bq_config = config['bigquery']
        pg_config = config['postgres']
        
        # Create BigQuery client
        from google.oauth2 import service_account
        creds = service_account.Credentials.from_service_account_info(credentials)
        
        log(job_id, "Creating BigQuery extractor...")
        extractor = BigQueryExtractor()
        extractor.connect(credentials_info=credentials, project_id=bq_config['project_id'])
        
        # Build query
        dataset = bq_config['dataset']
        table = bq_config['table']
        project_id = bq_config['project_id']
        
        if '.' in dataset:
            table_ref = f"{dataset}.{table}"
        else:
            table_ref = f"{project_id}.{dataset}.{table}"
            
        query = f"SELECT * FROM `{table_ref}`"
        
        if bq_config.get('where_clause'):
            query += f" WHERE {bq_config['where_clause']}"
        
        if bq_config.get('row_limit'):
            query += f" LIMIT {bq_config['row_limit']}"
        
        log(job_id, f"Query: {query}")
        
        # Get total row count for progress
        count_query = f"SELECT COUNT(*) as count FROM `{table_ref}`"
        if bq_config.get('where_clause'):
            count_query += f" WHERE {bq_config['where_clause']}"
        
        result = extractor.client.query(count_query).result()
        total_rows = list(result)[0]['count']
        
        if bq_config.get('row_limit'):
            total_rows = min(total_rows, int(bq_config['row_limit']))
        
        log(job_id, f"Total rows to migrate: {total_rows}")
        
        # Create PostgreSQL loader
        log(job_id, "Creating PostgreSQL loader...")
        pg_conn_str = f"postgresql://{pg_config['user']}:{pg_config['password']}@{pg_config['host']}:{pg_config['port']}/{pg_config['database']}"
        loader = PostgresLoader(connection_string=pg_conn_str)
        
        # Create table
        update_status(job_id, message="Creating target table...")
        
        target_table = pg_config.get('table', 'patents')
        if not validate_db_identifier(target_table):
            raise ValueError(f"Invalid characters in target table name: {target_table}")

        if pg_config.get('drop_table'):
            log(job_id, f"Dropping table {pg_config['table']} if exists...")
            loader.drop_table(pg_config['table'])
        
        loader.create_table(pg_config['table'])
        log(job_id, f"Table {pg_config['table']} created")
        
        # Extract and load
        update_status(job_id, message="Extracting data from BigQuery...")
        
        total_extracted = 0
        total_loaded = 0
        total_failed = 0
        
        for chunk_num, df_chunk in extractor.extract(query, chunk_size=10000):
            log(job_id, f"Processing chunk {chunk_num} ({len(df_chunk)} rows)")
            
            try:
                # Load to PostgreSQL
                success = loader.load_dataframe(
                    df_chunk,
                    pg_config['table'],
                    if_exists='append'
                )
                
                if success:
                    total_loaded += len(df_chunk)
                    log(job_id, f"Chunk {chunk_num} loaded successfully")
                else:
                    total_failed += len(df_chunk)
                    log(job_id, f"Chunk {chunk_num} failed to load")
                
            except Exception as e:
                total_failed += len(df_chunk)
                log(job_id, f"Error loading chunk {chunk_num}: {str(e)}")
            
            total_extracted += len(df_chunk)
            
            # Update progress
            progress = int((total_extracted / total_rows) * 100) if total_rows > 0 else 0
            update_status(
                job_id,
                progress=progress,
                rows_extracted=total_extracted,
                rows_loaded=total_loaded,
                rows_failed=total_failed,
                message=f"Processing... {total_loaded:,} rows loaded"
            )
        
        # Complete
        log(job_id, f"Migration complete! Loaded {total_loaded:,} rows")
        update_status(
            job_id,
            status='completed',
            progress=100,
            rows_extracted=total_extracted,
            rows_loaded=total_loaded,
            rows_failed=total_failed,
            message=f"Migration complete! {total_loaded:,} rows loaded"
        )
        
    except Exception as e:
        log(job_id, f"ERROR: {str(e)}")
        update_status(
            job_id,
            status='failed',
            message=f"Migration failed: {str(e)}",
            error=str(e)
        )


def update_status(job_id, **kwargs):
    """Update job status"""
    if job_id in migration_jobs:
        migration_jobs[job_id].update(kwargs)


def log(job_id, message):
    """Add log message"""
    timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    log_message = f"[{timestamp}] {message}"
    
    if job_id in migration_logs:
        migration_logs[job_id].append(log_message)
    
    # Mask sensitive data in console/file logs
    safe_log = log_message
    for key in ['password', 'private_key', 'client_email']:
        if f"'{key}':" in safe_log or f'"{key}":' in safe_log:
            safe_log = re.sub(rf'"{key}":\s*"[^"]+"', f'"{key}": "****"', safe_log)
            safe_log = re.sub(rf"'{key}':\s*'[^']+'", f"'{key}': '****'", safe_log)
    
    print(safe_log)


if __name__ == '__main__':
    print("=" * 60)
    print(" 🚀 BlueQuery Migration Tool - API Server")
    print("=" * 60)
    print(f"Server starting, Access it at: http://localhost:5000")
    print("=" * 60)
    
    # Run with explicit security defaults
    app.run(host='0.0.0.0', port=5000, debug=False, threaded=True)
