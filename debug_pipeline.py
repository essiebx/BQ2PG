#!/usr/bin/env python3
import sys
import os

# Add src to path
sys.path.append('/app/src')

print("1. Checking environment...")
print(f"   POSTGRES_HOST: {os.getenv('POSTGRES_HOST', 'NOT SET')}")
print(f"   GOOGLE_CLOUD_PROJECT: {os.getenv('GOOGLE_CLOUD_PROJECT', 'NOT SET')}")

print("\n2. Testing imports...")
try:
    from config import config
    print("   [OK] config imported")
    
    from extract import BigQueryExtractor
    print("   [OK] extract imported")
    
    from load import PostgresLoader
    print("   [OK] load imported")
    
    from schema_mapper import generate_extraction_query
    print("   [OK] schema_mapper imported")
    
except ImportError as e:
    print(f"   [ERROR] Import error: {e}")
    sys.exit(1)

print("\n3. Testing configuration...")
try:
    config.validate()
    print("   [OK] Configuration valid")
except Exception as e:
    print(f"   [ERROR] Config error: {e}")
    sys.exit(1)

print("\n4. Testing BigQuery connection...")
try:
    extractor = BigQueryExtractor()
    if extractor.connect():
        print("   [OK] BigQuery connected")
        
        # Generate test query
        query = generate_extraction_query(limit=5)
        print(f"   [OK] Query generated (limit 5)")
        
        # Estimate cost
        bytes_processed, cost = extractor.estimate_cost(query)
        print(f"   💰 Estimated cost: ${cost:.6f}")
        
        # Try to extract
        print("   📥 Testing extraction...")
        df = extractor.extract_dataframe(query)
        print(f"   [OK] Extracted {len(df)} rows")
        print(f"   Columns: {list(df.columns)}")
    else:
        print("   [ERROR] BigQuery connection failed")
        
except Exception as e:
    print(f"   [ERROR] BigQuery error: {e}")

print("\n5. Testing PostgreSQL connection...")
try:
    loader = PostgresLoader()
    print("   [OK] PostgreSQL connected")
    
    # Test query
    with loader.engine.connect() as conn:
        result = conn.execute("SELECT 1 as test")
        print("   [OK] PostgreSQL query works")
        
except Exception as e:
    print(f"   [ERROR] PostgreSQL error: {e}")

print("\n🎉 Debug complete!")
