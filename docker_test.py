#!/usr/bin/env python3
# docker_test.py - Test Podman environment

def main():
    print("=" * 60)
    print("🧪 Podman Environment Test")
    print("=" * 60)
    
    tests = [
        ("Python Version", test_python),
        ("Imports", test_imports),
        ("Environment", test_environment),
        ("Filesystem", test_filesystem),
    ]
    
    results = []
    for name, test_func in tests:
        print(f"\n📊 {name}:")
        try:
            result = test_func()
            results.append((name, True, result))
            print(f"  [OK] {result}")
        except Exception as e:
            results.append((name, False, str(e)))
            print(f"  [ERROR] {e}")
    
    print("\n" + "=" * 60)
    print("📈 Test Summary:")
    print("=" * 60)
    
    for name, success, message in results:
        status = "[OK]" if success else "[ERROR]"
        print(f"{status} {name}: {message}")
    
    all_passed = all(success for _, success, _ in results)
    if all_passed:
        print("\n🎉 All tests passed! Environment is ready.")
        print("\nNext commands:")
        print("  make test      # Run pipeline test")
        print("  make psql      # Connect to database")
        print("  make pgadmin   # Open admin interface")
    else:
        print("\n[WARNING]  Some tests failed. Check configuration.")
    
    return 0 if all_passed else 1

def test_python():
    import sys
    version = sys.version_info
    return f"Python {version.major}.{version.minor}.{version.micro}"

def test_imports():
    import pandas as pd
    import sqlalchemy as sa
    import numpy as np
    from google.cloud import bigquery
    import psycopg2
    return f"All imports successful (pandas {pd.__version__})"

def test_environment():
    import os
    required_vars = [
        'POSTGRES_HOST',
        'POSTGRES_DB', 
        'POSTGRES_USER'
    ]
    
    missing = [var for var in required_vars if not os.getenv(var)]
    if missing:
        raise ValueError(f"Missing env vars: {missing}")
    
    present = {var: os.getenv(var) for var in required_vars}
    return f"Environment OK: {present}"

def test_filesystem():
    import os
    required_dirs = ['/app', '/app/src', '/app/credentials']
    missing = [d for d in required_dirs if not os.path.exists(d)]
    
    if missing:
        raise ValueError(f"Missing directories: {missing}")
    
    return "Filesystem structure OK"

if __name__ == "__main__":
    exit(main())