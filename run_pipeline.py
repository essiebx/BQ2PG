#!/usr/bin/env python3
"""
Simple pipeline runner that works with Podman.
"""
import sys
import os

# Add src to path
sys.path.insert(0, '/app/src')

# Set command line arguments for the main script
sys.argv = ['main.py', '--test']

# Import and run main
try:
    from main import main
    exit_code = main()
    sys.exit(exit_code)
except ImportError as e:
    print(f"❌ Import error: {e}")
    print("\n💡 Check that:")
    print("  1. src/ directory exists with Python files")
    print("  2. main.py is in the root directory")
    sys.exit(1)
