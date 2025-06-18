#!/usr/bin/env python3
"""
Simple script to run pool performance tests against IBM i.
"""

import subprocess
import sys
import argparse
import os
from pathlib import Path


def setup_environment():
    """Set up environment variables for IBM i testing."""
    # Set IBM i credentials from pytest.ini defaults
    env_vars = {
        "VITE_SERVER": "OSSBUILD.rzkh.de",
        "VITE_DB_USER": "MAPEPIRE", 
        "VITE_DB_PASS": "bushmaster3000",
        "VITE_DB_PORT": "8076"
    }
    
    for key, value in env_vars.items():
        if key not in os.environ:
            os.environ[key] = value
            print(f"Set {key}={value}")


def check_dependencies():
    """Check if required dependencies are installed."""
    missing = []
    
    try:
        import pytest_asyncio
    except ImportError:
        missing.append("pytest-asyncio")
    
    if missing:
        print("❌ Missing required dependencies:")
        for dep in missing:
            print(f"   • {dep}")
        print("\nInstall with:")
        print(f"   pip install {' '.join(missing)}")
        print("\nOr install all test dependencies:")
        print("   pip install pytest-asyncio pytest-timeout")
        return False
    
    return True


def main():
    parser = argparse.ArgumentParser(description="Run pool performance tests against IBM i server")
    
    parser.add_argument("--all", action="store_true", help="Run all performance tests")
    parser.add_argument("--test", choices=["init", "query", "concurrent", "summary", "basic", "small_concurrent"], help="Run specific test")
    parser.add_argument("--verbose", action="store_true", help="Show verbose output")
    
    args = parser.parse_args()
    
    if not args.all and not args.test:
        print("❌ Please specify either --all or --test <testname>")
        parser.print_help()
        sys.exit(1)
    
    # Set up environment variables
    setup_environment()
    
    # Check dependencies first
    if not check_dependencies():
        sys.exit(1)
    
    # Base pytest command
    cmd = ["python", "-m", "pytest"]
    
    if args.verbose:
        cmd.extend(["-v", "-s"])
    else:
        cmd.append("-v")
    
    if args.all:
        # Run all performance tests
        cmd.extend(["-m", "performance_ibmi"])
        print("🚀 Running all pool performance tests...")
    else:
        # Run specific test
        test_map = {
            "init": "pool/test_pool_performance_comparison.py::test_pool_initialization_performance",
            "query": "pool/test_pool_performance_comparison.py::test_query_execution_performance", 
            "concurrent": "pool/test_pool_performance_comparison.py::test_concurrent_load_performance",
            "summary": "pool/test_pool_performance_comparison.py::test_comprehensive_performance_summary",
            "basic": "pool/test_pool_basic_performance.py::test_basic_pool_functionality_comparison",
            "small_concurrent": "pool/test_pool_basic_performance.py::test_small_concurrent_load"
        }
        
        test_path = test_map[args.test]
        cmd.append(test_path)
        print(f"🚀 Running {args.test} performance test...")
    
    print(f"Command: {' '.join(cmd)}")
    print()
    
    try:
        subprocess.run(cmd, check=True)
        print("✅ Performance tests completed successfully!")
    except subprocess.CalledProcessError as e:
        print(f"❌ Performance tests failed with exit code {e.returncode}")
        sys.exit(1)
    except KeyboardInterrupt:
        print("⚠️ Tests interrupted by user")
        sys.exit(1)


if __name__ == "__main__":
    main()