#!/usr/bin/env python3
"""
Convenience script to run pool performance tests against IBM i.

This script provides an easy way to execute performance comparisons between 
the original and optimized pool implementations.
"""

import subprocess
import sys
import argparse
from pathlib import Path


def run_command(cmd, description):
    """Run a command and handle output."""
    print(f"\n{'='*60}")
    print(f"🚀 {description}")
    print(f"{'='*60}")
    print(f"Command: {' '.join(cmd)}")
    print()
    
    try:
        result = subprocess.run(cmd, check=True, capture_output=False, text=True)
        print(f"\n✅ {description} completed successfully!")
        return True
    except subprocess.CalledProcessError as e:
        print(f"\n❌ {description} failed with exit code {e.returncode}")
        return False
    except KeyboardInterrupt:
        print(f"\n⚠️ {description} interrupted by user")
        return False


def main():
    parser = argparse.ArgumentParser(
        description="Run pool performance tests against IBM i server",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Run all performance tests
  python run_pool_performance.py --all

  # Run only initialization performance test
  python run_pool_performance.py --test init

  # Run with verbose output and custom timeout
  python run_pool_performance.py --all --verbose --timeout 600

  # Quick performance summary
  python run_pool_performance.py --test summary
        """
    )
    
    parser.add_argument(
        "--all", 
        action="store_true",
        help="Run all performance tests"
    )
    
    parser.add_argument(
        "--test",
        choices=["init", "query", "concurrent", "summary"],
        help="Run specific performance test"
    )
    
    parser.add_argument(
        "--verbose",
        action="store_true", 
        help="Show verbose output"
    )
    
    parser.add_argument(
        "--timeout",
        type=int,
        default=300,
        help="Test timeout in seconds (default: 300)"
    )
    
    parser.add_argument(
        "--no-capture",
        action="store_true",
        help="Don't capture output (show live output)"
    )
    
    args = parser.parse_args()
    
    if not args.all and not args.test:
        print("❌ Please specify either --all or --test <testname>")
        parser.print_help()
        sys.exit(1)
    
    # Change to tests directory
    tests_dir = Path(__file__).parent
    print(f"📁 Working directory: {tests_dir}")
    
    # Base pytest command
    base_cmd = ["python", "-m", "pytest"]
    
    # Add common flags
    if args.verbose:
        base_cmd.extend(["-v", "-s"])
    else:
        base_cmd.extend(["-v"])
    
    if args.no_capture:
        base_cmd.append("-s")
    
    # Add timeout if pytest-timeout is available
    try:
        import pytest_timeout
        base_cmd.extend(["--timeout", str(args.timeout)])
    except ImportError:
        print(f"⚠️  pytest-timeout not installed, skipping timeout of {args.timeout}s")
        print("   Install with: pip install pytest-timeout")
    
    success_count = 0
    total_tests = 0
    
    if args.all:
        # Run all performance tests
        cmd = base_cmd + ["-m", "performance_ibmi"]
        total_tests = 1
        
        if run_command(cmd, "All Pool Performance Tests"):
            success_count += 1
    
    elif args.test:
        # Run specific test
        test_map = {
            "init": "test_pool_initialization_performance",
            "query": "test_query_execution_performance", 
            "concurrent": "test_concurrent_load_performance",
            "summary": "test_comprehensive_performance_summary"
        }
        
        test_name = test_map[args.test]
        test_file = "pool/test_pool_performance_comparison.py"
        cmd = base_cmd + [f"{test_file}::{test_name}"]
        total_tests = 1
        
        if run_command(cmd, f"Pool Performance Test: {args.test}"):
            success_count += 1
    
    # Summary
    print(f"\n{'='*60}")
    print(f"📊 PERFORMANCE TEST SUMMARY")
    print(f"{'='*60}")
    print(f"Tests completed: {success_count}/{total_tests}")
    
    if success_count == total_tests:
        print("✅ All performance tests completed successfully!")
        print("\n💡 Key insights to look for:")
        print("  • Initialization time improvements with pre-warming")
        print("  • Query execution performance (QPS improvements)")
        print("  • Cache hit ratios (should be >80%)")
        print("  • Ready queue utilization under load")
        print("  • Resource efficiency (jobs created vs queries)")
        sys.exit(0)
    else:
        print("❌ Some performance tests failed!")
        print("\n🔧 Troubleshooting tips:")
        print("  • Check IBM i server connectivity")
        print("  • Verify credentials in pytest.ini")
        print("  • Increase timeout with --timeout option")
        print("  • Run with --verbose for detailed output")
        sys.exit(1)


if __name__ == "__main__":
    main()