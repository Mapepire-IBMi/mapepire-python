#!/usr/bin/env python3
"""
Mapepire-Python Functionality Demo

This script demonstrates the key features and capabilities of mapepire-python,
a Python client library for connecting to IBM i systems via the Mapepire server.

Features demonstrated:
- Synchronous SQL execution with SQLJob
- Asynchronous connection pooling with Pool
- PEP 249 (DB API 2.0) compliance with Connection/Cursor
- Error handling and resource management
- Performance comparison between sync and async approaches

Requirements:
- IBM i server running Mapepire server component
- Network connectivity to IBM i server
- Valid IBM i credentials

Quick Start:
    # Install mapepire-python (if not already installed)
    pip install .

    # Run demo with default server (demo credentials)
    python demo.py

    # Or with your IBM i server details:
    python demo.py --host your.ibmi.server --user youruser --password yourpass

Environment Variables (alternative to command line):
    VITE_SERVER - IBM i server hostname
    VITE_DB_USER - IBM i username
    VITE_DB_PASS - IBM i password
    VITE_DB_PORT - Mapepire server port (default: 8076)
"""

import argparse
import asyncio
import os
import sys
import time
from typing import Any, Dict

# Import mapepire-python components
try:
    from mapepire_python.client.sql_job import SQLJob
    from mapepire_python.core.connection import Connection
    from mapepire_python.data_types import DaemonServer
    from mapepire_python.pool.optimized_pool_client import OptimizedPool
    from mapepire_python.pool.optimized_pool_client import (
        PoolOptions as OptimizedPoolOptions,
    )
    from mapepire_python.pool.pool_client import Pool, PoolOptions
except ImportError as e:
    print(f"❌ Error importing mapepire-python: {e}")
    print("Please ensure mapepire-python is installed: pip install .")
    sys.exit(1)


class MapepireDemo:
    """Comprehensive demonstration of mapepire-python functionality."""

    def __init__(self, host: str, user: str, password: str, port: int = 8076):
        self.server_config = {"host": host, "user": user, "password": password, "port": port}
        print(f"🚀 Mapepire-Python Demo")
        print(f"📡 Server: {host}:{port}")
        print(f"👤 User: {user}")
        print("=" * 60)

    def run_all_demos(self):
        """Run all demonstration scenarios."""
        print("\n🎯 Running comprehensive mapepire-python functionality demo...\n")

        try:
            # 1. Synchronous SQL execution
            print("1️⃣ SYNCHRONOUS SQL EXECUTION (SQLJob)")
            self.demo_sync_sql_job()

            # 2. PEP 249 DB API compliance
            print("\n2️⃣ PEP 249 DB API 2.0 COMPLIANCE (Connection/Cursor)")
            self.demo_pep249_interface()

            # 3. Asynchronous connection pooling
            print("\n3️⃣ ASYNCHRONOUS CONNECTION POOLING (Pool)")
            asyncio.run(self.demo_async_pool())

            # 4. Performance comparison
            print("\n4️⃣ PERFORMANCE COMPARISON")
            asyncio.run(self.demo_performance_comparison())

            # 5. Error handling
            print("\n5️⃣ ERROR HANDLING & RESOURCE MANAGEMENT")
            self.demo_error_handling()

            print("\n" + "=" * 60)
            print("✅ All demos completed successfully!")
            print("🎉 Mapepire-Python is working correctly with your IBM i server.")

        except Exception as e:
            print(f"\n❌ Demo failed: {e}")
            print("\n🔧 Troubleshooting tips:")
            print("• Verify IBM i server is running and accessible")
            print("• Check Mapepire server component is installed and running")
            print("• Confirm credentials are correct")
            print("• Ensure network connectivity to server")

    def demo_sync_sql_job(self):
        """Demonstrate synchronous SQL execution with SQLJob."""
        print("   Testing basic connection and query execution...")

        # Create SQLJob and execute queries
        with SQLJob(self.server_config) as job:
            # Simple query
            result1 = job.query_and_run("VALUES (CURRENT TIMESTAMP)")
            print(f"   ✓ Current timestamp: {result1['data'][0] if result1['data'] else 'N/A'}")

            # Job information query
            result2 = job.query_and_run("VALUES (JOB_NAME)")
            print(f"   ✓ Job name: {result2['data'][0] if result2['data'] else 'N/A'}")

            # System information
            result3 = job.query_and_run("VALUES (CURRENT SERVER)")
            print(f"   ✓ Current server: {result3['data'][0] if result3['data'] else 'N/A'}")

        print("   ✅ Synchronous SQL execution completed")

    def demo_pep249_interface(self):
        """Demonstrate PEP 249 DB API 2.0 compliance."""
        print("   Testing PEP 249 Connection and Cursor interface...")

        # Use Connection and Cursor (PEP 249 interface)
        with Connection(self.server_config) as conn:
            with conn.cursor() as cursor:
                # Execute query
                cursor.execute("VALUES (CURRENT USER)")
                result = cursor.fetchone()
                print(f"   ✓ Current user: {result}")

                # Execute query with description
                cursor.execute("VALUES (CURRENT TIMESTAMP, CURRENT USER)")
                print(f"   ✓ Column descriptions: {[desc[0] for desc in cursor.description]}")
                result = cursor.fetchone()
                print(f"   ✓ Query result: {result}")

        print("   ✅ PEP 249 interface validation completed")

    async def demo_async_pool(self):
        """Demonstrate asynchronous connection pooling."""
        print("   Testing asynchronous connection pool with concurrent queries...")

        # Create connection pool
        pool_options = OptimizedPoolOptions(creds=self.server_config, max_size=5, starting_size=2)

        async with OptimizedPool(pool_options) as pool:
            # Execute concurrent queries
            queries = [
                "VALUES (CURRENT TIMESTAMP)",
                "VALUES (JOB_NAME)",
                "VALUES (CURRENT USER)",
                "VALUES (CURRENT SERVER)",
                "VALUES (CURRENT SCHEMA)",
            ]

            print(f"   Running {len(queries)} concurrent queries...")
            start_time = time.perf_counter()

            # Execute all queries concurrently
            tasks = [pool.execute(query) for query in queries]
            results = await asyncio.gather(*tasks)

            execution_time = time.perf_counter() - start_time

            print(f"   ✓ All queries completed in {execution_time:.3f}s")
            print(f"   ✓ Pool stats: {pool.get_active_job_count()} active jobs")

            # Show sample results
            for i, result in enumerate(results[:3]):  # Show first 3 results
                if result["data"]:
                    print(f"   ✓ Query {i+1} result: {result['data'][0]}")

        print("   ✅ Asynchronous connection pooling completed")

    async def demo_performance_comparison(self):
        """Compare performance between sync and async approaches."""
        print("   Comparing sync vs async performance...")

        test_query = "VALUES (CURRENT TIMESTAMP)"
        num_queries = 10

        # Test synchronous performance
        print(f"   Testing {num_queries} synchronous queries...")
        sync_start = time.perf_counter()

        with SQLJob(self.server_config) as job:
            for _ in range(num_queries):
                job.query_and_run(test_query)

        sync_time = time.perf_counter() - sync_start

        # Test asynchronous performance
        print(f"   Testing {num_queries} asynchronous queries...")
        async_start = time.perf_counter()

        pool_options = OptimizedPoolOptions(creds=self.server_config, max_size=3, starting_size=2)
        async with OptimizedPool(pool_options) as pool:
            tasks = [pool.execute(test_query) for _ in range(num_queries)]
            await asyncio.gather(*tasks)

        async_time = time.perf_counter() - async_start

        # Calculate performance difference
        improvement = ((sync_time - async_time) / sync_time * 100) if sync_time > 0 else 0

        print(f"   📊 Performance Results:")
        print(f"      Synchronous: {sync_time:.3f}s ({num_queries/sync_time:.1f} QPS)")
        print(f"      Asynchronous: {async_time:.3f}s ({num_queries/async_time:.1f} QPS)")
        print(f"      Performance difference: {improvement:+.1f}%")

        print("   ✅ Performance comparison completed")

    def demo_error_handling(self):
        """Demonstrate error handling and resource management."""
        print("   Testing error handling and resource management...")

        try:
            # Test connection with invalid query
            with SQLJob(self.server_config) as job:
                try:
                    result = job.query_and_run("SELECT * FROM NONEXISTENT_TABLE")
                except Exception as e:
                    print(f"   ✓ Query error handled: {type(e).__name__}")

                # Test successful query after error
                result = job.query_and_run("VALUES (1)")
                print(f"   ✓ Connection recovered after error: {result['success']}")

        except Exception as e:
            print(f"   ✓ Connection error handled: {type(e).__name__}")

        # Test resource cleanup
        print("   ✓ Resources automatically cleaned up (context managers)")
        print("   ✅ Error handling demonstration completed")


def get_credentials() -> Dict[str, Any]:
    """Get IBM i credentials from environment or user input."""
    from dotenv import load_dotenv

    load_dotenv()
    # Try environment variables first
    host = os.getenv("VITE_SERVER")
    user = os.getenv("VITE_DB_USER")
    password = os.getenv("VITE_DB_PASS")
    port = int(os.getenv("VITE_DB_PORT", "8076"))

    # If environment variables are available, use them
    if host and user and password:
        return {"host": host, "user": user, "password": password, "port": port}


def main():
    """Main entry point for the demo application."""
    parser = argparse.ArgumentParser(
        description="Mapepire-Python functionality demonstration",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
    # Use environment variables or defaults
    python demo.py
    
    # Specify server details
    python demo.py --host your.server.com --user youruser --password yourpass
    
    # Use different port
    python demo.py --host your.server.com --user youruser --password yourpass --port 8443

Environment Variables:
    VITE_SERVER    - IBM i server hostname
    VITE_DB_USER   - IBM i username
    VITE_DB_PASS   - IBM i password  
    VITE_DB_PORT   - Mapepire server port (default: 8076)
        """,
    )

    # Get default credentials
    defaults = get_credentials()

    parser.add_argument(
        "--host",
        default=defaults["host"],
        help=f"IBM i server hostname (default: {defaults['host']})",
    )
    parser.add_argument(
        "--user", default=defaults["user"], help=f"IBM i username (default: {defaults['user']})"
    )
    parser.add_argument("--password", default=defaults["password"], help="IBM i password")
    parser.add_argument(
        "--port",
        type=int,
        default=defaults["port"],
        help=f"Mapepire server port (default: {defaults['port']})",
    )

    args = parser.parse_args()

    # Validate required arguments
    if not all([args.host, args.user, args.password]):
        print("❌ Error: Missing required connection details")
        print("Please provide --host, --user, and --password, or set environment variables")
        print("Run 'python demo.py --help' for more information")
        sys.exit(1)

    # Run the demo
    try:
        demo = MapepireDemo(args.host, args.user, args.password, args.port)
        demo.run_all_demos()
    except KeyboardInterrupt:
        print("\n⚠️ Demo interrupted by user")
    except Exception as e:
        print(f"\n❌ Demo failed with error: {e}")
        print("\n🔧 Troubleshooting:")
        print("• Check your IBM i server connection details")
        print("• Verify the Mapepire server component is running")
        print("• Ensure network connectivity to the server")
        sys.exit(1)


if __name__ == "__main__":
    main()
