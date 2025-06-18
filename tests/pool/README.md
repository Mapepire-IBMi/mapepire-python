# Pool Testing Suite

This directory contains comprehensive tests for both the original Pool and optimized OptimizedPool implementations.

## Test Files

### Core Pool Tests
- **`test_pool_client.py`** - Original pool tests using real IBM i server
- **`test_pool_job.py`** - Pool job functionality tests  
- **`test_optimized_pool.py`** - Mock-based optimized pool unit tests

### Performance Tests
- **`test_pool_performance_comparison.py`** - Performance comparison between original and optimized pools

## Running Tests

### Standard Pool Tests
```bash
# Run all pool tests
pytest pool/ -v

# Run original pool tests only
pytest pool/test_pool_client.py -v

# Run optimized pool tests only  
pytest pool/test_optimized_pool.py -v
```

### Performance Tests (IBM i Required)

**Prerequisites:**
```bash
pip install pytest-asyncio
```

**Run Performance Tests:**
```bash
# Comprehensive performance summary (recommended)
python ../run_pool_performance_simple.py --test summary --verbose

# All performance tests
python ../run_pool_performance_simple.py --all --verbose

# Direct pytest
pytest -m performance_ibmi -v -s
```

## Test Markers

- **`performance_ibmi`** - Performance tests requiring IBM i server
- **`slow`** - Long-running tests (>5 seconds)

Skip performance tests in normal runs:
```bash
pytest -m "not performance_ibmi"
```

## Expected Performance Results

The optimized pool should demonstrate:
- **O(1) job selection** vs O(n) linear search
- **>80% cache hit ratios** 
- **Better concurrent load handling**
- **Improved resource efficiency**

## IBM i Configuration

Tests use credentials from `../pytest.ini`:
```ini
env =
    VITE_SERVER=OSSBUILD.rzkh.de
    VITE_DB_USER=MAPEPIRE
    VITE_DB_PASS=bushmaster3000
    VITE_DB_PORT=8076
```