# Pool Performance Testing Guide

## Quick Start

### 1. Install Dependencies
```bash
pip install pytest-asyncio
```

### 2. Run Performance Tests
```bash
# Comprehensive summary (recommended)
python run_pool_performance_simple.py --test summary --verbose

# All performance tests
python run_pool_performance_simple.py --all --verbose

# Direct pytest alternative
pytest -m performance_ibmi -v -s
```

### 3. Individual Tests
```bash
# Pool initialization
python run_pool_performance_simple.py --test init --verbose

# Query execution
python run_pool_performance_simple.py --test query --verbose

# Concurrent load
python run_pool_performance_simple.py --test concurrent --verbose
```

## Expected Results

Performance tests validate:
- **O(1) job selection** vs O(n) linear search
- **Cache hit ratios** >80% 
- **Concurrent load improvements**
- **Resource efficiency** gains

## Troubleshooting

**Dependencies Missing:**
```bash
pip install pytest-asyncio
```

**Connection Issues:**
- Check IBM i server connectivity
- Verify credentials in `pytest.ini`

## Key Success Indicators
- Cache hit ratios >80%
- Ready queue utilization indicating O(1) efficiency
- Performance competitive with or better than original
- Resource efficiency improvements