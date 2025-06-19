# Parameter Handling in Mapepire Python

This document explains how parameters are standardized across `execute()` and `executemany()` methods in the mapepire-python library.

## Overview

The mapepire server expects parameters as `Optional[List[Any]]`, but PEP 249 allows various parameter formats:

- `Sequence[Any]` (list, tuple, etc.)
- `Dict[Union[str, int], Any]` (named parameters)
- `None` (no parameters)

To ensure consistency, we've implemented a centralized `ParameterParser` class that converts all PEP 249 `QueryParameters` to the format expected by the mapepire server.

## Parameter Conversion Rules

### Single Parameter Sets (`execute()`)

| Input Type | Example | Converted Output | Notes |
|------------|---------|------------------|-------|
| `None` | `None` | `None` | No parameters |
| `tuple` | `(1, 2, 3)` | `[1, 2, 3]` | Standard sequence |
| `list` | `[1, 2, 3]` | `[1, 2, 3]` | Already correct format |
| `dict` | `{"b": 2, "a": 1}` | `[1, 2]` | Sorted by key for consistency |
| `str` | `"hello"` | `["hello"]` | Single string wrapped in list |
| `int/float` | `42` | `[42]` | Single value wrapped in list |

### Multiple Parameter Sets (`executemany()`)

The `executemany()` method processes each parameter set individually using the same rules as `execute()`, then combines them into a list of lists.

Example:
```python
# Input
seq_of_parameters = [
    (1, 2),           # tuple
    {"a": 3, "b": 4}, # dict  
    [5, 6]            # list
]

# Output
[[1, 2], [3, 4], [5, 6]]
```

## Usage Examples

### Basic Parameter Usage

```python
import mapepire_python as mp

# Tuple parameters (recommended)
cursor.execute("SELECT * FROM table WHERE id = ? AND name = ?", (123, "test"))

# List parameters
cursor.execute("SELECT * FROM table WHERE id = ? AND name = ?", [123, "test"])

# Named parameters (dict)
cursor.execute("SELECT * FROM table WHERE id = ? AND name = ?", {"0": 123, "1": "test"})

# Single parameter (automatic wrapping)
cursor.execute("SELECT * FROM table WHERE id = ?", 123)  # Becomes [123]
```

### ExecuteMany Usage

```python
# Multiple parameter sets
cursor.executemany(
    "INSERT INTO table (id, name) VALUES (?, ?)",
    [
        (1, "Alice"),
        (2, "Bob"),
        {"0": 3, "1": "Charlie"}  # Mixed types supported
    ]
)
```

## Implementation Details

### ParameterParser Class

The `ParameterParser` class provides static methods for parameter conversion:

- `parse_single_parameter_set()`: Converts single parameter set for `execute()`
- `parse_multiple_parameter_sets()`: Converts multiple parameter sets for `executemany()`
- `validate_parameter_count()`: Validates parameter count matches SQL parameter markers

### Integration Points

1. **QueryFactory**: Uses `ParameterParser.parse_single_parameter_set()` in `_build_query_options()`
2. **Cursor.execute()**: Automatically uses centralized parsing via QueryFactory
3. **Cursor.executemany()**: Calls `execute()` for each parameter set, inheriting the standardized parsing

## Error Handling

The parameter parser includes validation to catch common mistakes:

```python
# Parameter count mismatch
cursor.execute("SELECT ? FROM table", [1, 2])  # Raises ValueError

# Automatic handling of edge cases
cursor.execute("SELECT ? FROM table", "string")  # Converts to ["string"]
```

## Migration Guide

### Before (Inconsistent)

```python
# Different parameter handling in different parts of code
if isinstance(params, dict):
    converted = [params[k] for k in sorted(params.keys())]
elif isinstance(params, str):
    converted = list(params)  # WRONG: converts to chars
else:
    converted = list(params)
```

### After (Standardized)

```python
# Centralized parameter handling
from mapepire_python.core.parameter_parser import ParameterParser

converted = ParameterParser.parse_single_parameter_set(params)
```

## Benefits

1. **Consistency**: All parameter conversion follows the same rules
2. **Maintainability**: Single source of truth for parameter handling
3. **Robustness**: Handles edge cases like single strings correctly
4. **Validation**: Built-in parameter count validation
5. **Documentation**: Clear conversion rules and examples
