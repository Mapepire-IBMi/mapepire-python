"""
Parameter parsing utilities for converting PEP 249 QueryParameters to mapepire server format.

This module provides centralized parameter conversion to ensure consistency
between execute() and executemany() methods.
"""

from typing import TYPE_CHECKING, Any, List, Optional, Sequence

from pep249 import QueryParameters

if TYPE_CHECKING:
    from ..data_types import QueryOptions

__all__ = ["ParameterParser"]


class ParameterParser:
    """
    Centralized parser for converting PEP 249 QueryParameters to mapepire server format.

    The mapepire server expects parameters as Optional[List[Any]], but PEP 249 allows
    various formats:
    - Sequence[Any] (list, tuple, etc.)
    - Dict[Union[str, int], Any] (named parameters)
    - None (no parameters)

    This class handles all these cases and converts them to the expected format.
    """

    @staticmethod
    def parse_single_parameter_set(parameters: Optional[QueryParameters]) -> Optional[List[Any]]:
        """
        Parse a single parameter set for execute() method.

        Args:
            parameters: PEP 249 QueryParameters (sequence, dict, or None)

        Returns:
            Optional[List[Any]]: List of parameter values in the order expected by mapepire server

        Examples:
            >>> ParameterParser.parse_single_parameter_set((1, 2, 3))
            [1, 2, 3]
            >>> ParameterParser.parse_single_parameter_set({"a": 1, "b": 2})
            [1, 2]  # Sorted by key
            >>> ParameterParser.parse_single_parameter_set("single_string")
            ["single_string"]
            >>> ParameterParser.parse_single_parameter_set(None)
            None
        """
        if parameters is None:
            return None

        # Handle QueryOptions objects (extract the parameters field)
        if hasattr(parameters, "parameters") and hasattr(parameters, "isTerseResults"):
            # This is likely a QueryOptions object, extract the parameters field
            return parameters.parameters

        if isinstance(parameters, dict):
            # Convert dict to list of values in sorted key order for consistency
            # This ensures deterministic parameter ordering
            return [parameters[key] for key in sorted(parameters.keys())]

        elif isinstance(parameters, (list, tuple)):
            # Convert sequence to list
            return list(parameters)

        elif isinstance(parameters, str):
            # Special case: single string parameter should be wrapped in a list
            # This handles the common mistake of passing a string instead of (string,)
            return [parameters]

        else:
            # Handle other sequence types, but be careful not to iterate over strings
            try:
                # Check if it's iterable
                iter(parameters)
                # If it's not a string but is iterable, convert to list
                return list(parameters)
            except TypeError:
                # Single non-iterable value (int, float, etc.)
                return [parameters]

    @staticmethod
    def parse_multiple_parameter_sets(
        seq_of_parameters: Sequence[QueryParameters],
    ) -> List[List[Any]]:
        """
        Parse multiple parameter sets for executemany() method.

        Args:
            seq_of_parameters: Sequence of PEP 249 QueryParameters

        Returns:
            List[List[Any]]: List of parameter lists, each converted to mapepire format

        Examples:
            >>> ParameterParser.parse_multiple_parameter_sets([(1, 2), (3, 4)])
            [[1, 2], [3, 4]]
            >>> ParameterParser.parse_multiple_parameter_sets([{"a": 1}, {"a": 2}])
            [[1], [2]]
            >>> ParameterParser.parse_multiple_parameter_sets([])
            []
        """
        result = []
        for param_set in seq_of_parameters:
            parsed = ParameterParser.parse_single_parameter_set(param_set)
            # For executemany, we need a list of lists, so None becomes empty list
            result.append(parsed if parsed is not None else [])
        return result

    @staticmethod
    def validate_parameter_count(sql: str, parameters: Optional[List[Any]]) -> None:
        """
        Validate that the number of parameters matches the parameter markers in SQL.

        Args:
            sql: SQL query string
            parameters: Parsed parameter list

        Raises:
            ValueError: If parameter count doesn't match parameter markers

        Note:
            This is a basic validation that counts '?' characters. More sophisticated
            parsing would be needed to handle '?' in string literals, but this provides
            basic protection against common parameter count mismatches.
        """
        if parameters is None:
            parameters = []

        # Count parameter markers (?) in SQL
        # This is a simple count - doesn't handle ? in string literals
        marker_count = sql.count("?")
        param_count = len(parameters)

        if marker_count != param_count:
            raise ValueError(
                f"Parameter count mismatch: SQL has {marker_count} parameter markers "
                f"but {param_count} parameters were provided"
            )
