"""
Tests for the centralized parameter parser.
"""

import pytest

from mapepire_python.core.parameter_parser import ParameterParser
from mapepire_python.data_types import QueryOptions


class TestParameterParser:
    """Test cases for the ParameterParser class."""

    def test_parse_single_parameter_set_none(self):
        """Test parsing None parameters."""
        result = ParameterParser.parse_single_parameter_set(None)
        assert result is None

    def test_parse_single_parameter_set_tuple(self):
        """Test parsing tuple parameters."""
        result = ParameterParser.parse_single_parameter_set((1, 2, 3))
        assert result == [1, 2, 3]

    def test_parse_single_parameter_set_list(self):
        """Test parsing list parameters."""
        result = ParameterParser.parse_single_parameter_set([1, 2, 3])
        assert result == [1, 2, 3]

    def test_parse_single_parameter_set_dict(self):
        """Test parsing dict parameters with sorted keys."""
        result = ParameterParser.parse_single_parameter_set({"b": 2, "a": 1, "c": 3})
        assert result == [1, 2, 3]  # Sorted by key: a, b, c

    def test_parse_single_parameter_set_string(self):
        """Test parsing single string parameter."""
        result = ParameterParser.parse_single_parameter_set("test_string")
        assert result == ["test_string"]

    def test_parse_single_parameter_set_single_value(self):
        """Test parsing single non-iterable value."""
        result = ParameterParser.parse_single_parameter_set(42)
        assert result == [42]

    def test_parse_single_parameter_set_query_options(self):
        """Test parsing QueryOptions object."""
        options = QueryOptions(parameters=[1, 2, 3])
        result = ParameterParser.parse_single_parameter_set(options)
        assert result == [1, 2, 3]

    def test_parse_single_parameter_set_query_options_none(self):
        """Test parsing QueryOptions object with None parameters."""
        options = QueryOptions(parameters=None)
        result = ParameterParser.parse_single_parameter_set(options)
        assert result is None

    def test_parse_single_parameter_set_empty_string(self):
        """Test parsing empty string parameter."""
        result = ParameterParser.parse_single_parameter_set("")
        assert result == [""]

    def test_parse_single_parameter_set_boolean(self):
        """Test parsing boolean parameters."""
        result = ParameterParser.parse_single_parameter_set(True)
        assert result == [True]

        result = ParameterParser.parse_single_parameter_set(False)
        assert result == [False]

    def test_parse_single_parameter_set_float(self):
        """Test parsing float parameters."""
        result = ParameterParser.parse_single_parameter_set(3.14)
        assert result == [3.14]

    def test_parse_multiple_parameter_sets_empty(self):
        """Test parsing empty sequence."""
        result = ParameterParser.parse_multiple_parameter_sets([])
        assert result == []

    def test_parse_multiple_parameter_sets_tuples(self):
        """Test parsing multiple tuple parameter sets."""
        result = ParameterParser.parse_multiple_parameter_sets([(1, 2), (3, 4)])
        assert result == [[1, 2], [3, 4]]

    def test_parse_multiple_parameter_sets_dicts(self):
        """Test parsing multiple dict parameter sets."""
        result = ParameterParser.parse_multiple_parameter_sets([{"a": 1, "b": 2}, {"a": 3, "b": 4}])
        assert result == [[1, 2], [3, 4]]

    def test_parse_multiple_parameter_sets_mixed(self):
        """Test parsing mixed parameter set types."""
        result = ParameterParser.parse_multiple_parameter_sets([(1, 2), {"a": 3, "b": 4}, [5, 6]])
        assert result == [[1, 2], [3, 4], [5, 6]]

    def test_parse_multiple_parameter_sets_with_none(self):
        """Test parsing parameter sets including None values."""
        result = ParameterParser.parse_multiple_parameter_sets([(1, 2), None, (3, 4)])
        assert result == [[1, 2], [], [3, 4]]

    def test_parse_multiple_parameter_sets_with_strings(self):
        """Test parsing parameter sets with string parameters."""
        result = ParameterParser.parse_multiple_parameter_sets([("hello",), ("world",)])
        assert result == [["hello"], ["world"]]

    def test_parse_multiple_parameter_sets_single_values(self):
        """Test parsing parameter sets with single values."""
        result = ParameterParser.parse_multiple_parameter_sets([42, "test", 3.14])
        assert result == [[42], ["test"], [3.14]]

    def test_validate_parameter_count_match(self):
        """Test parameter count validation when counts match."""
        # Should not raise
        ParameterParser.validate_parameter_count("SELECT ? FROM table WHERE ? = ?", [1, 2, 3])

    def test_validate_parameter_count_single_parameter(self):
        """Test parameter count validation with single parameter."""
        # Should not raise
        ParameterParser.validate_parameter_count("SELECT ? FROM table", [1])

    def test_validate_parameter_count_no_parameters(self):
        """Test parameter count validation with no parameters."""
        # Should not raise
        ParameterParser.validate_parameter_count("SELECT 1 FROM table", [])

    def test_validate_parameter_count_mismatch(self):
        """Test parameter count validation when counts don't match."""
        with pytest.raises(ValueError, match="Parameter count mismatch"):
            ParameterParser.validate_parameter_count("SELECT ? FROM table", [1, 2])

    def test_validate_parameter_count_none_parameters(self):
        """Test parameter count validation with None parameters."""
        with pytest.raises(ValueError, match="Parameter count mismatch"):
            ParameterParser.validate_parameter_count("SELECT ? FROM table", None)

    def test_validate_parameter_count_no_markers(self):
        """Test parameter count validation with no parameter markers."""
        # Should not raise
        ParameterParser.validate_parameter_count("SELECT 1 FROM table", [])
        ParameterParser.validate_parameter_count("SELECT 1 FROM table", None)

    def test_validate_parameter_count_multiple_question_marks_in_string(self):
        """Test parameter count validation with ? in string literals (basic test)."""
        # Note: This is a limitation - we don't parse string literals
        # But we test the basic counting behavior, this fails
        with pytest.raises(ValueError):
            ParameterParser.validate_parameter_count("SELECT '?' FROM table WHERE ? = ?", [1, 2])

    def test_parse_edge_cases(self):
        """Test parsing of various edge cases."""
        # Empty tuple
        result = ParameterParser.parse_single_parameter_set(())
        assert result == []

        # Empty list
        result = ParameterParser.parse_single_parameter_set([])
        assert result == []

        # Empty dict
        result = ParameterParser.parse_single_parameter_set({})
        assert result == []

        # Nested structures (should flatten one level)
        result = ParameterParser.parse_single_parameter_set([(1, 2), (3, 4)])
        assert result == [(1, 2), (3, 4)]  # Lists contain tuples as elements
