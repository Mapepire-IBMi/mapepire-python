"""
Integration tests for parameter handling with actual IBM i database connections.
These tests verify that the centralized parameter parser works correctly with real queries.
"""

from mapepire_python import connect


class TestParameterHandlingIBMi:
    """Test parameter handling with actual IBM i database queries."""

    def test_parameter_parser_basic_types(self, ibmi_credentials):
        """Test parameter parser with basic data types against IBM i."""
        with connect(ibmi_credentials) as conn:
            cursor = conn.cursor()

            # Test various basic types
            test_cases = [
                # (parameter_value, expected_result)
                (1, 1),
                (42, 42),
                (0, 0),
                (-5, -5),
                (3.14, 3.14),
                (-2.5, -2.5),
            ]

            for param_value, expected in test_cases:
                # Test tuple format
                cursor.execute("SELECT 1 FROM sysibm.sysdummy1 WHERE 1 = ?", (param_value,))
                result = cursor.fetchone()
                if param_value == 1 or param_value is True:
                    assert result is not None, f"Failed for parameter: {param_value}"

                # Test list format
                cursor.execute("SELECT 1 FROM sysibm.sysdummy1 WHERE 1 = ?", [param_value])
                result = cursor.fetchone()
                if param_value == 1 or param_value is True:
                    assert result is not None, f"Failed for parameter: {param_value}"

    # def test_parameter_parser_string_types(self, ibmi_credentials):
    #     """Test parameter parser with various string types."""
    #     with connect(ibmi_credentials) as conn:
    #         cursor = conn.cursor()

    #         string_test_cases = [
    #             "simple_string",
    #             "string with spaces",
    #             "string_with_underscores",
    #             "String With Mixed Case",
    #             "123numeric_string",
    #             "special!@#$%chars",
    #             "unicode_test_ñáéíóú",
    #             "emoji_test_🚀🎉",
    #             "'quoted_string'",
    #             '"double_quoted"',
    #             "string\nwith\nnewlines",
    #             "string\twith\ttabs",
    #         ]

    #         for test_string in string_test_cases:
    #             # Test with string equality - should return the string back
    #             cursor.execute("SELECT ? FROM sysibm.sysdummy1", (test_string,))
    #             result = cursor.fetchone()
    #             assert result is not None, f"Failed for string: {test_string}"
    #             assert (
    #                 result[0] == test_string
    #             ), f"String mismatch: expected {test_string}, got {result[0]}"

    # def test_parameter_parser_numeric_edge_cases(self, ibmi_credentials):
    #     """Test parameter parser with numeric edge cases."""
    #     with connect(ibmi_credentials) as conn:
    #         cursor = conn.cursor()

    #         numeric_test_cases = [
    #             0,
    #             1,
    #             -1,
    #             999999999,
    #             -999999999,
    #             0.0,
    #             1.0,
    #             -1.0,
    #             3.14159,
    #             -2.71828,
    #             0.000001,
    #             999999.999999,
    #         ]

    #         for num in numeric_test_cases:
    #             cursor.execute("SELECT ? FROM sysibm.sysdummy1", (num,))
    #             result = cursor.fetchone()
    #             assert result is not None, f"Failed for number: {num}"
    #             # Allow for floating point precision differences
    #             if isinstance(num, float):
    #                 assert (
    #                     abs(result[0] - num) < 0.000001
    #                 ), f"Number mismatch: expected {num}, got {result[0]}"
    #             else:
    #                 assert result[0] == num, f"Number mismatch: expected {num}, got {result[0]}"

    # def test_parameter_parser_null_handling(self, ibmi_credentials):
    #     """Test parameter parser with NULL values."""
    #     with connect(ibmi_credentials) as conn:
    #         cursor = conn.cursor()

    #         # Test NULL parameter
    #         cursor.execute("SELECT ? FROM sysibm.sysdummy1", (None,))
    #         result = cursor.fetchone()
    #         assert result is not None
    #         assert result[0] is None, "NULL parameter not handled correctly"

    #         # Test NULL in WHERE clause
    #         cursor.execute("SELECT 1 FROM sysibm.sysdummy1 WHERE ? IS NULL", (None,))
    #         result = cursor.fetchone()
    #         assert result is not None, "NULL in WHERE clause not handled"

    # def test_parameter_parser_multiple_parameters(self, ibmi_credentials):
    #     """Test parameter parser with multiple parameters in one query."""
    #     with connect(ibmi_credentials) as conn:
    #         cursor = conn.cursor()

    #         # Test multiple parameters of same type
    #         cursor.execute(
    #             "SELECT CASE WHEN ? = ? THEN 1 ELSE 0 END FROM sysibm.sysdummy1", (42, 42)
    #         )
    #         result = cursor.fetchone()
    #         assert result[0] == 1, "Multiple integer parameters failed"

    #         # Test multiple parameters of different types
    #         cursor.execute(
    #             "SELECT CASE WHEN ? = ? AND ? = ? THEN 1 ELSE 0 END FROM sysibm.sysdummy1",
    #             (1, 1, "test", "test"),
    #         )
    #         result = cursor.fetchone()
    #         assert result[0] == 1, "Multiple mixed parameters failed"

    #         # Test with list format
    #         cursor.execute(
    #             "SELECT CASE WHEN ? = ? THEN 1 ELSE 0 END FROM sysibm.sysdummy1", [100, 100]
    #         )
    #         result = cursor.fetchone()
    #         assert result[0] == 1, "Multiple parameters in list format failed"

    # def test_parameter_parser_dict_parameters(self, ibmi_credentials):
    #     """Test parameter parser with dictionary parameters."""
    #     with connect(ibmi_credentials) as conn:
    #         cursor = conn.cursor()

    #         # Test with simple dict (should be sorted by key)
    #         cursor.execute(
    #             "SELECT CASE WHEN ? = ? THEN 1 ELSE 0 END FROM sysibm.sysdummy1",
    #             {"b": 2, "a": 2},  # Should become [2, 2] after sorting
    #         )
    #         result = cursor.fetchone()
    #         assert result[0] == 1, "Dict parameters not sorted correctly"

    #         # Test with numbered keys
    #         cursor.execute("SELECT ? FROM sysibm.sysdummy1", {"0": "first_param"})
    #         result = cursor.fetchone()
    #         assert result[0] == "first_param", "Numbered dict parameter failed"

    # def test_parameter_parser_executemany(self, ibmi_credentials):
    #     """Test parameter parser with executemany method."""
    #     with connect(ibmi_credentials) as conn:
    #         cursor = conn.cursor()

    #         # Test executemany with multiple parameter sets
    #         parameter_sets = [
    #             (1,),
    #             (2,),
    #             (3,),
    #         ]

    #         cursor.executemany("SELECT ? FROM sysibm.sysdummy1", parameter_sets)
    #         # executemany doesn't return results, but should not raise errors

    #         # Test mixed parameter types in executemany
    #         mixed_parameters = [
    #             (1,),
    #             ("string",),
    #             (3.14,),
    #             (None,),
    #         ]

    #         cursor.executemany("SELECT ? FROM sysibm.sysdummy1", mixed_parameters)

    # def test_parameter_parser_edge_cases(self, ibmi_credentials):
    #     """Test parameter parser with edge cases and potential error conditions."""
    #     with connect(ibmi_credentials) as conn:
    #         cursor = conn.cursor()

    #         # Test empty tuple (should work for queries without parameters)
    #         cursor.execute("SELECT 1 FROM sysibm.sysdummy1", ())
    #         result = cursor.fetchone()
    #         assert result[0] == 1, "Empty tuple parameter failed"

    #         # Test empty list
    #         cursor.execute("SELECT 1 FROM sysibm.sysdummy1", [])
    #         result = cursor.fetchone()
    #         assert result[0] == 1, "Empty list parameter failed"

    # def test_parameter_parser_sql_injection_prevention(self, ibmi_credentials):
    #     """Test that parameter parser properly prevents SQL injection."""
    #     with connect(ibmi_credentials) as conn:
    #         cursor = conn.cursor()

    #         # These should be treated as literal parameter values, not SQL
    #         malicious_inputs = [
    #             "'; DROP TABLE test; --",
    #             "1' OR '1'='1",
    #             "UNION SELECT * FROM another_table",
    #             "'; SELECT 1; --",
    #         ]

    #         for malicious_input in malicious_inputs:
    #             # Using WHERE clause that won't match, ensuring parameterization works
    #             cursor.execute(
    #                 "SELECT 1 FROM sysibm.sysdummy1 WHERE 'never_match' = ?", (malicious_input,)
    #             )
    #             result = cursor.fetchone()
    #             # Should return None because 'never_match' != malicious_input
    #             assert result is None, f"SQL injection not prevented for: {malicious_input}"

    # def test_parameter_parser_special_characters(self, ibmi_credentials):
    #     """Test parameter parser with special characters that might cause issues."""
    #     with connect(ibmi_credentials) as conn:
    #         cursor = conn.cursor()

    #         special_chars = [
    #             "'single_quote'",
    #             '"double_quote"',
    #             "back`tick",
    #             "percent%",
    #             "underscore_",
    #             "semicolon;",
    #             "question?mark",
    #             "asterisk*",
    #             "plus+sign",
    #             "equals=sign",
    #             "parentheses()",
    #             "brackets[]",
    #             "braces{}",
    #             "pipe|",
    #             "backslash\\",
    #             "forward/slash",
    #         ]

    #         for special_char in special_chars:
    #             cursor.execute("SELECT ? FROM sysibm.sysdummy1", (special_char,))
    #             result = cursor.fetchone()
    #             assert result is not None, f"Failed for special character: {special_char}"
    #             assert result[0] == special_char, f"Special character corrupted: {special_char}"

    # def test_parameter_parser_large_values(self, ibmi_credentials):
    #     """Test parameter parser with large values."""
    #     with connect(ibmi_credentials) as conn:
    #         cursor = conn.cursor()

    #         # Large string
    #         large_string = "x" * 1000
    #         cursor.execute("SELECT ? FROM sysibm.sysdummy1", (large_string,))
    #         result = cursor.fetchone()
    #         assert result[0] == large_string, "Large string parameter failed"

    #         # Large number (within IBM i limits)
    #         large_number = 999999999
    #         cursor.execute("SELECT ? FROM sysibm.sysdummy1", (large_number,))
    #         result = cursor.fetchone()
    #         assert result[0] == large_number, "Large number parameter failed"

    # def test_parameter_parser_error_conditions(self, ibmi_credentials):
    #     """Test parameter parser error handling."""
    #     with connect(ibmi_credentials) as conn:
    #         cursor = conn.cursor()

    #         # Test parameter count mismatch - too few parameters
    #         with pytest.raises((DatabaseError, DataError, ValueError)):
    #             cursor.execute("SELECT ? FROM sysibm.sysdummy1 WHERE ? = ?", (1,))

    #         # Test parameter count mismatch - too many parameters
    #         with pytest.raises((DatabaseError, DataError, ValueError)):
    #             cursor.execute("SELECT ? FROM sysibm.sysdummy1", (1, 2, 3))

    # def test_parameter_validation_function(self):
    #     """Test the standalone parameter validation function."""
    #     # Test matching parameter counts
    #     ParameterParser.validate_parameter_count("SELECT ? FROM table WHERE ? = ?", [1, 2, 3])

    #     # Test mismatched parameter counts
    #     with pytest.raises(ValueError, match="Parameter count mismatch"):
    #         ParameterParser.validate_parameter_count("SELECT ? FROM table", [1, 2])

    #     # Test no parameters needed
    #     ParameterParser.validate_parameter_count("SELECT 1 FROM table", [])

    #     # Test None parameters
    #     ParameterParser.validate_parameter_count("SELECT 1 FROM table", None)
