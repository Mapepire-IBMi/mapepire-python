"""Unit tests for core/exceptions.py error classification.

Covers ``_parse_runtime_error`` and the ``convert_runtime_errors`` decorator
that map a raw ``RuntimeError`` (raised by the query layer) onto the
appropriate PEP 249 exception type.  No network or server is involved.
"""
import json

import pytest
from pep249 import DatabaseError, Error, ProgrammingError

from mapepire_python.core.exceptions import (
    CONNECTION_CLOSED,
    _parse_runtime_error,
    convert_runtime_errors,
)


# ---------------------------------------------------------------------------
# _parse_runtime_error
# ---------------------------------------------------------------------------

class TestParseRuntimeError:
    def test_already_a_pep249_error_is_returned_unchanged(self):
        original = ProgrammingError("boom")
        assert _parse_runtime_error(original) is original

    def test_non_runtime_error_is_returned_unchanged(self):
        original = ValueError("not a runtime error")
        assert _parse_runtime_error(original) is original

    def test_json_error_payload_maps_to_database_error_by_default(self):
        err = RuntimeError(json.dumps({"error": "something went wrong", "sql_rc": -443}))
        result = _parse_runtime_error(err)
        assert isinstance(result, DatabaseError)
        assert str(result) == "something went wrong"

    def test_programming_error_message_maps_to_programming_error(self):
        # "*FILE not found." is in PROGRAMMING_ERRORS
        err = RuntimeError(json.dumps({"error": "*FILE not found."}))
        result = _parse_runtime_error(err)
        assert isinstance(result, ProgrammingError)
        assert str(result) == "*FILE not found."

    def test_python_dict_repr_payload_is_parsed_via_literal_eval(self):
        # The query layer raises RuntimeError(dict); str() yields a repr,
        # not valid JSON, so it must fall back to ast.literal_eval.
        err = RuntimeError(repr({"error": "bad column", "sql_state": "42703"}))
        result = _parse_runtime_error(err)
        assert isinstance(result, DatabaseError)
        assert str(result) == "bad column"

    def test_plain_string_message_maps_to_database_error_with_full_text(self):
        err = RuntimeError("unstructured failure text")
        result = _parse_runtime_error(err)
        assert isinstance(result, DatabaseError)
        assert str(result) == "unstructured failure text"

    def test_dict_payload_without_error_key_falls_back_to_full_string(self):
        payload = json.dumps({"sql_state": "42000", "sql_rc": -104})
        err = RuntimeError(payload)
        result = _parse_runtime_error(err)
        assert isinstance(result, DatabaseError)
        # No "error" key -> KeyError -> returns the original string
        assert str(result) == payload

    def test_result_is_always_a_pep249_error(self):
        result = _parse_runtime_error(RuntimeError("x"))
        assert isinstance(result, Error)


# ---------------------------------------------------------------------------
# convert_runtime_errors decorator
# ---------------------------------------------------------------------------

class TestConvertRuntimeErrors:
    def test_passes_return_value_through(self):
        @convert_runtime_errors
        def f(a, b):
            return a + b

        assert f(2, 3) == 5

    def test_converts_runtime_error_to_pep249_error(self):
        @convert_runtime_errors
        def f():
            raise RuntimeError(json.dumps({"error": "*FILE not found."}))

        with pytest.raises(ProgrammingError, match=r"\*FILE not found\."):
            f()

    def test_chains_original_runtime_error_as_cause(self):
        @convert_runtime_errors
        def f():
            raise RuntimeError("kaboom")

        with pytest.raises(DatabaseError) as exc_info:
            f()
        assert isinstance(exc_info.value.__cause__, RuntimeError)

    def test_non_runtime_exceptions_propagate_unchanged(self):
        @convert_runtime_errors
        def f():
            raise KeyError("untouched")

        with pytest.raises(KeyError):
            f()

    def test_preserves_function_metadata(self):
        @convert_runtime_errors
        def documented():
            """my docstring"""

        assert documented.__name__ == "documented"
        assert documented.__doc__ == "my docstring"


# ---------------------------------------------------------------------------
# Module constants
# ---------------------------------------------------------------------------

class TestConnectionClosedSentinel:
    def test_connection_closed_is_a_programming_error(self):
        assert isinstance(CONNECTION_CLOSED, ProgrammingError)
        assert "closed connection" in str(CONNECTION_CLOSED)
