"""Unit tests for LOB (CLOB/BLOB) support.

Covers:
  - ClobValue / BlobValue: construction, read() semantics, NULL handling
  - _wrap_lob: type dispatch, case-insensitivity, NULL/unknown-type passthrough
  - row_to_tuple: LOB wrapping for both dict rows (normal) and list/tuple
    rows (terse mode)
"""
from mapepire_python.core.utils import ColumnMetaData, MetaData, _wrap_lob, row_to_tuple
from mapepire_python.lob import BLOB_TYPES, CLOB_TYPES, LOB_TYPES, BlobValue, ClobValue

# ---------------------------------------------------------------------------
# ClobValue
# ---------------------------------------------------------------------------

class TestClobValue:
    def test_wraps_string(self):
        clob = ClobValue("hello world")
        assert clob.value == "hello world"

    def test_none_becomes_empty_string(self):
        clob = ClobValue(None)
        assert clob.value == ""

    def test_read_all_returns_full_value(self):
        clob = ClobValue("hello world")
        assert clob.read() == "hello world"

    def test_read_exhausts_after_full_read(self):
        clob = ClobValue("hello world")
        clob.read()
        assert clob.read() == ""

    def test_read_chunked(self):
        clob = ClobValue("hello world")
        assert clob.read(5) == "hello"
        assert clob.read(1) == " "
        assert clob.read() == "world"

    def test_repr_does_not_raise(self):
        assert "ClobValue" in repr(ClobValue("x" * 100))


# ---------------------------------------------------------------------------
# BlobValue
# ---------------------------------------------------------------------------

class TestBlobValue:
    def test_wraps_hex_string(self):
        blob = BlobValue("68656c6c6f")
        assert blob.value == b"hello"

    def test_wraps_raw_bytes_unchanged(self):
        blob = BlobValue(b"\x01\x02\x03")
        assert blob.value == b"\x01\x02\x03"

    def test_none_becomes_empty_bytes(self):
        blob = BlobValue(None)
        assert blob.value == b""

    def test_read_all_returns_full_value(self):
        blob = BlobValue(b"\x01\x02\x03")
        assert blob.read() == b"\x01\x02\x03"

    def test_read_exhausts_after_full_read(self):
        blob = BlobValue(b"\x01\x02\x03")
        blob.read()
        assert blob.read() == b""

    def test_read_chunked(self):
        blob = BlobValue(b"\x01\x02\x03\x04")
        assert blob.read(2) == b"\x01\x02"
        assert blob.read() == b"\x03\x04"

    def test_repr_does_not_raise(self):
        assert "BlobValue" in repr(BlobValue(b"\x00" * 30))


# ---------------------------------------------------------------------------
# LOB type sets
# ---------------------------------------------------------------------------

class TestLobTypeSets:
    def test_clob_types_contents(self):
        assert CLOB_TYPES == {"CLOB", "NCLOB", "DBCLOB"}

    def test_blob_types_contents(self):
        assert BLOB_TYPES == {"BLOB"}

    def test_lob_types_is_union(self):
        assert LOB_TYPES == CLOB_TYPES | BLOB_TYPES


# ---------------------------------------------------------------------------
# _wrap_lob
# ---------------------------------------------------------------------------

class TestWrapLob:
    def test_clob_type_wraps_clob_value(self):
        result = _wrap_lob("some text", "CLOB")
        assert isinstance(result, ClobValue)
        assert result.value == "some text"

    def test_blob_type_wraps_blob_value(self):
        result = _wrap_lob("68656c6c6f", "BLOB")
        assert isinstance(result, BlobValue)
        assert result.value == b"hello"

    def test_case_insensitive_type_matching(self):
        result = _wrap_lob("some text", "clob")
        assert isinstance(result, ClobValue)

    def test_non_lob_type_passthrough(self):
        assert _wrap_lob("000010", "CHAR") == "000010"
        assert _wrap_lob(52750.00, "DECIMAL") == 52750.00

    def test_none_value_passthrough_even_for_lob_type(self):
        assert _wrap_lob(None, "CLOB") is None
        assert _wrap_lob(None, "BLOB") is None

    def test_none_sql_type_passthrough(self):
        assert _wrap_lob("some text", None) == "some text"

    def test_nclob_and_dbclob_wrap(self):
        assert isinstance(_wrap_lob("x", "NCLOB"), ClobValue)
        assert isinstance(_wrap_lob("x", "DBCLOB"), ClobValue)


# ---------------------------------------------------------------------------
# row_to_tuple LOB integration
# ---------------------------------------------------------------------------

_LOB_COLUMNS = [
    ColumnMetaData(name="ID", type="INTEGER", display_size=10, label="ID"),
    ColumnMetaData(name="NOTES", type="CLOB", display_size=100, label="NOTES"),
    ColumnMetaData(name="PHOTO", type="BLOB", display_size=100, label="PHOTO"),
]
_LOB_METADATA = MetaData(column_count=3, job="TEST/QUSER/JOB001", columns=_LOB_COLUMNS)


class TestRowToTupleLobWrapping:
    def test_dict_row_wraps_lob_columns(self):
        row = {"ID": 1, "NOTES": "hello world", "PHOTO": "68656c6c6f"}
        result = row_to_tuple(row, _LOB_METADATA)
        assert result[0] == 1
        assert isinstance(result[1], ClobValue)
        assert result[1].value == "hello world"
        assert isinstance(result[2], BlobValue)
        assert result[2].value == b"hello"

    def test_dict_row_null_lob_stays_none(self):
        row = {"ID": 1, "NOTES": None, "PHOTO": None}
        result = row_to_tuple(row, _LOB_METADATA)
        assert result[1] is None
        assert result[2] is None

    def test_terse_list_row_wraps_lob_columns(self):
        """Regression test: terse-mode rows arrive as lists, not dicts, but
        must still be wrapped in ClobValue/BlobValue using positional
        column metadata.
        """
        row = [1, "hello world", "68656c6c6f"]
        result = row_to_tuple(row, _LOB_METADATA)
        assert result[0] == 1
        assert isinstance(result[1], ClobValue)
        assert result[1].value == "hello world"
        assert isinstance(result[2], BlobValue)
        assert result[2].value == b"hello"

    def test_terse_tuple_row_wraps_lob_columns(self):
        row = (1, "hello world", "68656c6c6f")
        result = row_to_tuple(row, _LOB_METADATA)
        assert isinstance(result[1], ClobValue)
        assert isinstance(result[2], BlobValue)

    def test_list_row_without_metadata_passthrough(self):
        row = [1, "hello world", "68656c6c6f"]
        result = row_to_tuple(row, None)
        assert result == (1, "hello world", "68656c6c6f")
