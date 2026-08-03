"""LOB (Large Object) value wrappers for CLOB, NCLOB, DBCLOB, and BLOB columns.

The Mapepire server inlines LOB data directly in row payloads, so these
classes wrap the already-loaded value behind a file-like read() interface
as recommended by DB-API 2.0 (PEP 249).
"""

from typing import Optional, Union

__all__ = ["ClobValue", "BlobValue", "LOBValue", "LOB_TYPES", "BLOB_TYPES", "CLOB_TYPES"]

# SQL type names that map to character LOBs (str content)
CLOB_TYPES = frozenset({"CLOB", "NCLOB", "DBCLOB"})

# SQL type names that map to binary LOBs (bytes content)
BLOB_TYPES = frozenset({"BLOB"})

# Union of all LOB type names — used for fast membership checks
LOB_TYPES = CLOB_TYPES | BLOB_TYPES


class ClobValue:
    """Wraps an inlined CLOB/NCLOB/DBCLOB string value.

    Provides a file-like read() interface so callers can consume the
    content incrementally or all at once, matching DB-API 2.0 expectations.
    """

    def __init__(self, value: Optional[str]) -> None:
        self._value: str = value if value is not None else ""
        self._pos: int = 0

    @property
    def value(self) -> str:
        """The full underlying string, regardless of read position."""
        return self._value

    def read(self, size: int = -1) -> str:
        """Read up to *size* characters, advancing the position.

        If *size* is -1 (the default), return all remaining content.
        Returns an empty string when the end has been reached.
        """
        if size == -1:
            chunk = self._value[self._pos:]
            self._pos = len(self._value)
        else:
            chunk = self._value[self._pos: self._pos + size]
            self._pos += len(chunk)
        return chunk

    def __repr__(self) -> str:
        preview = self._value[:40] + "..." if len(self._value) > 40 else self._value
        return f"ClobValue({preview!r})"


class BlobValue:
    """Wraps an inlined BLOB bytes value.

    Provides a file-like read() interface so callers can consume the
    content incrementally or all at once, matching DB-API 2.0 expectations.
    """

    def __init__(self, value: Optional[Union[str, bytes]]) -> None:
        # The server may deliver BLOB data as a hex string or raw bytes.
        if isinstance(value, str):
            self._value = bytes.fromhex(value)
        elif isinstance(value, bytes):
            self._value = value
        else:
            self._value = b""
        self._pos: int = 0

    @property
    def value(self) -> bytes:
        """The full underlying bytes, regardless of read position."""
        return self._value

    def read(self, size: int = -1) -> bytes:
        """Read up to *size* bytes, advancing the position.

        If *size* is -1 (the default), return all remaining content.
        Returns an empty bytes object when the end has been reached.
        """
        if size == -1:
            chunk = self._value[self._pos:]
            self._pos = len(self._value)
        else:
            chunk = self._value[self._pos: self._pos + size]
            self._pos += len(chunk)
        return chunk

    def __repr__(self) -> str:
        preview = self._value[:20]
        return f"BlobValue({preview!r}{'...' if len(self._value) > 20 else ''})"


# Convenience union type alias for type annotations
LOBValue = Union[ClobValue, BlobValue]
