"""
Metadata processor for handling column descriptions and query metadata.

This module provides utilities for processing query metadata and converting it
to PEP 249 compatible formats, particularly for cursor.description handling.
"""

from typing import Any, Dict, List, Optional, Sequence, Tuple
from .query_base import QueryResult

# PEP 249 ColumnDescription type
ColumnDescription = Tuple[str, Any, Optional[int], Optional[int], Optional[int], Optional[int], Optional[bool]]


class MetadataProcessor:
    """
    Process query metadata for PEP 249 compatibility.
    
    This class handles the conversion between IBM i query metadata and the
    standardized column description format required by PEP 249.
    """

    # IBM i to Python type mapping
    TYPE_MAPPING = {
        'CHAR': str,
        'VARCHAR': str,
        'CLOB': str,
        'GRAPHIC': str,
        'VARGRAPHIC': str,
        'DBCLOB': str,
        'BINARY': bytes,
        'VARBINARY': bytes,
        'BLOB': bytes,
        'SMALLINT': int,
        'INTEGER': int,
        'BIGINT': int,
        'DECIMAL': float,
        'NUMERIC': float,
        'REAL': float,
        'DOUBLE': float,
        'FLOAT': float,
        'DATE': str,  # Will be converted to datetime objects
        'TIME': str,  # Will be converted to time objects
        'TIMESTAMP': str,  # Will be converted to datetime objects
        'XML': str,
        'BOOLEAN': bool,
    }

    @staticmethod
    def extract_column_descriptions(result: QueryResult) -> Optional[Sequence[ColumnDescription]]:
        """
        Extract column descriptions from query result.
        
        Args:
            result: QueryResult containing metadata
            
        Returns:
            Sequence of PEP 249 column descriptions or None if no metadata
        """
        if not result.metadata or "columns" not in result.metadata:
            # Fallback: create basic descriptions from data
            return MetadataProcessor._create_fallback_descriptions(result)
        
        columns = []
        for col_info in result.metadata["columns"]:
            col_desc = MetadataProcessor._create_column_description(col_info)
            columns.append(col_desc)
            
        return tuple(columns) if columns else None

    @staticmethod
    def _create_column_description(col_info: Dict[str, Any]) -> ColumnDescription:
        """
        Create a PEP 249 column description from IBM i column metadata.
        
        Args:
            col_info: Dictionary containing column metadata
            
        Returns:
            PEP 249 ColumnDescription tuple
        """
        name = col_info.get("name", "UNKNOWN")
        
        # Map IBM i type to Python type
        ibmi_type = col_info.get("type", "VARCHAR").upper()
        type_code = MetadataProcessor.TYPE_MAPPING.get(ibmi_type, str)
        
        # Extract size information
        display_size = col_info.get("length")
        internal_size = col_info.get("length")
        precision = col_info.get("precision")
        scale = col_info.get("scale")
        
        # Handle nullable information
        null_ok = col_info.get("nullable", True)
        
        return (
            name,           # name
            type_code,      # type_code
            display_size,   # display_size
            internal_size,  # internal_size
            precision,      # precision
            scale,          # scale
            null_ok         # null_ok
        )

    @staticmethod
    def _create_fallback_descriptions(result: QueryResult) -> Optional[Sequence[ColumnDescription]]:
        """
        Create basic column descriptions when metadata is not available.
        
        Args:
            result: QueryResult to analyze
            
        Returns:
            Basic column descriptions based on data or None
        """
        if not result.data or len(result.data) == 0:
            return None
            
        # Use first row to determine column names and basic types
        first_row = result.data[0]
        columns = []
        
        for col_name, col_value in first_row.items():
            # Infer Python type from value
            if col_value is None:
                type_code = str  # Default to string for None values
            elif isinstance(col_value, bool):
                type_code = bool
            elif isinstance(col_value, int):
                type_code = int
            elif isinstance(col_value, float):
                type_code = float
            elif isinstance(col_value, bytes):
                type_code = bytes
            else:
                type_code = str
                
            col_desc = (
                col_name,   # name
                type_code,  # type_code
                None,       # display_size (unknown)
                None,       # internal_size (unknown)
                None,       # precision (unknown)
                None,       # scale (unknown)
                True        # null_ok (assume nullable)
            )
            columns.append(col_desc)
            
        return tuple(columns) if columns else None

    @staticmethod
    def get_column_names(result: QueryResult) -> List[str]:
        """
        Get list of column names from query result.
        
        Args:
            result: QueryResult to extract names from
            
        Returns:
            List of column names
        """
        descriptions = MetadataProcessor.extract_column_descriptions(result)
        if descriptions:
            return [desc[0] for desc in descriptions]
        
        # Fallback to data analysis
        if result.data and len(result.data) > 0:
            return list(result.data[0].keys())
            
        return []

    @staticmethod
    def get_column_types(result: QueryResult) -> List[Any]:
        """
        Get list of column types from query result.
        
        Args:
            result: QueryResult to extract types from
            
        Returns:
            List of Python types for each column
        """
        descriptions = MetadataProcessor.extract_column_descriptions(result)
        if descriptions:
            return [desc[1] for desc in descriptions]
            
        # Fallback to data analysis
        if result.data and len(result.data) > 0:
            first_row = result.data[0]
            return [type(value) if value is not None else str for value in first_row.values()]
            
        return []

    @staticmethod
    def has_metadata(result: QueryResult) -> bool:
        """
        Check if the result contains proper metadata.
        
        Args:
            result: QueryResult to check
            
        Returns:
            True if metadata is available, False otherwise
        """
        return (result.metadata is not None and 
                "columns" in result.metadata and 
                len(result.metadata["columns"]) > 0)

    @staticmethod
    def get_table_info(result: QueryResult) -> Dict[str, Any]:
        """
        Extract table information from metadata if available.
        
        Args:
            result: QueryResult to analyze
            
        Returns:
            Dictionary with table information
        """
        table_info = {
            "table_name": None,
            "schema_name": None,
            "catalog_name": None
        }
        
        if result.metadata and "table_info" in result.metadata:
            table_info.update(result.metadata["table_info"])
            
        return table_info

    @staticmethod
    def format_metadata_for_debug(result: QueryResult) -> str:
        """
        Format metadata information for debugging purposes.
        
        Args:
            result: QueryResult to format
            
        Returns:
            Formatted string with metadata information
        """
        if not MetadataProcessor.has_metadata(result):
            return "No metadata available"
            
        descriptions = MetadataProcessor.extract_column_descriptions(result)
        if not descriptions:
            return "No column descriptions available"
            
        lines = ["Column Descriptions:"]
        for i, desc in enumerate(descriptions):
            name, type_code, display_size, internal_size, precision, scale, null_ok = desc
            lines.append(f"  {i+1}. {name}: {type_code.__name__} "
                        f"(size={display_size}, precision={precision}, scale={scale}, nullable={null_ok})")
            
        return "\n".join(lines)