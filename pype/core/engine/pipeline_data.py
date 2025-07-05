from enum import Enum
from typing import Any, Dict, Optional, Union
import pandas as pd
import dask.dataframe as dd


class DataType(Enum):
    """Enumeration of supported data types in the pipeline."""
    PANDAS = "pandas"
    DASK = "dask"
    SCALAR = "scalar"
    COLLECTION = "collection"


class PipelineData:
    """
    Unified data abstraction for passing data between pipeline components.
    
    Supports pandas DataFrames, Dask DataFrames, scalar values, and collections
    with lazy conversion between formats and metadata tracking.
    """
    
    def __init__(
        self,
        data: Union[pd.DataFrame, dd.DataFrame, Any],
        data_type: Optional[DataType] = None,
        schema: Optional[Dict[str, str]] = None,
        source: Optional[str] = None,
        notes: Optional[str] = None
    ):
        """
        Initialize PipelineData with data and metadata.
        
        Args:
            data: The actual data (DataFrame, scalar, or collection)
            data_type: Explicit data type (auto-detected if None)
            schema: Dictionary mapping column names to dtypes
            source: Identifier for data provenance
            notes: Optional transformation metadata
            
        Raises:
            ValueError: If data type is not supported
        """
        self._data = data
        self._data_type = data_type or self._detect_data_type(data)
        self.schema = schema or self._extract_schema(data)
        self.source = source
        self.notes = notes
        
        # Cache for converted data to enable lazy conversion
        self._pandas_cache: Optional[pd.DataFrame] = None
        self._dask_cache: Optional[dd.DataFrame] = None
        
        # Validate supported data type
        if self._data_type not in DataType:
            raise ValueError(f"Unsupported data type: {type(data)}")
    
    @property
    def data_type(self) -> DataType:
        """Get the current data type."""
        return self._data_type
    
    @property
    def row_count(self) -> Optional[int]:
        """
        Get the number of rows if applicable.
        
        Returns:
            Number of rows for DataFrames, None for scalar/collection data
        """
        if self._data_type == DataType.PANDAS:
            return len(self._data)
        elif self._data_type == DataType.DASK:
            # Dask length computation can be expensive, cache it
            if hasattr(self, '_cached_row_count'):
                return self._cached_row_count
            try:
                self._cached_row_count = len(self._data)
                return self._cached_row_count
            except Exception:
                # Some Dask operations don't support len()
                return None
        else:
            return None
    
    def _detect_data_type(self, data: Any) -> DataType:
        """Automatically detect the data type from the input."""
        if isinstance(data, pd.DataFrame):
            return DataType.PANDAS
        elif isinstance(data, dd.DataFrame):
            return DataType.DASK
        elif isinstance(data, (list, dict, tuple)):
            return DataType.COLLECTION
        else:
            return DataType.SCALAR
    
    def _extract_schema(self, data: Any) -> Dict[str, str]:
        """Extract column schema from DataFrame data."""
        if isinstance(data, (pd.DataFrame, dd.DataFrame)):
            return {col: str(dtype) for col, dtype in data.dtypes.items()}
        return {}
    
    def to_pandas(self) -> pd.DataFrame:
        """
        Convert data to pandas DataFrame with lazy conversion.
        
        Returns:
            pandas DataFrame
            
        Raises:
            ValueError: If data cannot be converted to DataFrame
        """
        if self._pandas_cache is not None:
            return self._pandas_cache
        
        if self._data_type == DataType.PANDAS:
            self._pandas_cache = self._data
        elif self._data_type == DataType.DASK:
            self._pandas_cache = self._data.compute()
        else:
            raise ValueError(f"Cannot convert {self._data_type.value} data to pandas DataFrame")
        
        return self._pandas_cache
    
    def to_dask(self) -> dd.DataFrame:
        """
        Convert data to Dask DataFrame with lazy conversion.
        
        Returns:
            Dask DataFrame
            
        Raises:
            ValueError: If data cannot be converted to DataFrame
        """
        if self._dask_cache is not None:
            return self._dask_cache
        
        if self._data_type == DataType.DASK:
            self._dask_cache = self._data
        elif self._data_type == DataType.PANDAS:
            self._dask_cache = dd.from_pandas(self._data, npartitions=1)
        else:
            raise ValueError(f"Cannot convert {self._data_type.value} data to Dask DataFrame")
        
        return self._dask_cache
    
    def get_raw_data(self) -> Any:
        """
        Get the raw underlying data without conversion.
        
        Returns:
            The original data object
        """
        return self._data
    
    def get_metadata(self) -> Dict[str, Any]:
        """
        Get all metadata as a dictionary.
        
        Returns:
            Dictionary containing all metadata fields
        """
        return {
            "data_type": self._data_type.value,
            "schema": self.schema,
            "row_count": self.row_count,
            "source": self.source,
            "notes": self.notes
        }
    
    def clone_with_data(self, new_data: Any, **metadata_updates) -> 'PipelineData':
        """
        Create a new PipelineData instance with different data but same metadata.
        
        Args:
            new_data: New data to wrap
            **metadata_updates: Optional metadata field updates
            
        Returns:
            New PipelineData instance
        """
        return PipelineData(
            data=new_data,
            schema=metadata_updates.get('schema', self.schema),
            source=metadata_updates.get('source', self.source),
            notes=metadata_updates.get('notes', self.notes)
        )
    
    def __repr__(self) -> str:
        """Return detailed string representation."""
        schema_summary = f"{len(self.schema)} columns" if self.schema else "no schema"
        row_info = f", {self.row_count} rows" if self.row_count is not None else ""
        source_info = f" from {self.source}" if self.source else ""
        
        return (f"PipelineData(type={self._data_type.value}, "
                f"{schema_summary}{row_info}{source_info})")
    
    def __str__(self) -> str:
        """Return concise string representation."""
        if self._data_type in (DataType.PANDAS, DataType.DASK):
            return f"{self._data_type.value.title()} DataFrame: {self.row_count} rows, {len(self.schema)} columns"
        else:
            return f"{self._data_type.value.title()} data: {type(self._data).__name__}"