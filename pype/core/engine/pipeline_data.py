import threading
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
    Thread-safe unified data abstraction for passing data between pipeline components.
    
    Supports pandas DataFrames, Dask DataFrames, scalar values, and collections
    with lazy conversion between formats and metadata tracking. Thread-safe for
    concurrent access in Dask and ThreadPool executors.
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
            ValueError: If data type is not supported or data is None
            TypeError: If data type cannot be determined
        """
        if data is None:
            raise ValueError("Data cannot be None")
            
        self._data = data
        self._data_type = data_type or self._detect_data_type(data)
        self.schema = schema or self._extract_schema(data)
        self.source = source
        self.notes = notes
        
        # Thread-safe caching with single lock per instance
        self._lock = threading.RLock()  # Allow recursive locking
        self._pandas_cache: Optional[pd.DataFrame] = None
        self._dask_cache: Optional[dd.DataFrame] = None
        self._cached_row_count: Optional[int] = None
        self._row_count_estimated: bool = False
        
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
        Get the number of rows if applicable (thread-safe).
        
        For Dask DataFrames, returns estimated count to avoid expensive computation.
        
        Returns:
            Number of rows for DataFrames, None for scalar/collection data
        """
        if self._data_type not in (DataType.PANDAS, DataType.DASK):
            return None
            
        with self._lock:
            # Return cached count if available
            if self._cached_row_count is not None:
                return self._cached_row_count
                
            try:
                if self._data_type == DataType.PANDAS:
                    self._cached_row_count = len(self._data)
                    self._row_count_estimated = False
                elif self._data_type == DataType.DASK:
                    # Use npartitions estimate instead of expensive len() computation
                    # This avoids triggering computation of entire dataset
                    try:
                        # Try to get partition info for estimate
                        npartitions = getattr(self._data, 'npartitions', 1)
                        # Rough estimate: assume even distribution across partitions
                        if hasattr(self._data, 'map_partitions'):
                            # This is much faster than len() but still an estimate
                            sample_partition = self._data.get_partition(0)
                            if hasattr(sample_partition, 'compute'):
                                sample_size = len(sample_partition.compute())
                                self._cached_row_count = sample_size * npartitions
                                self._row_count_estimated = True
                            else:
                                self._cached_row_count = None
                        else:
                            self._cached_row_count = None
                    except Exception:
                        # If estimation fails, don't compute - return None
                        self._cached_row_count = None
                    
                return self._cached_row_count
            except Exception:
                # Some operations don't support length computation
                return None
    
    @property
    def is_row_count_estimated(self) -> bool:
        """
        Check if row count is estimated (for Dask) or exact (for pandas).
        
        Returns:
            True if row count is estimated, False if exact
        """
        return self._row_count_estimated
    
    def _detect_data_type(self, data: Any) -> DataType:
        """
        Automatically detect the data type from the input.
        
        Raises:
            TypeError: If data type cannot be determined or is unsupported
        """
        if isinstance(data, pd.DataFrame):
            return DataType.PANDAS
        elif isinstance(data, dd.DataFrame):
            return DataType.DASK
        elif isinstance(data, (list, dict, tuple)):
            return DataType.COLLECTION
        elif isinstance(data, (str, int, float, bool)) or data is None:
            return DataType.SCALAR
        else:
            # Don't silently default - raise error for unknown types
            raise TypeError(f"Unsupported data type: {type(data)}. "
                          f"Supported types: pandas.DataFrame, dask.DataFrame, "
                          f"list, dict, tuple, str, int, float, bool")
    
    def _extract_schema(self, data: Any) -> Dict[str, str]:
        """
        Extract column schema from DataFrame data safely.
        
        Returns:
            Dictionary mapping column names to string representations of dtypes
        """
        try:
            if isinstance(data, (pd.DataFrame, dd.DataFrame)):
                return {col: str(dtype) for col, dtype in data.dtypes.items()}
        except Exception:
            # If schema extraction fails, return empty dict rather than crash
            pass
        return {}
    
    def to_pandas(self) -> pd.DataFrame:
        """
        Convert data to pandas DataFrame with thread-safe lazy conversion.
        
        Returns:
            pandas DataFrame (copy to prevent mutations)
            
        Raises:
            ValueError: If data cannot be converted to DataFrame
        """
        with self._lock:
            if self._pandas_cache is not None:
                # Return copy to prevent mutations affecting cached data
                return self._pandas_cache.copy()
            
            if self._data_type == DataType.PANDAS:
                # Store copy in cache to prevent mutations to original
                self._pandas_cache = self._data.copy()
            elif self._data_type == DataType.DASK:
                # Convert Dask to pandas and cache result
                self._pandas_cache = self._data.compute()
            else:
                raise ValueError(f"Cannot convert {self._data_type.value} data to pandas DataFrame")
            
            return self._pandas_cache.copy()
    
    def to_dask(self) -> dd.DataFrame:
        """
        Convert data to Dask DataFrame with thread-safe lazy conversion.
        
        Returns:
            Dask DataFrame
            
        Raises:
            ValueError: If data cannot be converted to DataFrame
        """
        with self._lock:
            if self._dask_cache is not None:
                return self._dask_cache
            
            if self._data_type == DataType.DASK:
                # For Dask data, cache reference (Dask DataFrames are immutable)
                self._dask_cache = self._data
            elif self._data_type == DataType.PANDAS:
                # Convert pandas to Dask and cache result
                self._dask_cache = dd.from_pandas(self._data, npartitions=1)
            else:
                raise ValueError(f"Cannot convert {self._data_type.value} data to Dask DataFrame")
            
            return self._dask_cache
    
    def get_raw_data(self) -> Any:
        """
        Get the raw underlying data without conversion.
        
        For pandas DataFrames, returns a copy to prevent mutations.
        
        Returns:
            The original data object (copy for pandas DataFrames)
        """
        if self._data_type == DataType.PANDAS:
            return self._data.copy()
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
            "row_count_estimated": self.is_row_count_estimated,
            "source": self.source,
            "notes": self.notes
        }
    
    def clear_caches(self) -> None:
        """
        Clear conversion caches to free memory.
        
        Useful for memory management in long-running ETL jobs.
        Original data is preserved.
        """
        with self._lock:
            self._pandas_cache = None
            self._dask_cache = None
            self._cached_row_count = None
            self._row_count_estimated = False
    
    def clone_with_data(self, new_data: Any, **metadata_updates) -> 'PipelineData':
        """
        Create a new PipelineData instance with different data but same metadata.
        
        Schema is auto-detected from new data unless explicitly provided.
        
        Args:
            new_data: New data to wrap
            **metadata_updates: Optional metadata field updates
            
        Returns:
            New PipelineData instance with auto-detected schema for new data
        """
        # Auto-detect schema for new data unless explicitly provided
        new_schema = metadata_updates.get('schema')
        if new_schema is None:
            new_schema = self._extract_schema(new_data)
        
        return PipelineData(
            data=new_data,
            schema=new_schema,
            source=metadata_updates.get('source', self.source),
            notes=metadata_updates.get('notes', self.notes)
        )
    
    def __repr__(self) -> str:
        """Return detailed string representation."""
        schema_summary = f"{len(self.schema)} columns" if self.schema else "no schema"
        
        row_count = self.row_count
        if row_count is not None:
            estimate_marker = "~" if self.is_row_count_estimated else ""
            row_info = f", {estimate_marker}{row_count} rows"
        else:
            row_info = ""
            
        source_info = f" from {self.source}" if self.source else ""
        
        return (f"PipelineData(type={self._data_type.value}, "
                f"{schema_summary}{row_info}{source_info})")
    
    def __str__(self) -> str:
        """Return concise string representation."""
        if self._data_type in (DataType.PANDAS, DataType.DASK):
            row_count = self.row_count
            if row_count is not None:
                estimate_marker = "~" if self.is_row_count_estimated else ""
                return f"{self._data_type.value.title()} DataFrame: {estimate_marker}{row_count} rows, {len(self.schema)} columns"
            else:
                return f"{self._data_type.value.title()} DataFrame: unknown rows, {len(self.schema)} columns"
        else:
            return f"{self._data_type.value.title()} data: {type(self._data).__name__}"