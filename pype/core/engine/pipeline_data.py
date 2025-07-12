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
    with lazy conversion between formats and metadata tracking. Optimized for
    memory efficiency and performance in two-level execution architecture.
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
            ValueError: If data is None
            TypeError: If data type is not supported
        """
        if data is None:
            raise ValueError("Data cannot be None")
            
        self._data = data
        self._data_type = data_type or self._detect_data_type(data)
        self.schema = schema or self._extract_schema(data)
        self.source = source
        self.notes = notes
        
        # Thread-safe lazy conversion (no upfront caching)
        self._lock = threading.RLock()
        self._pandas_cache: Optional[pd.DataFrame] = None
        self._dask_cache: Optional[dd.DataFrame] = None
        self._cached_row_count: Optional[int] = None
        self._row_count_estimated: bool = False
    
    @property
    def data_type(self) -> DataType:
        """Get the current data type."""
        return self._data_type
    
    @property
    def row_count(self) -> Optional[int]:
        """
        Get the number of rows if applicable (thread-safe, no expensive computation).
        
        For Dask DataFrames, uses metadata-based estimation without computation.
        
        Returns:
            Number of rows for DataFrames, None for scalar/collection data
        """
        if self._data_type not in (DataType.PANDAS, DataType.DASK):
            return None
            
        with self._lock:
            if self._cached_row_count is not None:
                return self._cached_row_count
                
            try:
                if self._data_type == DataType.PANDAS:
                    self._cached_row_count = len(self._data)
                    self._row_count_estimated = False
                elif self._data_type == DataType.DASK:
                    # Use Dask metadata for fast estimation (no computation)
                    try:
                        # Method 1: Use len() when divisions are known (fast, no computation)
                        if hasattr(self._data, 'divisions') and self._data.divisions[0] is not None:
                            # When divisions are known, len() is fast and exact
                            self._cached_row_count = len(self._data)
                            self._row_count_estimated = False
                        else:
                            # Method 2: Use realistic ETL partition estimate
                            npartitions = getattr(self._data, 'npartitions', 1)
                            # Most ETL partitions have 50K-100K rows (more realistic than 10K)
                            estimated_rows_per_partition = 75000
                            self._cached_row_count = npartitions * estimated_rows_per_partition
                            self._row_count_estimated = True
                    except Exception:
                        # If all estimation fails, return None (no computation)
                        self._cached_row_count = None
                    
                return self._cached_row_count
            except Exception:
                return None
    
    @property
    def is_row_count_estimated(self) -> bool:
        """Check if row count is estimated (Dask) or exact (pandas)."""
        return self._row_count_estimated
    
    def _detect_data_type(self, data: Any) -> DataType:
        """
        Detect data type with explicit validation.
        
        Raises:
            TypeError: For unsupported data types
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
            raise TypeError(f"Unsupported data type: {type(data)}. "
                          f"Supported: pandas.DataFrame, dask.DataFrame, list, dict, tuple, primitives")
    
    def _extract_schema(self, data: Any) -> Dict[str, str]:
        """Extract column schema safely without triggering computation."""
        try:
            if isinstance(data, (pd.DataFrame, dd.DataFrame)):
                return {col: str(dtype) for col, dtype in data.dtypes.items()}
        except Exception:
            pass
        return {}
    
    def _calculate_optimal_partitions(self, df: pd.DataFrame) -> int:
        """Calculate optimal Dask partitions based on DataFrame size (performance optimized)."""
        try:
            # Use fast estimation for large DataFrames to avoid expensive memory_usage(deep=True)
            if len(df) > 100000:
                # Fast estimate: rows * columns * avg_bytes_per_cell
                # Use dtypes.value_counts() for faster column type analysis
                try:
                    dtype_counts = df.dtypes.value_counts()
                    numeric_cols = sum(dtype_counts.get(dt, 0) for dt in dtype_counts.index 
                                     if str(dt).startswith(('int', 'float', 'bool')))
                    object_cols = dtype_counts.get('object', 0)
                    other_cols = df.shape[1] - numeric_cols - object_cols
                    
                    memory_estimate = len(df) * (numeric_cols * 8 + object_cols * 50 + other_cols * 16)
                except (AttributeError, ValueError):
                    # Fallback to simple estimate if dtype analysis fails
                    memory_estimate = len(df) * df.shape[1] * 20  # 20 bytes avg per cell
            else:
                # For smaller DataFrames, precise calculation is acceptable
                memory_estimate = df.memory_usage(deep=True).sum()
            
            # Target ~100MB per partition for optimal parallelism
            target_partition_size = 100 * 1024 * 1024  # 100MB
            optimal_partitions = max(1, min(16, memory_estimate // target_partition_size))
            return int(optimal_partitions)
        except (MemoryError, AttributeError, ValueError) as e:
            # Fallback: Use row count based estimation for specific known errors
            try:
                rows = len(df)
                # Target ~500K rows per partition (reasonable for most ETL)
                return max(1, min(16, rows // 500000))
            except Exception:
                return 4  # Conservative default
    
    def to_pandas(self) -> pd.DataFrame:
        """
        Convert to pandas DataFrame with guaranteed immutability.
        
        Returns:
            pandas DataFrame (always a copy for data safety)
        """
        with self._lock:
            if self._data_type == DataType.PANDAS:
                # Direct copy from original (no intermediate cache needed)
                return self._data.copy()
            elif self._data_type == DataType.DASK:
                # Use cached conversion if available
                if self._pandas_cache is not None:
                    # CRITICAL: Always return copy to prevent mutation of cached data
                    return self._pandas_cache.copy()
                # Convert and cache
                self._pandas_cache = self._data.compute()
                # CRITICAL: Return copy to prevent mutation of cached data
                return self._pandas_cache.copy()
            else:
                raise ValueError(f"Cannot convert {self._data_type.value} to pandas DataFrame")
    
    def to_dask(self) -> dd.DataFrame:
        """
        Convert to Dask DataFrame with optimal partitioning.
        
        Returns:
            Dask DataFrame with optimal partitions
        """
        with self._lock:
            if self._data_type == DataType.DASK:
                # Return original (Dask DataFrames are immutable)
                return self._data
            elif self._data_type == DataType.PANDAS:
                # Check cached version
                if self._dask_cache is not None:
                    return self._dask_cache
                # Convert with optimal partitioning
                npartitions = self._calculate_optimal_partitions(self._data)
                self._dask_cache = dd.from_pandas(self._data, npartitions=npartitions)
                return self._dask_cache
            else:
                raise ValueError(f"Cannot convert {self._data_type.value} to Dask DataFrame")
    
    def get_raw_data(self) -> Any:
        """
        Get raw data without conversion.
        
        For pandas, returns copy to maintain immutability.
        For other types, returns original reference.
        """
        if self._data_type == DataType.PANDAS:
            return self._data.copy()
        return self._data
    
    def get_metadata(self) -> Dict[str, Any]:
        """Get comprehensive metadata dictionary."""
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
        
        Original data is preserved. Useful for memory management
        in long-running ETL jobs.
        """
        with self._lock:
            self._pandas_cache = None
            self._dask_cache = None
            self._cached_row_count = None
            self._row_count_estimated = False
    
    def clone_with_data(self, new_data: Any, **metadata_updates) -> 'PipelineData':
        """
        Create new instance with different data and auto-detected schema.
        
        Args:
            new_data: New data to wrap
            **metadata_updates: Optional metadata overrides
            
        Returns:
            New PipelineData with fresh schema detection
        """
        # Always auto-detect schema unless explicitly overridden
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
        """Detailed string representation."""
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
        """Concise string representation."""
        if self._data_type in (DataType.PANDAS, DataType.DASK):
            row_count = self.row_count
            if row_count is not None:
                estimate_marker = "~" if self.is_row_count_estimated else ""
                return f"{self._data_type.value.title()} DataFrame: {estimate_marker}{row_count} rows, {len(self.schema)} columns"
            else:
                return f"{self._data_type.value.title()} DataFrame: unknown rows, {len(self.schema)} columns"
        else:
            return f"{self._data_type.value.title()} data: {type(self._data).__name__}"