"""
PYpeline Artifact Manager - Lightweight data location registry
"""

import tempfile
import psutil
import logging
import shutil
import threading
import time
from pathlib import Path
from typing import Protocol, Union, Dict, Any, Optional
from collections import deque

try:
    import pandas as pd
    import dask.dataframe as dd
    import pyarrow.parquet as pq
except ImportError as e:
    raise ImportError(f"Missing required packages: {e}")

logger = logging.getLogger(__name__)


class DataArtifact(Protocol):
    """Universal interface for data location handles"""
    def to_pandas(self, **kwargs) -> pd.DataFrame: ...
    def to_dask(self, **kwargs) -> dd.DataFrame: ...
    @property
    def uri(self) -> str: ...
    @property
    def n_rows(self) -> int: ...


class MemoryArtifact:
    """Holds reference to in-memory data"""
    
    def __init__(self, data: Union[pd.DataFrame, dd.DataFrame], estimated_rows: Optional[int] = None):
        self._data = data
        self._uri = f"memory://{id(data)}"
        self._estimated_rows = estimated_rows
    
    def to_pandas(self, **kwargs) -> pd.DataFrame:
        data_type = type(self._data).__name__
        
        if hasattr(self._data, 'iloc'):  # pandas
            return self._data
        elif hasattr(self._data, 'compute'):  # dask or similar lazy frameworks
            return self._data.compute()
        else:
            raise NotImplementedError(f"to_pandas not implemented for {data_type}")
    
    def to_dask(self, **kwargs) -> dd.DataFrame:
        data_type = type(self._data).__name__
        
        if data_type in ("DataFrame", "Series") and hasattr(self._data, 'iloc'):  # pandas
            return dd.from_pandas(self._data, npartitions=1)
        elif hasattr(self._data, 'repartition') or 'dask' in str(type(self._data)):  # dask
            return self._data
        else:
            raise NotImplementedError(f"to_dask not implemented for {data_type}")
    
    @property
    def uri(self) -> str:
        return self._uri
    
    @property
    def n_rows(self) -> int:
        if self._estimated_rows is not None:
            return self._estimated_rows
        
        if hasattr(self._data, 'iloc'):  # pandas
            return len(self._data)
        elif hasattr(self._data, 'compute'):  # dask - expensive fallback
            return len(self._data.compute())
        else:
            return 0


class ParquetArtifact:
    """Points to parquet file/directory on disk"""
    
    def __init__(self, file_path: Path, n_rows: int, is_directory: bool = False):
        self.file_path = file_path
        self._n_rows = n_rows
        self.is_directory = is_directory
        
        if is_directory:
            self._uri = f"parquet_dir://{file_path}"
        else:
            self._uri = f"parquet://{file_path}"
    
    @classmethod
    def write(cls, data: Union[pd.DataFrame, dd.DataFrame], spill_dir: Path) -> "ParquetArtifact":
        try:
            if isinstance(data, pd.DataFrame):
                # Single file for pandas
                temp_file = tempfile.NamedTemporaryFile(
                    suffix=".parquet", dir=spill_dir, delete=False
                )
                file_path = Path(temp_file.name)
                temp_file.close()
                
                data.to_parquet(file_path, index=False)
                n_rows = len(data)
                is_directory = False
                
            else:  # dask
                # For dask, create a directory path that doesn't exist yet
                temp_dir_name = f"dask_{int(time.time() * 1000000)}_parquet"
                file_path = spill_dir / temp_dir_name
                
                # Dask will create the directory and write partition files
                data.to_parquet(str(file_path), write_index=False)
                
                # Estimate rows without full compute
                n_rows = max(1, int(data.size // len(data.columns))) if len(data.columns) > 0 else 0
                is_directory = True
            
            logger.info(f"Spilled ~{n_rows} rows to {file_path}")
            return cls(file_path, n_rows, is_directory)
            
        except Exception as e:
            logger.error(f"Failed to write parquet: {e}")
            raise
    
    def to_pandas(self, **kwargs) -> pd.DataFrame:
        try:
            return pd.read_parquet(self.file_path, **kwargs)
        except Exception as e:
            logger.error(f"Failed to read parquet as pandas: {e}")
            raise
    
    def to_dask(self, **kwargs) -> dd.DataFrame:
        try:
            return dd.read_parquet(self.file_path, **kwargs)
        except Exception as e:
            logger.error(f"Failed to read parquet as dask: {e}")
            raise
    
    @property
    def uri(self) -> str:
        return self._uri
    
    @property
    def n_rows(self) -> int:
        return self._n_rows
    
    def cleanup(self):
        try:
            if self.file_path.exists():
                if self.is_directory:
                    shutil.rmtree(self.file_path, ignore_errors=True)
                else:
                    self.file_path.unlink()
        except Exception as e:
            logger.warning(f"Failed to cleanup artifact {self.file_path}: {e}")


class ArtifactManager:
    """Decides where to store data and tracks artifacts"""
    
    def __init__(self, config: Optional[Dict[str, Any]] = None):
        # Default configuration
        defaults = {
            "spill_threshold_rows": 1_000_000,
            "max_ram_share": 0.7,
            "spill_dir": None,
            "memory_check_interval": 10,  # Check memory pressure every N registrations
            "enable_thread_safety": True
        }
        
        # Merge with user config
        config = config or {}
        final_config = {**defaults, **config}
        
        self.spill_threshold_rows = final_config["spill_threshold_rows"]
        self.max_ram_share = final_config["max_ram_share"]
        self.memory_check_interval = final_config["memory_check_interval"]
        self.enable_thread_safety = final_config["enable_thread_safety"]
        
        self.spill_dir = final_config["spill_dir"] or Path(tempfile.mkdtemp(prefix="pypeline_"))
        if isinstance(self.spill_dir, str):
            self.spill_dir = Path(self.spill_dir)
        
        self.spill_dir.mkdir(exist_ok=True)
        
        # Thread-safe artifact tracking
        if self.enable_thread_safety:
            self._artifacts = deque()
            self._lock = threading.RLock()
        else:
            self._artifacts = []
            self._lock = None
        
        # Memory pressure optimization
        self._memory_pressure_cache = False
        self._last_memory_check = 0
        self._registration_count = 0
    
    def register(self, data: Union[pd.DataFrame, dd.DataFrame], hint: Optional[str] = None) -> DataArtifact:
        """Register data and return artifact handle"""
        
        with self._lock if self._lock else self._null_context():
            self._registration_count += 1
            
            # Estimate size efficiently
            n_rows = self._estimate_rows(data)
            
            # Decide storage with batched memory checks
            should_spill = (
                hint == "force_disk" or
                n_rows > self.spill_threshold_rows or
                self._should_check_memory_pressure()
            )
            
            if should_spill:
                try:
                    artifact = ParquetArtifact.write(data, self.spill_dir)
                except Exception as e:
                    logger.warning(f"Parquet write failed, falling back to memory: {e}")
                    artifact = MemoryArtifact(data, n_rows)
            else:
                artifact = MemoryArtifact(data, n_rows)
            
            self._artifacts.append(artifact)
            return artifact
    
    def adopt(self, artifact: DataArtifact) -> DataArtifact:
        """Track existing artifact"""
        with self._lock if self._lock else self._null_context():
            if artifact not in self._artifacts:
                self._artifacts.append(artifact)
            return artifact
    
    def cleanup_all(self):
        """Remove all spilled files and cleanup directory"""
        with self._lock if self._lock else self._null_context():
            # Cleanup all artifacts
            for artifact in list(self._artifacts):
                if hasattr(artifact, 'cleanup'):
                    artifact.cleanup()
            
            self._artifacts.clear()
            
            # Robust directory cleanup
            if self.spill_dir.exists():
                try:
                    shutil.rmtree(self.spill_dir, ignore_errors=True)
                    logger.debug(f"Cleaned up spill directory: {self.spill_dir}")
                except Exception as e:
                    logger.warning(f"Failed to cleanup spill directory: {e}")
    
    def _estimate_rows(self, data: Union[pd.DataFrame, dd.DataFrame]) -> int:
        """Efficiently estimate row count without expensive operations"""
        try:
            if isinstance(data, pd.DataFrame):
                return len(data)
            else:  # dask
                # Simple estimation based on partitions
                if hasattr(data, 'npartitions'):
                    # Rough estimate: assume 10k rows per partition
                    return data.npartitions * 10000
                else:
                    return 1000  # conservative fallback
        except Exception:
            return 1000  # conservative fallback
    
    def _should_check_memory_pressure(self) -> bool:
        """Batched memory pressure checking to reduce psutil overhead"""
        current_time = time.time()
        
        # Check memory every N registrations or every few seconds
        if (self._registration_count % self.memory_check_interval == 0 or 
            current_time - self._last_memory_check > 5.0):
            
            try:
                memory = psutil.virtual_memory()
                self._memory_pressure_cache = memory.percent / 100 > self.max_ram_share
                self._last_memory_check = current_time
            except Exception:
                # Fallback if psutil unavailable
                self._memory_pressure_cache = False
        
        return self._memory_pressure_cache
    
    def _null_context(self):
        """No-op context manager for when thread safety is disabled"""
        from contextlib import nullcontext
        return nullcontext()
    
    def stats(self) -> Dict[str, Any]:
        """Get manager statistics"""
        with self._lock if self._lock else self._null_context():
            memory_count = sum(1 for a in self._artifacts if isinstance(a, MemoryArtifact))
            parquet_count = len(self._artifacts) - memory_count
            
            return {
                "total_artifacts": len(self._artifacts),
                "memory_artifacts": memory_count,
                "parquet_artifacts": parquet_count,
                "spill_dir": str(self.spill_dir),
                "registration_count": self._registration_count,
                "last_memory_pressure": self._memory_pressure_cache
            }


# Factory function
def create_artifact_manager(config: Optional[Dict[str, Any]] = None) -> ArtifactManager:
    return ArtifactManager(config)


if __name__ == "__main__":
    # Comprehensive test of artifact manager features
    import time
    
    print(" Artifact Manager - Feature Test")
    print("=" * 50)
    
    # Test 1: Basic functionality
    print("\n1. Testing basic registration...")
    config = {
        "spill_threshold_rows": 100,  # Low threshold for testing
        "max_ram_share": 0.9,
        "memory_check_interval": 2
    }
    mgr = ArtifactManager(config)
    
    # Small dataset (should stay in memory)
    small_df = pd.DataFrame({"id": range(50), "value": range(50, 100)})
    small_art = mgr.register(small_df)
    print(f"   Small dataset: {small_art.uri} ({small_art.n_rows} rows)")
    
    # Large dataset (should spill to parquet)
    large_df = pd.DataFrame({"id": range(200), "data": [f"row_{i}" for i in range(200)]})
    large_art = mgr.register(large_df)
    print(f"   Large dataset: {large_art.uri} ({large_art.n_rows} rows)")
    
    # Test 2: Data retrieval
    print("\n2. Testing data retrieval...")
    retrieved_pandas = small_art.to_pandas()
    retrieved_dask = small_art.to_dask()
    print(f"   Pandas: {type(retrieved_pandas).__name__} with {len(retrieved_pandas)} rows")
    print(f"   Dask: {type(retrieved_dask).__name__} with {retrieved_dask.compute().shape[0]} rows")
    
    # Test 3: Forced storage hint
    print("\n3. Testing storage hints...")
    force_disk_df = pd.DataFrame({"tiny": [1, 2, 3]})  # Tiny but force to disk
    disk_art = mgr.register(force_disk_df, hint="force_disk")
    print(f"   Force disk (tiny data): {disk_art.uri}")
    
    # Test 4: Dask DataFrame handling
    print("\n4. Testing Dask DataFrame...")
    try:
        import dask.dataframe as dd
        dask_df = dd.from_pandas(pd.DataFrame({"x": range(150), "y": range(150, 300)}), npartitions=3)
        dask_art = mgr.register(dask_df)
        print(f"   Dask artifact: {dask_art.uri} (~{dask_art.n_rows} rows)")
        
        # Test conversion both ways
        from_dask_to_pandas = dask_art.to_pandas()
        from_dask_to_dask = dask_art.to_dask()
        print(f"   Dask→Pandas: {len(from_dask_to_pandas)} rows")
        print(f"   Dask→Dask: {from_dask_to_dask.compute().shape[0]} rows")
    except ImportError:
        print("   Dask not available, skipping dask tests")
    
    # Test 5: Statistics and monitoring
    print("\n5. Manager statistics...")
    stats = mgr.stats()
    for key, value in stats.items():
        print(f"   {key}: {value}")
    
    # Test 6: Memory pressure simulation
    print("\n6. Testing memory pressure handling...")
    original_threshold = mgr.max_ram_share
    mgr.max_ram_share = 0.01  # Force memory pressure
    
    pressure_df = pd.DataFrame({"test": range(10)})
    pressure_art = mgr.register(pressure_df)
    print(f"   Under memory pressure: {pressure_art.uri}")
    mgr.max_ram_share = original_threshold  # Restore
    
    # Test 7: Artifact adoption
    print("\n7. Testing artifact adoption...")
    existing_art = large_art  # Use existing artifact
    adopted_art = mgr.adopt(existing_art)
    print(f"   Adopted artifact: {adopted_art.uri}")
    print(f"   Same object: {existing_art is adopted_art}")
    
    # Test 8: Error handling
    print("\n8. Testing error handling...")
    try:
        # Try to read from cleaned up artifact (should work until cleanup)
        test_data = large_art.to_pandas()
        print(f"   Read successful: {len(test_data)} rows")
    except Exception as e:
        print(f"   Expected error: {e}")
    
    # Test 9: Final cleanup
    print("\n9. Testing cleanup...")
    print(f"   Before cleanup: {len(mgr._artifacts)} artifacts")
    mgr.cleanup_all()
    print(f"   After cleanup: {len(mgr._artifacts)} artifacts")
    
    # Test 10: Performance with multiple registrations
    print("\n10. Performance test...")
    perf_mgr = ArtifactManager({"memory_check_interval": 5})
    start_time = time.time()
    
    for i in range(20):
        test_df = pd.DataFrame({"batch": [i] * 10})
        perf_mgr.register(test_df)
    
    end_time = time.time()
    print(f"    Registered 20 small datasets in {end_time - start_time:.3f}s")
    print(f"    Final stats: {perf_mgr.stats()}")
    perf_mgr.cleanup_all()
    
    print("\n All tests completed successfully!")
    print("=" * 50)