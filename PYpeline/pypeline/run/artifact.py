"""
PYpeline Artifact Manager - Lightweight data location registry
"""

import tempfile
import psutil
import logging
from pathlib import Path
from typing import Protocol, Union, Dict, Any, Optional

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
    
    def __init__(self, data: Union[pd.DataFrame, dd.DataFrame]):
        self._data = data
        self._uri = f"memory://{id(data)}"
    
    def to_pandas(self, **kwargs) -> pd.DataFrame:
        data_type = type(self._data).__name__
        
        if data_type == "DataFrame" and hasattr(self._data, 'iloc'):  # pandas
            return self._data
        elif hasattr(self._data, 'compute'):  # dask or similar lazy frameworks
            return self._data.compute()
        else:
            raise NotImplementedError(f"to_pandas not implemented for {data_type}")
    
    def to_dask(self, **kwargs) -> dd.DataFrame:
        data_type = type(self._data).__name__
        
        if data_type in ("DataFrame", "Series") and hasattr(self._data, 'iloc'):  # pandas
            return dd.from_pandas(self._data, npartitions=1)
        elif hasattr(self._data, 'repartition'):  # dask
            return self._data
        else:
            raise NotImplementedError(f"to_dask not implemented for {data_type}")
    
    @property
    def uri(self) -> str:
        return self._uri
    
    @property
    def n_rows(self) -> int:
        if isinstance(self._data, pd.DataFrame):
            return len(self._data)
        return len(self._data.compute())


class ParquetArtifact:
    """Points to parquet file on disk"""
    
    def __init__(self, file_path: Path, n_rows: int):
        self.file_path = file_path
        self._n_rows = n_rows
        self._uri = f"parquet://{file_path}"
    
    @classmethod
    def write(cls, data: Union[pd.DataFrame, dd.DataFrame], spill_dir: Path) -> "ParquetArtifact":
        temp_file = tempfile.NamedTemporaryFile(
            suffix=".parquet", dir=spill_dir, delete=False
        )
        file_path = Path(temp_file.name)
        temp_file.close()
        
        if isinstance(data, pd.DataFrame):
            data.to_parquet(file_path, index=False)
            n_rows = len(data)
        else:  # dask
            data.to_parquet(file_path, index=False)
            n_rows = len(data.compute())
        
        logger.info(f"Spilled {n_rows} rows to {file_path}")
        return cls(file_path, n_rows)
    
    def to_pandas(self, **kwargs) -> pd.DataFrame:
        return pd.read_parquet(self.file_path, **kwargs)
    
    def to_dask(self, **kwargs) -> dd.DataFrame:
        return dd.read_parquet(self.file_path, **kwargs)
    
    @property
    def uri(self) -> str:
        return self._uri
    
    @property
    def n_rows(self) -> int:
        return self._n_rows
    
    def cleanup(self):
        if self.file_path.exists():
            self.file_path.unlink()


class ArtifactManager:
    """Decides where to store data and tracks artifacts"""
    
    def __init__(self, config: Optional[Dict[str, Any]] = None):
        # Default configuration
        defaults = {
            "spill_threshold_rows": 1_000_000,
            "max_ram_share": 0.7,
            "spill_dir": None
        }
        
        # Merge with user config
        config = config or {}
        final_config = {**defaults, **config}
        
        self.spill_threshold_rows = final_config["spill_threshold_rows"]
        self.max_ram_share = final_config["max_ram_share"]
        self.spill_dir = final_config["spill_dir"] or Path(tempfile.mkdtemp(prefix="pypeline_"))
        
        if isinstance(self.spill_dir, str):
            self.spill_dir = Path(self.spill_dir)
        
        self.spill_dir.mkdir(exist_ok=True)
        self._artifacts = []
    
    def register(self, data: Union[pd.DataFrame, dd.DataFrame], hint: Optional[str] = None) -> DataArtifact:
        """Register data and return artifact handle"""
        
        # Estimate size
        if isinstance(data, pd.DataFrame):
            n_rows = len(data)
        else:  # dask
            n_rows = data.map_partitions(len).sum().compute()
        
        # Decide storage
        should_spill = (
            hint == "force_disk" or
            n_rows > self.spill_threshold_rows or
            self._memory_pressure()
        )
        
        if should_spill:
            artifact = ParquetArtifact.write(data, self.spill_dir)
        else:
            artifact = MemoryArtifact(data)
        
        self._artifacts.append(artifact)
        return artifact
    
    def adopt(self, artifact: DataArtifact) -> DataArtifact:
        """Track existing artifact"""
        if artifact not in self._artifacts:
            self._artifacts.append(artifact)
        return artifact
    
    def cleanup_all(self):
        """Remove all spilled files"""
        for artifact in self._artifacts:
            if hasattr(artifact, 'cleanup'):
                artifact.cleanup()
        self._artifacts.clear()
        
        if self.spill_dir.exists():
            try:
                self.spill_dir.rmdir()
            except OSError:
                pass
    
    def _memory_pressure(self) -> bool:
        """Check if system memory usage is high"""
        memory = psutil.virtual_memory()
        return memory.percent / 100 > self.max_ram_share
    
    def stats(self) -> Dict[str, Any]:
        """Get manager statistics"""
        memory_count = sum(1 for a in self._artifacts if isinstance(a, MemoryArtifact))
        parquet_count = len(self._artifacts) - memory_count
        
        return {
            "total_artifacts": len(self._artifacts),
            "memory_artifacts": memory_count,
            "parquet_artifacts": parquet_count,
            "spill_dir": str(self.spill_dir)
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