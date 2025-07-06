import pytest
import tempfile
import os
import time
import sqlite3
from contextlib import contextmanager
from pathlib import Path
from pype.core.registry.sqlite_backend import BaseSQLBackend


@contextmanager
def windows_safe_temp_db():
    """Context manager for Windows-safe temporary database files."""
    temp_file = None
    temp_path = None
    try:
        temp_file = tempfile.NamedTemporaryFile(delete=False, suffix='.db')
        temp_path = temp_file.name
        temp_file.close()  # Close immediately to avoid Windows locking issues
        yield temp_path
    finally:
        # Cleanup with retry logic for Windows
        if temp_path and os.path.exists(temp_path):
            for attempt in range(10):
                try:
                    os.unlink(temp_path)
                    break
                except (PermissionError, OSError):
                    time.sleep(0.1)
                    continue


@contextmanager
def isolated_temp_dir():
    """Context manager for isolated temporary directory with proper cleanup."""
    temp_dir = None
    try:
        temp_dir = tempfile.mkdtemp()
        yield temp_dir
    finally:
        if temp_dir:
            # Force cleanup with retry for Windows
            import shutil
            for attempt in range(10):
                try:
                    shutil.rmtree(temp_dir)
                    break
                except (PermissionError, OSError):
                    time.sleep(0.1)
                    continue


class TestBaseSQLBackend:
    
    @pytest.fixture
    def backend(self):
        """Create backend with temporary file database."""
        with windows_safe_temp_db() as temp_path:
            backend_instance = BaseSQLBackend(temp_path)
            yield backend_instance
            # Force close any connections
            del backend_instance
    
    def test_initialization_creates_db_file(self):
        """Test that initialization creates database file."""
        with windows_safe_temp_db() as temp_path:
            backend = BaseSQLBackend(temp_path)
            assert Path(temp_path).exists()
            del backend  # Clean up
    
    def test_initialization_creates_parent_directory(self):
        """Test that initialization creates parent directories if needed."""
        with isolated_temp_dir() as temp_dir:
            nested_path = os.path.join(temp_dir, "nested", "path", "test.db")
            backend = BaseSQLBackend(nested_path)
            assert os.path.exists(nested_path)
            # Force connection close before cleanup
            del backend
    
    def test_init_db_creates_components_table(self, backend):
        """Test that _init_db creates components table."""
        with sqlite3.connect(backend.db_path) as conn:
            cursor = conn.execute("""
                SELECT name FROM sqlite_master 
                WHERE type='table' AND name='components'
            """)
            assert cursor.fetchone() is not None
    
    def test_init_db_creates_joblets_table(self, backend):
        """Test that _init_db creates joblets table."""
        with sqlite3.connect(backend.db_path) as conn:
            cursor = conn.execute("""
                SELECT name FROM sqlite_master 
                WHERE type='table' AND name='joblets'
            """)
            assert cursor.fetchone() is not None
    
    def test_serialize_field_list(self, backend):
        """Test serializing list to JSON string."""
        test_list = ["item1", "item2", "item3"]
        result = backend._serialize_field(test_list)
        assert result == '["item1", "item2", "item3"]'
    
    def test_serialize_field_dict(self, backend):
        """Test serializing dict to JSON string."""
        test_dict = {"key1": "value1", "key2": 42}
        result = backend._serialize_field(test_dict)
        assert '"key1": "value1"' in result
        assert '"key2": 42' in result
    
    def test_serialize_field_string(self, backend):
        """Test serializing string returns string."""
        test_string = "test_value"
        result = backend._serialize_field(test_string)
        assert result == "test_value"
    
    def test_serialize_field_none(self, backend):
        """Test serializing None returns empty string."""
        result = backend._serialize_field(None)
        assert result == ""
    
    def test_deserialize_field_json_list(self, backend):
        """Test deserializing JSON list."""
        json_string = '["item1", "item2"]'
        result = backend._deserialize_field(json_string, [])
        assert result == ["item1", "item2"]
    
    def test_deserialize_field_json_dict(self, backend):
        """Test deserializing JSON dict."""
        json_string = '{"key": "value"}'
        result = backend._deserialize_field(json_string, {})
        assert result == {"key": "value"}
    
    def test_deserialize_field_empty_string_returns_default(self, backend):
        """Test that empty string returns default value."""
        result = backend._deserialize_field("", ["default"])
        assert result == ["default"]
    
    def test_deserialize_field_invalid_json_returns_default(self, backend):
        """Test that invalid JSON returns default value."""
        result = backend._deserialize_field("invalid_json", {"default": True})
        assert result == {"default": True}
    
    def test_get_current_timestamp_format(self, backend):
        """Test that timestamp is in ISO format."""
        timestamp = backend._get_current_timestamp()
        # Should be parseable as ISO datetime
        from datetime import datetime
        parsed = datetime.fromisoformat(timestamp)
        assert isinstance(parsed, datetime)
    
    def test_get_current_timestamp_unique(self, backend):
        """Test that consecutive timestamps are different."""
        timestamp1 = backend._get_current_timestamp()
        time.sleep(0.001)  # Small delay
        timestamp2 = backend._get_current_timestamp()
        assert timestamp1 != timestamp2
    
    def test_multiple_backends_same_db(self):
        """Test multiple backend instances can use same database."""
        with windows_safe_temp_db() as temp_path:
            backend1 = BaseSQLBackend(temp_path)
            backend2 = BaseSQLBackend(temp_path)
            
            # Test using a separate connection to avoid locking
            with sqlite3.connect(temp_path) as conn:
                cursor = conn.execute("""
                    SELECT COUNT(*) FROM sqlite_master 
                    WHERE type='table' AND name IN ('components', 'joblets')
                """)
                table_count = cursor.fetchone()[0]
                assert table_count == 2
            
            # Explicit cleanup
            del backend1
            del backend2
    
    def test_default_db_path_constant(self):
        """Test that None db_path uses constant from utils."""
        backend = BaseSQLBackend(None)
        try:
            assert backend.db_path is not None
            assert str(backend.db_path).endswith('.db')
        finally:
            del backend
    
    def test_serialization_round_trip(self, backend):
        """Test that serialize/deserialize is lossless."""
        test_data = {
            "list": ["a", "b", "c"],
            "dict": {"nested": {"key": "value"}},
            "mixed": [{"item": 1}, {"item": 2}]
        }
        
        for key, original in test_data.items():
            serialized = backend._serialize_field(original)
            deserialized = backend._deserialize_field(serialized, None)
            assert deserialized == original, f"Round trip failed for {key}"
    
    def test_database_file_creation_permissions(self):
        """Test database file is created with proper permissions."""
        with windows_safe_temp_db() as temp_path:
            backend = BaseSQLBackend(temp_path)
            
            # File should exist and be readable/writable
            assert os.path.exists(temp_path)
            assert os.access(temp_path, os.R_OK)
            assert os.access(temp_path, os.W_OK)
            
            del backend
    
    def test_concurrent_initialization_same_path(self):
        """Test that concurrent initialization doesn't cause conflicts."""
        with windows_safe_temp_db() as temp_path:
            # Initialize multiple backends with same path
            backends = []
            try:
                for i in range(3):
                    backends.append(BaseSQLBackend(temp_path))
                
                # All should work without errors
                assert len(backends) == 3
                
                # Database should still be valid
                with sqlite3.connect(temp_path) as conn:
                    cursor = conn.execute("SELECT name FROM sqlite_master WHERE type='table'")
                    tables = [row[0] for row in cursor.fetchall()]
                    assert 'components' in tables
                    assert 'joblets' in tables
                    
            finally:
                # Clean up all backends
                for backend in backends:
                    del backend