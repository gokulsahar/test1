import pytest
import tempfile
import os
import time
import atexit
from contextlib import contextmanager
from pathlib import Path
from pype.core.registry.joblet_registry import JobletRegistry


# Global list to track temp files for emergency cleanup
_temp_files_to_cleanup = []

def _emergency_cleanup():
    """Emergency cleanup function called on exit."""
    for temp_file in _temp_files_to_cleanup:
        try:
            if os.path.exists(temp_file):
                os.unlink(temp_file)
        except:
            pass

# Register emergency cleanup
atexit.register(_emergency_cleanup)


@contextmanager
def temp_db_file():
    """Context manager for temporary database files with guaranteed cleanup."""
    temp_file = tempfile.NamedTemporaryFile(delete=False, suffix='.db')
    temp_file.close()
    temp_path = temp_file.name
    
    # Add to emergency cleanup list
    _temp_files_to_cleanup.append(temp_path)
    
    try:
        yield temp_path
    finally:
        # Primary cleanup
        for attempt in range(5):
            try:
                if os.path.exists(temp_path):
                    os.unlink(temp_path)
                # Remove from emergency list if successfully deleted
                if temp_path in _temp_files_to_cleanup:
                    _temp_files_to_cleanup.remove(temp_path)
                break
            except (PermissionError, OSError):
                time.sleep(0.1)
                continue


class TestJobletRegistry:
    
    @pytest.fixture
    def registry(self):
        """Create registry with temporary file database."""
        with temp_db_file() as temp_path:
            yield JobletRegistry(temp_path)
    
    @pytest.fixture
    def sample_joblet(self):
        """Sample joblet data for testing."""
        return {
            "name": "test_joblet",
            "file_name": "test_joblet.joblet.yaml",
            "hash": "abc123def456789",
            "input_ports": ["input1", "input2"],
            "output_ports": ["output1"]
        }
    
    def test_registry_initialization_creates_empty_db(self, registry):
        """Test registry initializes with empty database."""
        joblets = registry.list_joblets()
        assert joblets == []
    
    def test_add_joblet_success(self, registry, sample_joblet):
        """Test successful joblet addition."""
        result = registry.add_joblet(sample_joblet)
        assert result is True
        
        retrieved = registry.get_joblet("test_joblet")
        assert retrieved is not None
        assert retrieved["name"] == "test_joblet"
        assert retrieved["file_name"] == "test_joblet.joblet.yaml"
        assert retrieved["hash"] == "abc123def456789"
    
    def test_add_joblet_duplicate_name_fails(self, registry, sample_joblet):
        """Test adding joblet with duplicate name fails."""
        # Add first joblet
        result1 = registry.add_joblet(sample_joblet)
        assert result1 is True
        
        # Try to add second joblet with same name
        duplicate_joblet = sample_joblet.copy()
        duplicate_joblet["hash"] = "different_hash"
        result2 = registry.add_joblet(duplicate_joblet)
        assert result2 is False
    
    def test_add_joblet_validation_failure(self, registry):
        """Test addition fails with invalid joblet data."""
        invalid_joblet = {"name": "test"}  # Missing required fields
        
        result = registry.add_joblet(invalid_joblet)
        assert result is False
    
    def test_update_joblet_success(self, registry, sample_joblet):
        """Test successful joblet update."""
        # Add original joblet
        registry.add_joblet(sample_joblet)
        
        # Update joblet data
        updated_joblet = sample_joblet.copy()
        updated_joblet["hash"] = "updated_hash_value"
        updated_joblet["file_name"] = "updated_file.joblet.yaml"
        
        result = registry.update_joblet(updated_joblet)
        assert result is True
        
        # Verify update
        retrieved = registry.get_joblet("test_joblet")
        assert retrieved["hash"] == "updated_hash_value"
        assert retrieved["file_name"] == "updated_file.joblet.yaml"
    
    def test_update_joblet_not_found(self, registry, sample_joblet):
        """Test updating non-existent joblet returns False."""
        result = registry.update_joblet(sample_joblet)
        assert result is False
    
    def test_get_joblet_not_found(self, registry):
        """Test get_joblet returns None for non-existent joblet."""
        result = registry.get_joblet("nonexistent")
        assert result is None
    
    def test_list_joblets_multiple(self, registry, sample_joblet):
        """Test listing multiple registered joblets."""
        # Add first joblet
        registry.add_joblet(sample_joblet)
        
        # Add second joblet
        second_joblet = sample_joblet.copy()
        second_joblet["name"] = "second_joblet"
        second_joblet["hash"] = "different_hash"
        registry.add_joblet(second_joblet)
        
        joblets = registry.list_joblets()
        assert len(joblets) == 2
        
        names = [j["name"] for j in joblets]
        assert "test_joblet" in names
        assert "second_joblet" in names
    
    def test_delete_joblet_success(self, registry, sample_joblet):
        """Test successful joblet deletion."""
        # Add joblet
        registry.add_joblet(sample_joblet)
        assert registry.get_joblet("test_joblet") is not None
        
        # Delete joblet
        result = registry.delete_joblet("test_joblet")
        assert result is True
        
        # Verify deletion
        assert registry.get_joblet("test_joblet") is None
    
    def test_delete_joblet_not_found(self, registry):
        """Test deleting non-existent joblet returns False."""
        result = registry.delete_joblet("nonexistent")
        assert result is False
    
    def test_field_serialization_deserialization(self, registry, sample_joblet):
        """Test port fields are properly serialized and deserialized."""
        registry.add_joblet(sample_joblet)
        retrieved = registry.get_joblet("test_joblet")
        
        # Test list fields
        assert retrieved["input_ports"] == ["input1", "input2"]
        assert retrieved["output_ports"] == ["output1"]
    
    def test_optional_port_fields_default_empty(self, registry):
        """Test that optional port fields default to empty arrays."""
        minimal_joblet = {
            "name": "minimal",
            "file_name": "minimal.joblet.yaml",
            "hash": "minimal_hash"
            # No port fields provided
        }
        
        result = registry.add_joblet(minimal_joblet)
        assert result is True
        
        retrieved = registry.get_joblet("minimal")
        assert retrieved["input_ports"] == []
        assert retrieved["output_ports"] == []
    
    def test_database_persistence(self, sample_joblet):
        """Test data persists across registry instances."""
        with temp_db_file() as temp_path:
            # Add joblet with first registry instance
            registry1 = JobletRegistry(temp_path)
            registry1.add_joblet(sample_joblet)
            
            # Create new registry instance with same database
            registry2 = JobletRegistry(temp_path)
            retrieved = registry2.get_joblet("test_joblet")
            
            assert retrieved is not None
            assert retrieved["name"] == "test_joblet"
    
    def test_validate_joblet_missing_required_fields(self, registry):
        """Test validation fails when required fields are missing."""
        incomplete_joblets = [
            {"file_name": "test.yaml", "hash": "hash123"},  # Missing name
            {"name": "test", "hash": "hash123"},  # Missing file_name
            {"name": "test", "file_name": "test.yaml"},  # Missing hash
        ]
        
        for incomplete in incomplete_joblets:
            result = registry._validate_joblet(incomplete)
            assert result is False
    
    def test_validate_joblet_invalid_name_pattern(self, registry, sample_joblet):
        """Test validation fails with invalid name patterns."""
        invalid_names = ["123test", "test-name", "test.name", "", "test name"]
        
        for invalid_name in invalid_names:
            sample_joblet["name"] = invalid_name
            result = registry._validate_joblet(sample_joblet)
            assert result is False, f"Name '{invalid_name}' should be invalid"
    
    def test_validate_joblet_empty_file_name(self, registry, sample_joblet):
        """Test validation fails with empty file name."""
        sample_joblet["file_name"] = ""
        result = registry._validate_joblet(sample_joblet)
        assert result is False
    
    def test_validate_joblet_empty_hash(self, registry, sample_joblet):
        """Test validation fails with empty hash."""
        sample_joblet["hash"] = ""
        result = registry._validate_joblet(sample_joblet)
        assert result is False
    
    def test_validate_joblet_invalid_port_types(self, registry, sample_joblet):
        """Test validation fails when port fields are not lists."""
        # Test invalid input_ports
        sample_joblet["input_ports"] = "not_a_list"
        assert registry._validate_joblet(sample_joblet) is False
        
        # Test invalid output_ports
        sample_joblet["input_ports"] = ["valid"]
        sample_joblet["output_ports"] = "not_a_list"
        assert registry._validate_joblet(sample_joblet) is False
    
    def test_validate_joblet_valid_minimal(self, registry):
        """Test validation passes with minimal valid joblet."""
        minimal_joblet = {
            "name": "valid_name",
            "file_name": "valid.yaml",
            "hash": "valid_hash"
        }
        
        result = registry._validate_joblet(minimal_joblet)
        assert result is True
    
    def test_timestamps_created_and_updated(self, registry, sample_joblet):
        """Test that created_at and updated_at timestamps are set."""
        registry.add_joblet(sample_joblet)
        retrieved = registry.get_joblet("test_joblet")
        
        assert "created_at" in retrieved
        assert "updated_at" in retrieved
        assert retrieved["created_at"] is not None
        assert retrieved["updated_at"] is not None
    
    def test_update_changes_updated_at_timestamp(self, registry, sample_joblet):
        """Test that update operation changes updated_at timestamp."""
        # Add original
        registry.add_joblet(sample_joblet)
        original = registry.get_joblet("test_joblet")
        
        # Small delay to ensure timestamp difference
        time.sleep(0.001)
        
        # Update
        sample_joblet["hash"] = "new_hash"
        registry.update_joblet(sample_joblet)
        updated = registry.get_joblet("test_joblet")
        
        assert updated["created_at"] == original["created_at"]  # Should not change
        assert updated["updated_at"] != original["updated_at"]  # Should change