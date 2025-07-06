import pytest
import tempfile
import os
import time
import atexit
from contextlib import contextmanager
from pathlib import Path
from pype.core.registry.component_registry import ComponentRegistry


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


class TestComponentRegistry:
    
    @pytest.fixture
    def registry(self):
        """Create registry with temporary file database."""
        with temp_db_file() as temp_path:
            yield ComponentRegistry(temp_path)
    
    @pytest.fixture
    def sample_component(self):
        """Sample component data for testing."""
        return {
            "name": "test_component",
            "class_name": "TestComponent", 
            "module_path": "test.module",
            "category": "test",
            "description": "A test component",
            "input_ports": ["input1"],
            "output_ports": ["output1"],
            "required_params": {"param1": {"type": "str"}},
            "optional_params": {"param2": {"type": "int", "default": 42}},
            "output_globals": ["global1"],
            "dependencies": ["dep1"],
            "startable": True,
            "allow_multi_in": False,
            "idempotent": True
        }
    
    def test_registry_initialization_creates_empty_db(self, registry):
        """Test registry initializes with empty database."""
        components = registry.list_components()
        assert components == []
    
    def test_register_component_success(self, registry, sample_component):
        """Test successful component registration."""
        result = registry.register_component(sample_component)
        assert result is True
        
        retrieved = registry.get_component("test_component")
        assert retrieved is not None
        assert retrieved["name"] == "test_component"
        assert retrieved["class_name"] == "TestComponent"
    
    def test_register_component_updates_existing(self, registry, sample_component):
        """Test registering component with same name updates existing."""
        # Register original
        registry.register_component(sample_component)
        
        # Update and register again
        updated = sample_component.copy()
        updated["description"] = "Updated description"
        result = registry.register_component(updated)
        
        assert result is True
        retrieved = registry.get_component("test_component")
        assert retrieved["description"] == "Updated description"
    
    def test_register_component_validation_failure(self, registry):
        """Test registration fails with invalid component data."""
        invalid_component = {"name": "test"}  # Missing required fields
        
        result = registry.register_component(invalid_component)
        assert result is False
    
    def test_register_component_invalid_name_pattern(self, registry, sample_component):
        """Test registration fails with invalid name pattern."""
        sample_component["name"] = "123invalid"  # Can't start with number
        result = registry.register_component(sample_component)
        assert result is False
    
    def test_register_component_invalid_types(self, registry, sample_component):
        """Test registration fails with invalid field types."""
        sample_component["input_ports"] = "not_a_list"  # Should be list
        result = registry.register_component(sample_component)
        assert result is False
    
    def test_get_component_not_found(self, registry):
        """Test get_component returns None for non-existent component."""
        result = registry.get_component("nonexistent")
        assert result is None
    
    def test_list_components_multiple(self, registry, sample_component):
        """Test listing multiple registered components."""
        # Register first component
        registry.register_component(sample_component)
        
        # Register second component
        second_component = sample_component.copy()
        second_component["name"] = "second_component"
        registry.register_component(second_component)
        
        components = registry.list_components()
        assert len(components) == 2
        
        names = [c["name"] for c in components]
        assert "test_component" in names
        assert "second_component" in names
    
    def test_delete_component_success(self, registry, sample_component):
        """Test successful component deletion."""
        # Register component
        registry.register_component(sample_component)
        assert registry.get_component("test_component") is not None
        
        # Delete component
        result = registry.delete_component("test_component")
        assert result is True
        
        # Verify deletion
        assert registry.get_component("test_component") is None
    
    def test_delete_component_not_found(self, registry):
        """Test deleting non-existent component returns False."""
        result = registry.delete_component("nonexistent")
        assert result is False
    
    def test_field_serialization_deserialization(self, registry, sample_component):
        """Test complex fields are properly serialized and deserialized."""
        registry.register_component(sample_component)
        retrieved = registry.get_component("test_component")
        
        # Test list fields
        assert retrieved["input_ports"] == ["input1"]
        assert retrieved["output_ports"] == ["output1"]
        
        # Test dict fields
        assert retrieved["required_params"] == {"param1": {"type": "str"}}
        assert retrieved["optional_params"] == {"param2": {"type": "int", "default": 42}}
        
        # Test boolean fields
        assert retrieved["startable"] is True
        assert retrieved["allow_multi_in"] is False
        assert retrieved["idempotent"] is True
    
    def test_default_values_applied(self, registry):
        """Test default values are applied for missing fields."""
        minimal_component = {
            "name": "minimal",
            "class_name": "Minimal",
            "module_path": "test.minimal",
            "category": "test",
            "description": "Minimal component",
            "input_ports": [],
            "output_ports": [],
            "required_params": {},
            "optional_params": {},
            "output_globals": [],
            "dependencies": [],
            "startable": False,
            "allow_multi_in": False,
            "idempotent": True
        }
        
        result = registry.register_component(minimal_component)
        assert result is True
        
        retrieved = registry.get_component("minimal")
        assert retrieved["startable"] is False
    
    def test_database_persistence(self, sample_component):
        """Test data persists across registry instances using temporary file."""
        with temp_db_file() as temp_path:
            # Register component with first registry instance
            registry1 = ComponentRegistry(temp_path)
            registry1.register_component(sample_component)
            
            # Create new registry instance with same database
            registry2 = ComponentRegistry(temp_path)
            retrieved = registry2.get_component("test_component")
            
            assert retrieved is not None
            assert retrieved["name"] == "test_component"
    
    def test_extract_component_metadata_method(self, registry):
        """Test _extract_component_metadata handles component classes correctly."""
        # Test with mock class
        class MockComponent:
            COMPONENT_NAME = "mock"
            CATEGORY = "test"
            INPUT_PORTS = ["in1"]
            OUTPUT_PORTS = ["out1"]
            CONFIG_SCHEMA = {"required": {}, "optional": {}}
            OUTPUT_GLOBALS = []
            DEPENDENCIES = []
            STARTABLE = True
            ALLOW_MULTI_IN = False
            IDEMPOTENT = True
            __doc__ = "Mock component for testing"
        
        metadata = registry._extract_component_metadata(MockComponent, "MockComponent", "test.mock")
        
        assert metadata["name"] == "mock"
        assert metadata["class_name"] == "MockComponent"
        assert metadata["module_path"] == "test.mock"
        assert metadata["description"] == "Mock component for testing"
    
    def test_extract_component_metadata_defaults(self, registry):
        """Test _extract_component_metadata uses defaults for missing attributes."""
        class MinimalComponent:
            pass
        
        metadata = registry._extract_component_metadata(MinimalComponent, "MinimalComponent", "test.minimal")
        
        assert metadata["name"] == "minimalcomponent"  # class_name.lower()
        assert metadata["category"] == "unknown"
        assert metadata["input_ports"] == []
        assert metadata["startable"] is False
        assert metadata["idempotent"] is True
    
    def test_prepare_component_data_with_defaults(self, registry):
        """Test _prepare_component_data applies defaults correctly."""
        minimal_data = {
            "name": "test",
            "class_name": "Test",
            "module_path": "test.module"
        }
        
        result = registry._prepare_component_data(minimal_data)
        
        # Should have 15 elements (all fields)
        assert len(result) == 15
        assert result[0] == "test"  # name
        assert result[3] == "unknown"  # category default
        assert result[4] == ""  # description default
    
    def test_validate_component_missing_required_fields(self, registry):
        """Test validation fails when required fields are missing."""
        incomplete_component = {
            "name": "test",
            "class_name": "Test"
            # Missing other required fields
        }
        
        result = registry._validate_component(incomplete_component)
        assert result is False
    
    def test_validate_component_invalid_name_pattern(self, registry, sample_component):
        """Test validation fails with various invalid name patterns."""
        invalid_names = ["123test", "test-name", "test.name", "", "test name"]
        
        for invalid_name in invalid_names:
            sample_component["name"] = invalid_name
            result = registry._validate_component(sample_component)
            assert result is False, f"Name '{invalid_name}' should be invalid"
    
    def test_validate_component_type_checking(self, registry, sample_component):
        """Test validation performs type checking on fields."""
        # Test invalid list field
        sample_component["input_ports"] = "not_a_list"
        assert registry._validate_component(sample_component) is False
        
        # Test invalid dict field
        sample_component["input_ports"] = ["valid"]
        sample_component["required_params"] = "not_a_dict"
        assert registry._validate_component(sample_component) is False
        
        # Test invalid boolean field
        sample_component["required_params"] = {}
        sample_component["startable"] = "not_a_bool"
        assert registry._validate_component(sample_component) is False