import pytest
from typing import Dict, Any
from pype.components.base import BaseComponent


class TestableComponent(BaseComponent):
    """Concrete implementation for testing BaseComponent."""
    
    COMPONENT_NAME = "testable"
    CATEGORY = "test"
    INPUT_PORTS = ["input1", "input2"]
    OUTPUT_PORTS = ["output1"]
    OUTPUT_GLOBALS = ["global1"]
    DEPENDENCIES = ["dep1"]
    STARTABLE = True
    EVENTS = ["ok", "error"]
    ALLOW_MULTI_IN = True
    IDEMPOTENT = False
    
    CONFIG_SCHEMA = {
        "required": {
            "name": {"type": "str", "description": "Required name parameter"}
        },
        "optional": {
            "count": {"type": "int", "default": 10, "description": "Optional count parameter"}
        }
    }
    
    def execute(self, context: Dict[str, Any], inputs: Dict[str, Any]) -> Dict[str, Any]:
        return {"output1": "test_result"}


class TestBaseComponent:
    
    def test_initialization_valid_config(self):
        """Test component initializes with valid configuration."""
        config = {"name": "test", "count": 5}
        component = TestableComponent("comp1", config)
        
        assert component.get_name() == "comp1"
        assert component.get_config() == config
    
    def test_initialization_minimal_config(self):
        """Test component initializes with only required parameters."""
        config = {"name": "test"}
        component = TestableComponent("comp1", config)
        assert component.get_config() == config
    
    def test_initialization_no_config(self):
        """Test component initialization with None config defaults to empty dict."""
        with pytest.raises(ValueError, match="Missing required parameter: name"):
            TestableComponent("comp1", None)
    
    def test_missing_required_parameter(self):
        """Test validation fails when required parameter is missing."""
        with pytest.raises(ValueError, match="Missing required parameter: name"):
            TestableComponent("comp1", {"count": 5})
    
    def test_wrong_required_parameter_type(self):
        """Test validation fails when required parameter has wrong type."""
        with pytest.raises(TypeError, match="Required parameter name must be of type str"):
            TestableComponent("comp1", {"name": 123})
    
    def test_wrong_optional_parameter_type(self):
        """Test validation fails when optional parameter has wrong type."""
        config = {"name": "test", "count": "not_int"}
        with pytest.raises(TypeError, match="Parameter count must be of type int"):
            TestableComponent("comp1", config)
    
    def test_unknown_type_ignored(self):
        """Test that unknown type strings are ignored during validation."""
        class UnknownTypeComponent(BaseComponent):
            CONFIG_SCHEMA = {
                "required": {"param": {"type": "unknown", "description": "test"}},
                "optional": {}
            }
            def execute(self, context, inputs):
                return {}
        
        # Should not raise error
        component = UnknownTypeComponent("test", {"param": "value"})
        assert component.get_config() == {"param": "value"}
    
    def test_metadata_attributes(self):
        """Test all metadata getter methods."""
        component = TestableComponent("comp1", {"name": "test"})
        
        assert component.get_component_name() == "testable"
        assert component.get_category() == "test"
        assert component.get_input_ports() == ["input1", "input2"]
        assert component.get_output_ports() == ["output1"]
        assert component.get_output_globals() == ["global1"]
        assert component.get_dependencies() == ["dep1"]
        assert component.get_events() == ["ok", "error"]
        assert component.is_startable() is True
        assert component.allows_multi_input() is True
        assert component.is_idempotent() is False
    
    def test_get_config_schema(self):
        """Test config schema getter returns correct structure."""
        schema = TestableComponent("comp1", {"name": "test"}).get_config_schema()
        
        assert schema["required"] == {"name": {"type": "str", "description": "Required name parameter"}}
        assert schema["optional"] == {"count": {"type": "int", "default": 10, "description": "Optional count parameter"}}
    
    def test_get_metadata_classmethod(self):
        """Test get_metadata class method returns complete metadata."""
        metadata = TestableComponent.get_metadata()
        
        assert metadata["name"] == "testable"
        assert metadata["class_name"] == "TestableComponent"
        assert metadata["category"] == "test"
        assert metadata["input_ports"] == ["input1", "input2"]
        assert metadata["output_ports"] == ["output1"]
        assert metadata["startable"] is True
        assert metadata["idempotent"] is False
    
    def test_getters_return_copies(self):
        """Test that getter methods return copies, not references."""
        component = TestableComponent("comp1", {"name": "test"})
        
        # Modify returned lists/dicts
        component.get_input_ports().append("new_port")
        component.get_config()["new_key"] = "new_value"
        
        # Original should be unchanged
        assert "new_port" not in component.get_input_ports()
        assert "new_key" not in component.get_config()
    
    def test_abstract_base_cannot_instantiate(self):
        """Test BaseComponent cannot be instantiated directly."""
        with pytest.raises(TypeError):
            BaseComponent("test", {})
    
    def test_str_and_repr(self):
        """Test string representations."""
        component = TestableComponent("comp1", {"name": "test"})
        
        assert str(component) == "TestableComponent(name=comp1)"
        assert "TestableComponent" in repr(component)
        assert "comp1" in repr(component)