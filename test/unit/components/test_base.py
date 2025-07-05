import pytest
from typing import Dict, Any
from pype.components.base import BaseComponent
from pype.core.engine.pipeline_data import PipelineData


class ComponentForTesting(BaseComponent):
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
    
    def execute(self, context: Dict[str, Any], inputs: Dict[str, PipelineData]) -> Dict[str, PipelineData]:
        return {
            "output1": self._wrap_raw_data(
                "test_result",
                source=f"{self.name}_test_output"
            )
        }


class TestBaseComponent:
    
    def test_initialization_valid_config(self):
        """Test component initializes with valid configuration."""
        config = {"name": "test", "count": 5}
        component = ComponentForTesting("comp1", config)
        
        assert component.get_name() == "comp1"
        assert component.get_config() == config
    
    def test_initialization_minimal_config(self):
        """Test component initializes with only required parameters."""
        config = {"name": "test"}
        component = ComponentForTesting("comp1", config)
        assert component.get_config() == config
    
    def test_initialization_no_config_fails(self):
        """Test component initialization with None config fails for required params."""
        with pytest.raises(ValueError, match="Missing required parameter: name"):
            ComponentForTesting("comp1", None)
    
    def test_missing_required_parameter(self):
        """Test validation fails when required parameter is missing."""
        with pytest.raises(ValueError, match="Missing required parameter: name"):
            ComponentForTesting("comp1", {"count": 5})
    
    def test_wrong_required_parameter_type(self):
        """Test validation fails when required parameter has wrong type."""
        with pytest.raises(TypeError, match="Required parameter name must be of type str"):
            ComponentForTesting("comp1", {"name": 123})
    
    def test_wrong_optional_parameter_type(self):
        """Test validation fails when optional parameter has wrong type."""
        config = {"name": "test", "count": "not_int"}
        with pytest.raises(TypeError, match="Parameter count must be of type int"):
            ComponentForTesting("comp1", config)
    
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
        component = ComponentForTesting("comp1", {"name": "test"})
        
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
        schema = ComponentForTesting("comp1", {"name": "test"}).get_config_schema()
        
        assert schema["required"] == {"name": {"type": "str", "description": "Required name parameter"}}
        assert schema["optional"] == {"count": {"type": "int", "default": 10, "description": "Optional count parameter"}}
    
    def test_get_metadata_classmethod(self):
        """Test get_metadata class method returns complete metadata."""
        metadata = ComponentForTesting.get_metadata()
        
        assert metadata["name"] == "testable"
        assert metadata["class_name"] == "ComponentForTesting"
        assert metadata["category"] == "test"
        assert metadata["input_ports"] == ["input1", "input2"]
        assert metadata["output_ports"] == ["output1"]
        assert metadata["startable"] is True
        assert metadata["idempotent"] is False
    
    def test_getters_return_copies(self):
        """Test that getter methods return copies, not references."""
        component = ComponentForTesting("comp1", {"name": "test"})
        
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
        component = ComponentForTesting("comp1", {"name": "test"})
        
        assert str(component) == "ComponentForTesting(name=comp1)"
        assert "ComponentForTesting" in repr(component)
        assert "comp1" in repr(component)
    
    def test_pipeline_data_helper_methods(self):
        """Test PipelineData helper methods."""
        component = ComponentForTesting("comp1", {"name": "test"})
        
        # Test _wrap_raw_data
        raw_data = {"test": "data"}
        wrapped = component._wrap_raw_data(raw_data, "test_source")
        
        assert isinstance(wrapped, PipelineData)
        assert wrapped.get_raw_data() == raw_data
        assert wrapped.source == "test_source"
        
        # Test with default source
        wrapped_default = component._wrap_raw_data(raw_data)
        assert wrapped_default.source == "comp1_testable"
        
        # Test _extract_raw_data
        extracted = component._extract_raw_data(wrapped)
        assert extracted == raw_data
    
    def test_execute_returns_pipeline_data(self):
        """Test execute method returns PipelineData objects."""
        component = ComponentForTesting("comp1", {"name": "test"})
        
        # Create test inputs with PipelineData
        inputs = {
            "input1": PipelineData("test_input", source="upstream")
        }
        
        result = component.execute({}, inputs)
        
        # Verify output structure
        assert "output1" in result
        assert isinstance(result["output1"], PipelineData)
        assert result["output1"].get_raw_data() == "test_result"
        assert result["output1"].source == "comp1_test_output"