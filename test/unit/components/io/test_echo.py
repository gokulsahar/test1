import pytest
from pype.components.io.echo import EchoComponent


class TestEchoComponent:
    
    def test_component_metadata(self):
        """Test EchoComponent has correct metadata."""
        assert EchoComponent.COMPONENT_NAME == "echo"
        assert EchoComponent.CATEGORY == "io"
        assert EchoComponent.INPUT_PORTS == ["main"]
        assert EchoComponent.OUTPUT_PORTS == ["main"]
        assert EchoComponent.OUTPUT_GLOBALS == []
        assert EchoComponent.DEPENDENCIES == []
        assert EchoComponent.STARTABLE is True
        assert EchoComponent.EVENTS == ["ok", "error"]
        assert EchoComponent.ALLOW_MULTI_IN is False
        assert EchoComponent.IDEMPOTENT is True
    
    def test_config_schema(self):
        """Test EchoComponent configuration schema."""
        schema = EchoComponent.CONFIG_SCHEMA
        
        assert schema["required"] == {}
        assert "message" in schema["optional"]
        assert schema["optional"]["message"]["type"] == "str"
        assert schema["optional"]["message"]["default"] == ""
    
    def test_initialization_no_config(self):
        """Test initialization without configuration."""
        component = EchoComponent("echo1")
        assert component.get_name() == "echo1"
        assert component.get_config() == {}
    
    def test_initialization_with_message(self):
        """Test initialization with message configuration."""
        config = {"message": "test message"}
        component = EchoComponent("echo1", config)
        assert component.get_config() == config
    
    def test_invalid_message_type(self):
        """Test initialization fails with invalid message type."""
        with pytest.raises(TypeError, match="Parameter message must be of type str"):
            EchoComponent("echo1", {"message": 123})
    
    def test_execute_passthrough_single_input(self):
        """Test execute passes through single input unchanged."""
        component = EchoComponent("echo1")
        inputs = {"main": {"data": "test_data", "value": 42}}
        
        result = component.execute({}, inputs)
        assert result == inputs
    
    def test_execute_passthrough_multiple_inputs(self):
        """Test execute passes through multiple inputs unchanged."""
        component = EchoComponent("echo1")
        inputs = {
            "main": {"data": "main_data"},
            "other": {"data": "other_data"}
        }
        
        result = component.execute({}, inputs)
        assert result == inputs
    
    def test_execute_no_inputs_creates_main_none(self):
        """Test execute with no inputs creates main output with None."""
        component = EchoComponent("echo1")
        
        result = component.execute({}, {})
        assert result == {"main": None}
    
    def test_execute_preserves_data_types(self):
        """Test execute preserves all Python data types."""
        component = EchoComponent("echo1")
        inputs = {
            "main": {
                "string": "text",
                "integer": 42,
                "float": 3.14,
                "boolean": True,
                "list": [1, 2, 3],
                "dict": {"nested": "value"},
                "none": None
            }
        }
        
        result = component.execute({}, inputs)
        assert result == inputs
        # Verify types are preserved
        main_data = result["main"]
        assert isinstance(main_data["string"], str)
        assert isinstance(main_data["integer"], int)
        assert isinstance(main_data["float"], float)
        assert isinstance(main_data["boolean"], bool)
        assert isinstance(main_data["list"], list)
        assert isinstance(main_data["dict"], dict)
        assert main_data["none"] is None
    
    def test_execute_with_context(self):
        """Test execute works with context (context is ignored)."""
        component = EchoComponent("echo1")
        context = {"global_var": "global_value"}
        inputs = {"main": {"data": "test"}}
        
        result = component.execute(context, inputs)
        assert result == inputs
    
    def test_execute_with_message_config(self):
        """Test execute with message configuration (message is ignored in execution)."""
        component = EchoComponent("echo1", {"message": "Processing data"})
        inputs = {"main": {"value": 123}}
        
        result = component.execute({}, inputs)
        assert result == inputs
    
    def test_get_metadata(self):
        """Test get_metadata returns correct component metadata."""
        metadata = EchoComponent.get_metadata()
        
        assert metadata["name"] == "echo"
        assert metadata["class_name"] == "EchoComponent"
        assert metadata["category"] == "io"
        assert metadata["input_ports"] == ["main"]
        assert metadata["output_ports"] == ["main"]
        assert metadata["required_params"] == {}
        assert metadata["optional_params"]["message"]["type"] == "str"
        assert metadata["startable"] is True
        assert metadata["idempotent"] is True
    
    def test_inheritance_from_base(self):
        """Test EchoComponent inherits BaseComponent functionality."""
        component = EchoComponent("echo1")
        
        # Test inherited methods
        assert component.get_component_name() == "echo"
        assert component.get_category() == "io"
        assert component.is_startable() is True
        assert component.allows_multi_input() is False
        assert component.is_idempotent() is True