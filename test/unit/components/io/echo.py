import pytest
from typing import Dict, Any
from pype.components.io.echo import EchoComponent


class TestEchoComponent:
    
    def test_component_metadata(self):
        """Test EchoComponent metadata attributes."""
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
    
    def test_initialization_no_config(self):
        """Test EchoComponent initialization without config."""
        component = EchoComponent("echo1")
        assert component.get_name() == "echo1"
        assert component.get_config() == {}
    
    def test_initialization_with_message(self):
        """Test EchoComponent initialization with message config."""
        config = {"message": "test message"}
        component = EchoComponent("echo1", config)
        assert component.get_config() == config
    
    def test_execute_with_inputs(self):
        """Test execute method passes through inputs unchanged."""
        component = EchoComponent("echo1")
        context = {"global_var": "global_value"}
        inputs = {"main": {"data": "test_data", "count": 42}}
        
        result = component.execute(context, inputs)
        assert result == inputs
    
    def test_execute_multiple_inputs(self):
        """Test execute method with multiple input ports."""
        component = EchoComponent("echo1")
        context = {}
        inputs = {
            "main": {"data": "main_data"},
            "secondary": {"data": "secondary_data"}
        }
        
        result = component.execute(context, inputs)
        assert result == inputs
    
    def test_execute_no_inputs(self):
        """Test execute method with no inputs creates main output."""
        component = EchoComponent("echo1")
        context = {}
        inputs = {}
        
        result = component.execute(context, inputs)
        assert result == {"main": None}
    
    def test_execute_with_message_config(self):
        """Test execute method with message configuration."""
        config = {"message": "Processing data"}
        component = EchoComponent("echo1", config)
        context = {}
        inputs = {"main": {"value": 123}}
        
        result = component.execute(context, inputs)
        assert result == inputs
    
    def test_get_metadata(self):
        """Test get_metadata returns correct component metadata."""
        metadata = EchoComponent.get_metadata()
        
        expected_required = {}
        expected_optional = {
            "message": {
                "type": "str",
                "default": "",
                "description": "Optional message to log during execution"
            }
        }
        
        assert metadata["name"] == "echo"
        assert metadata["class_name"] == "EchoComponent"
        assert metadata["category"] == "io"
        assert metadata["required_params"] == expected_required
        assert metadata["optional_params"] == expected_optional