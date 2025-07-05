import pytest
import pandas as pd
from pype.components.io.echo import EchoComponent
from pype.core.engine.pipeline_data import PipelineData, DataType


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
    
    def test_execute_passthrough_pandas_data(self):
        """Test execute passes through pandas DataFrame unchanged."""
        component = EchoComponent("echo1")
        
        # Create test DataFrame
        test_df = pd.DataFrame({"col1": [1, 2, 3], "col2": ["a", "b", "c"]})
        pipeline_data = PipelineData(test_df, source="test_source")
        inputs = {"main": pipeline_data}
        
        result = component.execute({}, inputs)
        
        # Verify output structure
        assert "main" in result
        assert isinstance(result["main"], PipelineData)
        
        # Verify data content is preserved
        output_df = result["main"].to_pandas()
        pd.testing.assert_frame_equal(output_df, test_df)
        
        # Verify metadata is updated
        assert result["main"].source == "echo1_echo_passthrough"
    
    def test_execute_passthrough_scalar_data(self):
        """Test execute passes through scalar data unchanged."""
        component = EchoComponent("echo1")
        
        scalar_data = PipelineData(42, source="test_scalar")
        inputs = {"main": scalar_data}
        
        result = component.execute({}, inputs)
        
        assert "main" in result
        assert isinstance(result["main"], PipelineData)
        assert result["main"].get_raw_data() == 42
        assert result["main"].data_type == DataType.SCALAR
    
    def test_execute_passthrough_collection_data(self):
        """Test execute passes through collection data unchanged."""
        component = EchoComponent("echo1")
        
        test_list = [1, 2, 3, {"key": "value"}]
        collection_data = PipelineData(test_list, source="test_collection")
        inputs = {"main": collection_data}
        
        result = component.execute({}, inputs)
        
        assert "main" in result
        assert isinstance(result["main"], PipelineData)
        assert result["main"].get_raw_data() == test_list
        assert result["main"].data_type == DataType.COLLECTION
    
    def test_execute_passthrough_multiple_inputs(self):
        """Test execute passes through multiple inputs unchanged."""
        component = EchoComponent("echo1")
        
        inputs = {
            "main": PipelineData({"data": "main_data"}, source="main_source"),
            "other": PipelineData({"data": "other_data"}, source="other_source")
        }
        
        result = component.execute({}, inputs)
        
        assert len(result) == 2
        assert "main" in result
        assert "other" in result
        
        # Verify both outputs are PipelineData
        for port_name, output_data in result.items():
            assert isinstance(output_data, PipelineData)
            assert output_data.source == "echo1_echo_passthrough"
    
    def test_execute_no_inputs_creates_main_none(self):
        """Test execute with no inputs creates main output with None."""
        component = EchoComponent("echo1")
        
        result = component.execute({}, {})
        
        assert "main" in result
        assert isinstance(result["main"], PipelineData)
        assert result["main"].get_raw_data() is None
        assert result["main"].data_type == DataType.SCALAR
        assert result["main"].source == "echo1_echo_empty"
    
    def test_execute_preserves_pipeline_data_metadata(self):
        """Test execute preserves PipelineData metadata."""
        component = EchoComponent("echo1")
        
        # Create PipelineData with rich metadata
        test_df = pd.DataFrame({"id": [1, 2], "name": ["Alice", "Bob"]})
        pipeline_data = PipelineData(
            test_df,
            schema={"id": "int64", "name": "object"},
            source="upstream_component",
            notes="Test data with metadata"
        )
        inputs = {"main": pipeline_data}
        
        result = component.execute({}, inputs)
        
        output_data = result["main"]
        assert output_data.schema == {"id": "int64", "name": "object"}
        assert output_data.notes == "Test data with metadata"
        assert output_data.row_count == 2
        # Source should be updated by echo component
        assert output_data.source == "echo1_echo_passthrough"
    
    def test_execute_with_context(self):
        """Test execute works with context (context is ignored)."""
        component = EchoComponent("echo1")
        context = {"global_var": "global_value"}
        
        test_data = PipelineData("test", source="test")
        inputs = {"main": test_data}
        
        result = component.execute(context, inputs)
        
        assert "main" in result
        assert isinstance(result["main"], PipelineData)
        assert result["main"].get_raw_data() == "test"
    
    def test_execute_with_message_config(self):
        """Test execute with message configuration (message is ignored in execution)."""
        component = EchoComponent("echo1", {"message": "Processing data"})
        
        test_data = PipelineData(123, source="test")
        inputs = {"main": test_data}
        
        result = component.execute({}, inputs)
        
        assert "main" in result
        assert result["main"].get_raw_data() == 123
    
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
        
    def test_helper_methods(self):
        """Test BaseComponent helper methods work correctly."""
        component = EchoComponent("echo1")
        
        # Test _wrap_raw_data
        raw_data = {"test": "data"}
        wrapped = component._wrap_raw_data(raw_data, "test_source")
        assert isinstance(wrapped, PipelineData)
        assert wrapped.get_raw_data() == raw_data
        assert wrapped.source == "test_source"
        
        # Test _extract_raw_data
        extracted = component._extract_raw_data(wrapped)
        assert extracted == raw_data