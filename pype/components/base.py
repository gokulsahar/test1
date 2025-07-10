from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional
from pype.core.engine.pipeline_data import PipelineData


class BaseComponent(ABC):
    """
    Base class for all DataPY components.
    
    All components must override execute(). Optional lifecycle hooks:
    - setup(): Called once before first execution  
    - cleanup(): Called after component completes
    """
    
    # Component metadata - override in subclasses
    COMPONENT_NAME: str = "base"
    VERSION: str = "1.0.0"  # Required: Semantic versioning for rebuild detection
    CATEGORY: str = "unknown"
    INPUT_PORTS: List[str] = []
    OUTPUT_PORTS: List[str] = []
    OUTPUT_GLOBALS: List[str] = []
    DEPENDENCIES: List[str] = []
    STARTABLE: bool = False
    ALLOW_MULTI_IN: bool = False
    IDEMPOTENT: bool = True
    
    CONFIG_SCHEMA: Dict[str, Any] = {
        "required": {},  # param_name: {"type": "str", "description": "..."}
        "optional": {}   # param_name: {"type": "str", "default": value, "description": "..."}
    }
    
    def __init__(self, name: str, config: Optional[Dict[str, Any]] = None):
        """Initialize component with name and configuration."""
        self.name = name
        self.config = config or {}
        self._validate_config()
        self._setup_called = False
        self._cleanup_called = False
    
    def _validate_config(self) -> None:
        """Validate component configuration against schema."""
        schema = self.CONFIG_SCHEMA
        required = schema.get("required", {})
        optional = schema.get("optional", {})
        
        # Type mapping for data type checks for params
        type_mapping = {
            "str": str,
            "int": int,
            "float": float,
            "bool": bool,
            "list": list,
            "dict": dict
        }
        
        # Check required parameters
        for param_name, param_spec in required.items():
            if param_name not in self.config:
                raise ValueError(f"Missing required parameter: {param_name}")
            
            # Type checking for required parameters
            expected_type_str = param_spec.get("type")
            if expected_type_str in type_mapping:
                expected_type = type_mapping[expected_type_str]
                if not isinstance(self.config[param_name], expected_type):
                    raise TypeError(f"Required parameter {param_name} must be of type {expected_type_str}")
        
        # Validate optional parameter types
        for param_name, value in self.config.items():
            if param_name in optional:
                param_spec = optional[param_name]
                expected_type_str = param_spec.get("type")
                if expected_type_str in type_mapping:
                    expected_type = type_mapping[expected_type_str]
                    if not isinstance(value, expected_type):
                        raise TypeError(f"Parameter {param_name} must be of type {expected_type_str}")
    
    # === LIFECYCLE HOOKS (Optional - Override if needed) ===
    
    def setup(self, context: Dict[str, Any]) -> None:
        """
        Optional lifecycle hook called once before first execution.
        
        Args:
            context: Execution context with run metadata and globals
        """
        pass
    
    def cleanup(self, context: Dict[str, Any]) -> None:
        """
        Optional lifecycle hook called after component execution completes.
        
        Args:
            context: Execution context with run metadata and globals
        """
        pass
    
    # === CORE EXECUTION (Must Override) ===
    
    @abstractmethod
    def execute(self, context: Dict[str, Any], inputs: Dict[str, PipelineData]) -> Dict[str, PipelineData]:
        """
        Execute the component logic with PipelineData contract.
        
        Args:
            context: Execution context with global variables and metadata
            inputs: Input PipelineData from connected upstream components
                   Keys are port names, values are PipelineData instances
            
        Returns:
            Dict with PipelineData outputs for downstream components
            Keys are port names, values are PipelineData instances
        """
        pass
    
    # === ENGINE INTEGRATION METHODS (Do not override) ===
    
    def _execute_with_lifecycle(self, context: Dict[str, Any], inputs: Dict[str, PipelineData]) -> Dict[str, PipelineData]:
        """
        Engine-facing execution wrapper that handles lifecycle hooks.
        
        This method is called by the engine, not by component developers.
        """
        # Call setup hook if not already called
        if not self._setup_called:
            self.setup(context)
            self._setup_called = True
        
        # Execute component logic
        outputs = self.execute(context, inputs)
        
        return outputs
    
    def _cleanup_component(self, context: Dict[str, Any]) -> None:
        """
        Engine-facing cleanup wrapper.
        
        This method is called by the engine during component lifecycle cleanup.
        """
        if not self._cleanup_called:
            self.cleanup(context)
            self._cleanup_called = True
    
    # === HELPER METHODS ===
    
    def _wrap_raw_data(self, data: Any, source: Optional[str] = None) -> PipelineData:
        """
        Helper method to wrap raw data in PipelineData.
        
        Args:
            data: Raw data to wrap
            source: Optional source identifier
            
        Returns:
            PipelineData instance
        """
        return PipelineData(
            data=data,
            source=source or f"{self.name}_{self.COMPONENT_NAME}"
        )
    
    def _extract_raw_data(self, pipeline_data: PipelineData) -> Any:
        """
        Helper method to extract raw data from PipelineData.
        
        Args:
            pipeline_data: PipelineData instance
            
        Returns:
            Raw underlying data
        """
        return pipeline_data.get_raw_data()
    
    def __str__(self) -> str:
        return f"{self.__class__.__name__}(name={self.name})"
    
    def __repr__(self) -> str:
        return f"{self.__class__.__name__}(name='{self.name}', config={self.config})"