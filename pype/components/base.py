from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional, Union


class BaseComponent(ABC):
    """Base class for all DataPY components."""
    
    # Component metadata - override in subclasses
    COMPONENT_NAME: str = "base"
    CATEGORY: str = "unknown"
    INPUT_PORTS: List[str] = []
    OUTPUT_PORTS: List[str] = []
    OUTPUT_GLOBALS: List[str] = []
    DEPENDENCIES: List[str] = []
    STARTABLE: bool = False
    EVENTS: List[str] = ["ok", "error"]
    ALLOW_MULTI_IN: bool = False
    IDEMPOTENT: bool = True
    
    CONFIG_SCHEMA: Dict[str, Any] = {
        "required": {},  # param_name: {"type": SomeType, "description": "..."}
        "optional": {}   # param_name: {"type": SomeType, "default": value, "description": "..."}
    }
    
    def __init__(self, name: str, config: Optional[Dict[str, Any]] = None):
        """Initialize component with name and configuration."""
        self.name = name
        self.config = config or {}
        self._validate_config()
    
    def _validate_config(self) -> None:
        """Validate component configuration against schema."""
        schema = self.CONFIG_SCHEMA
        required = schema.get("required", {})
        optional = schema.get("optional", {})
        
        # Check required parameters
        for param_name, param_spec in required.items():
            if param_name not in self.config:
                raise ValueError(f"Missing required parameter: {param_name}")
            
            # Type checking for required parameters
            expected_type = param_spec.get("type")
            if expected_type and not isinstance(self.config[param_name], expected_type):
                raise TypeError(f"Required parameter {param_name} must be of type {expected_type.__name__}")
        
        # Validate optional parameter types
        for param_name, value in self.config.items():
            if param_name in optional:
                param_spec = optional[param_name]
                expected_type = param_spec.get("type")
                if expected_type and not isinstance(value, expected_type):
                    raise TypeError(f"Parameter {param_name} must be of type {expected_type.__name__}")
    
    @abstractmethod
    def execute(self, context: Dict[str, Any], inputs: Dict[str, Any]) -> Dict[str, Any]:
        """Execute the component logic.
        
        Args:
            context: Execution context with global variables and metadata
            inputs: Input data from connected upstream components
            
        Returns:
            Dict with output data for downstream components
        """
        pass
    
    #getters
    def get_name(self) -> str:
        """Get component instance name."""
        return self.name
    
    def get_component_name(self) -> str:
        """Get component type name."""
        return self.COMPONENT_NAME
    
    def get_category(self) -> str:
        """Get component category."""
        return self.CATEGORY
    
    def get_description(self) -> str:
        """Get component description."""
        return self.__doc__.strip() if self.__doc__ else ""
    
    def get_config(self) -> Dict[str, Any]:
        """Get component configuration."""
        return self.config.copy()
    
    def get_input_ports(self) -> List[str]:
        """Get list of input port names."""
        return self.INPUT_PORTS.copy()
    
    def get_output_ports(self) -> List[str]:
        """Get list of output port names."""
        return self.OUTPUT_PORTS.copy()
    
    def get_output_globals(self) -> List[str]:
        """Get list of global variables this component outputs."""
        return self.OUTPUT_GLOBALS.copy()
    
    def get_dependencies(self) -> List[str]:
        """Get list of component dependencies."""
        return self.DEPENDENCIES.copy()
    
    def get_events(self) -> List[str]:
        """Get list of event names this component can emit."""
        return self.EVENTS.copy()
    
    def get_config_schema(self) -> Dict[str, Any]:
        """Get component configuration schema."""
        return {
            "required": self.CONFIG_SCHEMA.get("required", {}).copy(),
            "optional": self.CONFIG_SCHEMA.get("optional", {}).copy()
        }
    
    def is_startable(self) -> bool:
        """Check if component can be a starting point in the pipeline."""
        return self.STARTABLE
    
    def allows_multi_input(self) -> bool:
        """Check if component allows multiple input connections."""
        return self.ALLOW_MULTI_IN
    
    def is_idempotent(self) -> bool:
        """Check if component is idempotent (safe to retry)."""
        return self.IDEMPOTENT
    
    @classmethod
    def get_metadata(cls) -> Dict[str, Any]:
        """Get component metadata for registry registration."""
        return {
            "name": cls.COMPONENT_NAME,
            "class_name": cls.__name__,
            "module_path": cls.__module__,
            "category": cls.CATEGORY,
            "description": cls.__doc__.strip() if cls.__doc__ else "", #""" the doc string in the component"""
            "input_ports": cls.INPUT_PORTS,
            "output_ports": cls.OUTPUT_PORTS,
            "required_params": cls.CONFIG_SCHEMA.get("required", {}),
            "optional_params": cls.CONFIG_SCHEMA.get("optional", {}),
            "output_globals": cls.OUTPUT_GLOBALS,
            "dependencies": cls.DEPENDENCIES,
            "startable": cls.STARTABLE,
            "events": cls.EVENTS,
            "allow_multi_in": cls.ALLOW_MULTI_IN,
            "idempotent": cls.IDEMPOTENT
        }
    
    def __str__(self) -> str:
        return f"{self.__class__.__name__}(name={self.name})"
    
    def __repr__(self) -> str:
        return f"{self.__class__.__name__}(name='{self.name}', config={self.config})"