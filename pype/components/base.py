from abc import ABC, abstractmethod
from typing import Any, Dict, List, Union
import pandas as pd
import dask.dataframe as dd


class BaseComponent(ABC):
    """
    Base class for all DataPY components with simplified execution architecture.
    
    Components automatically adapt to job-level execution mode (pandas/dask) and
    receive raw DataFrames directly. Global variables managed through injected GlobalStore.
    """
    
    # Component metadata - override in subclasses
    COMPONENT_NAME: str = "base"
    CATEGORY: str = "unknown"
    INPUT_PORTS: List[str] = []
    OUTPUT_PORTS: List[str] = []
    OUTPUT_GLOBALS: List[str] = []
    DEPENDENCIES: List[str] = []
    STARTABLE: bool = False
    ALLOW_MULTI_IN: bool = False
    IDEMPOTENT: bool = True
    VERSION: str = "1.0.0"
    
    CONFIG_SCHEMA: Dict[str, Any] = {
        "required": {},  # param_name: {"type": "str", "description": "..."}
        "optional": {}   # param_name: {"type": "str", "default": value, "description": "..."}
    }
    
    def __init__(self, name: str, config: Dict[str, Any], global_store):
        """
        Initialize component with name, configuration, and global store.
        
        Args:
            name: Unique component instance name
            config: Component configuration parameters
            global_store: Thread-safe global store for job-wide variables
        """
        self.name = name
        self.config = config
        self._global_store = global_store
        self._setup_called = False
        self._cleanup_called = False
    
    @property
    def execution_mode(self) -> str:
        """
        Get the job-level execution mode from current execution context.
        
        Returns:
            "pandas" or "dask" based on job configuration
        """
        # Will be set by run() method from context
        return getattr(self, '_current_execution_mode', 'pandas')
    
    def run(
        self, 
        context: Dict[str, Any], 
        inputs: Dict[str, Union[pd.DataFrame, dd.DataFrame]]
    ) -> Dict[str, Union[pd.DataFrame, dd.DataFrame]]:
        """
        Engine-facing execution wrapper that handles complete component lifecycle.
        
        The engine always calls this method, never execute() directly.
        Handles setup, execution, and cleanup automatically.
        
        Args:
            context: Read-only execution context with metadata
            inputs: Input DataFrames keyed by port name
            
        Returns:
            Output DataFrames keyed by port name
        """
        # Store execution mode for property access
        self._current_execution_mode = context.get("execution_mode", "pandas")
        
        # Setup phase - called once per component instance
        if not self._setup_called:
            self.setup(context)
            self._setup_called = True
        
        try:
            # Execute phase - component business logic
            result = self.execute(context, inputs)
            return result
            
        finally:
            # Cleanup phase - always called, even on errors
            if not self._cleanup_called:
                self.cleanup(context)
                self._cleanup_called = True
    
    @abstractmethod
    def execute(
        self, 
        context: Dict[str, Any], 
        inputs: Dict[str, Union[pd.DataFrame, dd.DataFrame]]
    ) -> Dict[str, Union[pd.DataFrame, dd.DataFrame]]:
        """
        Execute the component logic with direct DataFrame handling.
        
        Args:
            context: Read-only execution context with metadata
            inputs: Input DataFrames keyed by port name
                   Values are pandas or dask DataFrames based on job execution_mode
            
        Returns:
            Output DataFrames keyed by port name
            Must return pandas or dask DataFrames matching job execution_mode
            
        Note:
            Use self.set_global() to update global variables thread-safely
            Context is read-only - all dynamic state goes through GlobalStore
        """
        pass
    
    def setup(self, context: Dict[str, Any]) -> None:
        """
        Optional lifecycle hook called once before first execution.
        Override only if component needs initialization (connections, files, etc.)
        
        Args:
            context: Read-only execution context with metadata
        """
        pass
    
    def cleanup(self, context: Dict[str, Any]) -> None:
        """
        Optional lifecycle hook called after component execution completes.
        Override only if component needs cleanup (close connections, temp files, etc.)
        Called even if execute() raises an exception.
        
        Args:
            context: Read-only execution context with metadata
        """
        pass
    
    def get_global(self, full_key: str, default=None):
        """
        Get global variable value thread-safely from GlobalStore.
        
        Args:
            full_key: Full global variable key (component__variable format)
            default: Default value if key not found
            
        Returns:
            Global variable value or default
            
        Example:
            count = self.get_global("upstream_component__row_count", 0)
        """
        if not self._global_store:
            return default
        return self._global_store.get(full_key, default)
    
    def set_global(self, variable_name: str, value, mode: str = "replace"):
        """
        Set global variable value thread-safely in GlobalStore.
        
        Args:
            variable_name: Variable name without component prefix (must be in OUTPUT_GLOBALS)
            value: Value to set (must be JSON serializable and <64KB)
            mode: Update mode - "replace" or "accumulate"
            
        Raises:
            ValueError: If variable_name not declared in OUTPUT_GLOBALS
            ValueError: If value exceeds 64KB or not serializable
            
        Example:
            self.set_global("row_count", 1500)  # Becomes "my_component__row_count"
        """
        # Validate against declared OUTPUT_GLOBALS
        if variable_name not in self.OUTPUT_GLOBALS:
            raise ValueError(
                f"Component '{self.name}' attempted to set undeclared global variable '{variable_name}'. "
                f"Add '{variable_name}' to OUTPUT_GLOBALS: {self.OUTPUT_GLOBALS}"
            )
        
        if not self._global_store:
            return
            
        # Engine handles component name prefix
        full_key = f"{self.name}__{variable_name}"
        self._global_store.set(full_key, value, mode=mode)
    
    def __str__(self) -> str:
        return f"{self.__class__.__name__}(name={self.name})"
    
    def __repr__(self) -> str:
        return f"{self.__class__.__name__}(name='{self.name}', mode={self.execution_mode})"