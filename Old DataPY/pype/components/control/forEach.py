"""
forEach iteration component for DataPY ETL framework.

Author: DataPY Team
Version: 1.0.0
Category: control
"""

from typing import Dict, Any, Union
import pandas as pd
import dask.dataframe as dd
from pype.components.base import BaseComponent


class ForEachComponent(BaseComponent):
    """forEach iteration component that processes DataFrame rows one at a time."""
    
    COMPONENT_NAME = "forEach"
    VERSION = "1.0.0"
    CATEGORY = "control"
    INPUT_PORTS = ["main"]
    OUTPUT_PORTS = ["item"]
    OUTPUT_GLOBALS = ["current_item", "current_index", "total_items", "iteration_complete"]
    DEPENDENCIES = []
    STARTABLE = False
    ALLOW_MULTI_IN = False
    IDEMPOTENT = False
    
    CONFIG_SCHEMA = {
        "required": {},
        "optional": {
            "max_iterations": {"type": "int", "default": None, "description": "Maximum iterations"},
            "batch_size": {"type": "int", "default": 1, "description": "Rows per iteration"}
        }
    }
    
    def __init__(self, name: str, config: Dict[str, Any], global_store):
        super().__init__(name, config, global_store)
        self._reset_state()
    
    def _reset_state(self):
        """Reset iteration state."""
        self._current_index = 0
        self._total_items = 0
        self._input_data = None
        self._is_complete = False
    
    def reset(self):
        """Public reset method for orchestrator."""
        self._reset_state()
    
    def execute(self, context: Dict[str, Any], 
                inputs: Dict[str, Union[pd.DataFrame, dd.DataFrame]]) -> Dict[str, Union[pd.DataFrame, dd.DataFrame]]:
        """Execute forEach iteration, returning batch of rows."""
        
        input_data = inputs.get("main")
        if input_data is None:
            return self._complete_iteration()
        
        # Initialize on first call
        if self._input_data is None:
            self._initialize_iteration(input_data)
        
        # Check completion
        if self._is_complete:
            return self._complete_iteration()
        
        # Get current batch
        current_batch = self._get_current_batch()
        if current_batch is None or len(current_batch) == 0:
            return self._complete_iteration()
        
        # Update state
        batch_size = self.config.get("batch_size", 1)
        self._current_index += batch_size
        self.set_global("current_index", self._current_index)
        self.set_global("current_item", self._serialize_item(current_batch))
        
        return {"item": current_batch}
    
    def _initialize_iteration(self, input_data):
        """Initialize iteration with input data."""
        # Convert dask to pandas for simplicity
        if self.execution_mode == "dask":
            self._input_data = input_data.compute()
        else:
            self._input_data = input_data
        
        # Calculate total items with max_iterations clamp
        actual_items = len(self._input_data)
        max_iterations = self.config.get("max_iterations")
        
        if max_iterations is not None:
            self._total_items = min(actual_items, max_iterations)
        else:
            self._total_items = actual_items
        
        # Set initial globals
        self.set_global("total_items", self._total_items)
        self.set_global("current_index", 0)
        self.set_global("iteration_complete", False)
    
    def _get_current_batch(self):
        """Get current batch."""
        batch_size = self.config.get("batch_size", 1)
        max_iterations = self.config.get("max_iterations")
        
        # Check limits
        if max_iterations is not None and self._current_index >= max_iterations:
            return None
        
        if self._current_index >= len(self._input_data):
            return None
        
        # Get batch
        end_index = min(
            self._current_index + batch_size,
            len(self._input_data)
        )
        
        if max_iterations is not None:
            end_index = min(end_index, max_iterations)
        
        return self._input_data.iloc[self._current_index:end_index].copy()
    
    def _serialize_item(self, item):
        """Serialize item for global storage."""
        try:
            if len(item) > 0:
                # Use first row as representative
                first_row = item.iloc[0].to_dict()
                # Convert to JSON-serializable types
                serializable = {}
                for key, value in first_row.items():
                    if pd.isna(value):
                        serializable[key] = None
                    elif hasattr(value, 'item'):  # numpy types
                        serializable[key] = value.item()
                    else:
                        serializable[key] = value
                return serializable
            return {}
        except:
            return {}
    
    def _complete_iteration(self):
        """Complete iteration."""
        self._is_complete = True
        self.set_global("iteration_complete", True)
        
        # TODO: BufferedStore.flush() integration point
        
        return {"item": None}
    
    def cleanup(self, context: Dict[str, Any]) -> None:
        """Clean up resources."""
        self._reset_state()


# TODO: Engine Integration Points for forEach Component
#
# 1. BufferedStore Integration (global_store.py):
#    - ForEach components should receive BufferedStore instance
#    - BufferedStore.flush() called at iteration completion
#    - Nested forEach components should have nested BufferedStore hierarchy
#
# 2. Orchestrator Integration (orchestrator.py):
#    - execute_subjob() needs forEach-aware execution logic
#    - NEW: execute_forEach_loop(forEach_comp, iterator_metadata)
#    - Handle recursive forEach execution for nested components
#
# 3. ExecutionManager Integration (execution_manager.py):
#    - create_component_instance() should inject BufferedStore for forEach scope
#    - Track forEach iteration state across nested loops
#    - Manage DataFrame passing between forEach iterations
#
# 4. Required Engine Methods:
#    async def execute_forEach_subjob(self, subjob_id, iterator_metadata):
#        """Execute subjob containing forEach components with recursive nesting"""
#    
#    async def _execute_nested_forEach(self, forEach_comp, iterator_metadata):
#        """Recursive execution of nested forEach components"""
#    
#    def _inject_buffered_store(self, component, forEach_scope):
#        """Inject BufferedStore for components in forEach scope"""