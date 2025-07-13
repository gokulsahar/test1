"""
Simple test component for DataPY testing and validation.

Author: DataPY Team
Version: 1.0.0
Category: utility
"""

from typing import Dict, Any, Union
import pandas as pd
import dask.dataframe as dd
from pype.components.base import BaseComponent


class TestEchoComponent(BaseComponent):
    """
    Simple test component that echoes a message and creates sample data.
    
    This component is designed for testing DataPY functionality without
    external dependencies. It creates sample data and logs messages.
    """
    
    COMPONENT_NAME = "test_echo"
    VERSION = "1.0.0"
    CATEGORY = "utility"
    INPUT_PORTS = ["main"]  # Can accept input data
    OUTPUT_PORTS = ["main"]
    OUTPUT_GLOBALS = ["row_count", "message"]
    DEPENDENCIES = []
    STARTABLE = True  # Can start the job (if no incoming edges)
    ALLOW_MULTI_IN = False
    IDEMPOTENT = True
    
    CONFIG_SCHEMA = {
        "required": {
            "message": {"type": "str", "description": "Message to echo"}
        },
        "optional": {
            "row_count": {"type": "int", "default": 5, "description": "Number of sample rows to generate"},
            "delay_seconds": {"type": "int", "default": 1, "description": "Artificial delay in seconds"}
        }
    }
    
    def execute(self, context: Dict[str, Any], 
                inputs: Dict[str, Union[pd.DataFrame, dd.DataFrame]]) -> Dict[str, Union[pd.DataFrame, dd.DataFrame]]:
        """
        Execute the test echo component.
        
        Creates sample data and sets global variables for testing.
        Can work with or without input data.
        """
        # Get configuration
        message = self.config["message"]
        row_count = self.config.get("row_count", 5)
        delay_seconds = self.config.get("delay_seconds", 1)
        
        # Simulate some work
        import time
        time.sleep(delay_seconds)
        
        # Check if we have input data
        input_data = inputs.get("main") if inputs else None
        input_row_count = 0
        
        if input_data is not None:
            # Process input data
            if self.execution_mode == "pandas":
                input_row_count = len(input_data)
            else:  # dask mode
                try:
                    input_row_count = len(input_data)  # This works for known divisions
                except:
                    input_row_count = 0  # Fallback for unknown divisions
        
        # Create sample data based on execution mode
        if self.execution_mode == "pandas":
            sample_data = pd.DataFrame({
                "id": range(1, row_count + 1),
                "message": [f"{message} - Row {i}" for i in range(1, row_count + 1)],
                "timestamp": [pd.Timestamp.now()] * row_count,
                "input_rows_processed": [input_row_count] * row_count
            })
        else:  # dask mode
            import dask.dataframe as dd
            sample_data = pd.DataFrame({
                "id": range(1, row_count + 1),
                "message": [f"{message} - Row {i}" for i in range(1, row_count + 1)],
                "timestamp": [pd.Timestamp.now()] * row_count,
                "input_rows_processed": [input_row_count] * row_count
            })
            sample_data = dd.from_pandas(sample_data, npartitions=2)
        
        # Set global variables for testing
        self.set_global("row_count", row_count)
        self.set_global("message", message)
        
        # Print for debugging
        print(f"TestEcho [{self.name}]: {message} (created {row_count} rows, processed {input_row_count} input rows)")
        
        return {"main": sample_data}