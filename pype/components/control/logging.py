import time
from typing import Dict, Any
from pype.components.base import BaseComponent
from pype.core.engine.pipeline_data import PipelineData, DataType


class LoggingComponent(BaseComponent):
    """
    Simple logging component that logs messages and passes data through.
    Useful for debugging and creating test subjobs with control edges.
    """
    
    COMPONENT_NAME = "logging"
    CATEGORY = "control"
    INPUT_PORTS = ["main"]
    OUTPUT_PORTS = ["main"]
    OUTPUT_GLOBALS = ["log_count", "log_timestamp"]
    DEPENDENCIES = []
    STARTABLE = True
    ALLOW_MULTI_IN = False
    IDEMPOTENT = True
    
    CONFIG_SCHEMA = {
        "required": {},
        "optional": {
            "message": {
                "type": "str",
                "default": "Processing data",
                "description": "Message to log during execution"
            },
            "level": {
                "type": "str", 
                "default": "INFO",
                "description": "Log level (DEBUG, INFO, WARNING, ERROR)"
            },
            "delay_seconds": {
                "type": "int",
                "default": 0,
                "description": "Artificial delay to simulate processing time"
            },
            "fail_condition": {
                "type": "str",
                "default": "",
                "description": "Condition to simulate failure (e.g., 'row_count < 10')"
            }
        }
    }
    
    def execute(self, context: Dict[str, Any], inputs: Dict[str, PipelineData]) -> Dict[str, PipelineData]:
        """
        Execute logging component with optional delay and failure simulation.
        
        Args:
            context: Execution context with global variables and metadata
            inputs: Input PipelineData from connected upstream components
            
        Returns:
            Dict with PipelineData outputs for downstream components
        """
        # Get configuration
        message = self.config.get("message", "Processing data")
        level = self.config.get("level", "INFO")
        delay_seconds = self.config.get("delay_seconds", 0)
        fail_condition = self.config.get("fail_condition", "")
        
        # Get input data (if any)
        input_data = inputs.get("main") if inputs else None
        
        # Extract context variables for message formatting
        context_vars = {
            "component_name": self.name,
            "run_id": context.get("run_id", "unknown"),
            "timestamp": time.strftime("%Y-%m-%d %H:%M:%S")
        }
        
        # Add data information if available
        if input_data:
            if input_data.data_type in (DataType.PANDAS, DataType.DASK):
                context_vars["row_count"] = input_data.row_count or 0
                context_vars["columns"] = len(input_data.schema) if input_data.schema else 0
            else:
                context_vars["data_type"] = input_data.data_type.value
        else:
            context_vars["row_count"] = 0
            context_vars["columns"] = 0
        
        # Format message with context variables
        try:
            formatted_message = message.format(**context_vars)
        except (KeyError, ValueError):
            formatted_message = message
        
        # Log the message (in real implementation, this would use proper logging)
        print(f"[{level}] {self.name}: {formatted_message}")
        
        # Simulate processing delay
        if delay_seconds > 0:
            print(f"[DEBUG] {self.name}: Simulating {delay_seconds}s processing delay...")
            time.sleep(delay_seconds)
        
        # Check failure condition
        if fail_condition:
            try:
                # Simple evaluation of failure conditions
                if self._evaluate_condition(fail_condition, context_vars):
                    raise RuntimeError(f"Simulated failure: {fail_condition} evaluated to True")
            except Exception as e:
                if "Simulated failure" in str(e):
                    raise
                # Ignore evaluation errors and continue
                pass
        
        # Prepare output data
        if input_data:
            # Pass through input data with updated source
            output_data = input_data.clone_with_data(
                input_data.get_raw_data(),
                source=f"{self.name}_logged"
            )
        else:
            # Create new data with logging info
            log_info = {
                "component": self.name,
                "message": formatted_message,
                "timestamp": context_vars["timestamp"]
            }
            output_data = self._wrap_raw_data(log_info, source=f"{self.name}_generated")
        
        # Set global variables
        context["globals"] = context.get("globals", {})
        context["globals"][f"{self.name}__log_count"] = context_vars.get("row_count", 1)
        context["globals"][f"{self.name}__log_timestamp"] = context_vars["timestamp"]
        
        return {"main": output_data}
    
    def _evaluate_condition(self, condition: str, variables: Dict[str, Any]) -> bool:
        """
        Simple condition evaluator for failure simulation.
        
        Args:
            condition: String condition like "row_count < 10"
            variables: Available variables for evaluation
            
        Returns:
            Boolean result of condition evaluation
        """
        # Replace variables in condition
        for var, value in variables.items():
            condition = condition.replace(var, str(value))
        
        # Simple safe evaluation (only basic operators)
        try:
            # Only allow basic comparison operators for safety
            allowed_ops = ['<', '>', '<=', '>=', '==', '!=']
            if any(op in condition for op in allowed_ops):
                return eval(condition)
            return False
        except:
            return False