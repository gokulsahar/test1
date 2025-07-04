from typing import Any, Dict
from pype.components.base import BaseComponent


class EchoComponent(BaseComponent):
   """Echo component that passes through all input data unchanged."""
   
   COMPONENT_NAME = "echo"
   CATEGORY = "io"
   INPUT_PORTS = ["main"]
   OUTPUT_PORTS = ["main"]
   OUTPUT_GLOBALS = []
   DEPENDENCIES = []
   STARTABLE = True
   EVENTS = ["ok", "error"]
   ALLOW_MULTI_IN = False
   IDEMPOTENT = True
   
   CONFIG_SCHEMA = {
       "required": {},
       "optional": {
           "message": {
               "type": "str",
               "default": "",
               "description": "Optional message to log during execution"
           }
       }
   }
   
   def execute(self, context: Dict[str, Any], inputs: Dict[str, Any]) -> Dict[str, Any]:
       """Execute the echo component by passing through all inputs unchanged.
       
       Args:
           context: Execution context with global variables and metadata
           inputs: Input data from connected upstream components
           
       Returns:
           Dict with the same data as inputs for downstream components
       """
       # Optional message logging could be handled by orchestrator
       message = self.config.get("message", "")
       if message:
           # In a real implementation, this would use the framework's logging
           pass
       
       # Pass through all inputs unchanged
       outputs = {}
       for port_name, data in inputs.items():
           outputs[port_name] = data
       
       # If no inputs provided, create empty main output
       if not outputs and "main" in self.OUTPUT_PORTS:
           outputs["main"] = None
           
       return outputs