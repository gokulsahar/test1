from typing import Dict
from pype.components.base import BaseComponent
from pype.core.engine.pipeline_data import PipelineData


class EchoComponent(BaseComponent):
   """Echo component that passes through all input data unchanged using PipelineData contract."""
   
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
   
   def execute(self, context: Dict[str, any], inputs: Dict[str, PipelineData]) -> Dict[str, PipelineData]:
       """Execute the echo component by passing through all inputs unchanged.
       
       Args:
           context: Execution context with global variables and metadata
           inputs: Input PipelineData from connected upstream components
           
       Returns:
           Dict with the same PipelineData as inputs for downstream components
       """
       # Optional message logging could be handled by orchestrator
       message = self.config.get("message", "")
       if message:
           # In a real implementation, this would use the framework's logging
           pass
       
       # Pass through all inputs unchanged
       outputs = {}
       for port_name, pipeline_data in inputs.items():
           # Clone the PipelineData with updated source information
           outputs[port_name] = pipeline_data.clone_with_data(
               pipeline_data.get_raw_data(),
               source=f"{self.name}_echo_passthrough"
           )
       
       # If no inputs provided, create empty main output with None
       if not outputs and "main" in self.OUTPUT_PORTS:
           outputs["main"] = self._wrap_raw_data(
               None, 
               source=f"{self.name}_echo_empty"
           )
           
       return outputs