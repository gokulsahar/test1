"""
Regular executor for DataPY execution engine.
"""

import asyncio
import time
import logging
from typing import Dict, Any, Union, List

import pandas as pd
import dask.dataframe as dd

from pype.core.engine.subjob_tracker import SubJobTracker
from pype.core.engine.component_invoker import ComponentInvoker, ComponentResult, ExecutionError
from pype.core.engine.memory_manager import MemoryManager


class RegularExecutor:
   def __init__(self, component_invoker: ComponentInvoker, memory_manager: MemoryManager,
                execution_metadata: Dict[str, Any], logger: logging.Logger):
       self.component_invoker = component_invoker
       self.memory_manager = memory_manager
       self.logger = logger
       self.component_dependencies = execution_metadata['component_dependencies']
       self.port_mapping = execution_metadata['port_mapping']
   
   async def execute_regular_subjob(self, subjob_id: str, component_execution_order: List[str],
                                  context: Dict[str, Any], subjob_tracker: SubJobTracker,
                                  fired_tokens: set, global_store) -> None:
       component_outputs = {}
       edge_remaining = self.memory_manager.init_edge_reference_counts(component_execution_order)
       
       for component_name in component_execution_order:
           if not self._can_component_execute(component_name, component_outputs, fired_tokens):
               self.logger.warning(
                   "COMPONENT_NOT_READY",
                   extra={"component": component_name, "subjob_id": subjob_id}
               )
               continue
           
           subjob_tracker.notify_component_start(component_name)
           
           try:
               inputs = self._prepare_component_inputs(component_name, component_outputs)
               result = await self.component_invoker.execute_component(
                   component_name, context, inputs, global_store
               )
               
               if result.success:
                   component_outputs[component_name] = result.outputs
                   self.memory_manager.update_edge_reference_counts(
                       component_name, edge_remaining, component_outputs
                   )
                   subjob_tracker.notify_component_success(component_name)
               else:
                   subjob_tracker.notify_component_failure(component_name)
                   await self._handle_component_error(component_name, result.error, subjob_tracker)
               
           except (ExecutionError, TimeoutError) as e:
               subjob_tracker.notify_component_failure(component_name)
               await self._handle_component_error(component_name, e, subjob_tracker)
           
           self.memory_manager.maybe_collect_garbage("component_execution")
   
   def _can_component_execute(self, component_name: str, component_outputs: Dict[str, Dict[str, Any]], 
                            fired_tokens: set) -> bool:
       deps = self.component_dependencies[component_name]
       data_deps = deps['data']
       data_ready = all(dep in component_outputs for dep in data_deps)
       
       control_deps = deps['control']
       control_ready = all(f"{dep}::ok" in fired_tokens for dep in control_deps)
       
       port_info = self.port_mapping[component_name]
       inputs = port_info['inputs']
       data_available = all(
           source_comp in component_outputs and 
           any(port_name in component_outputs[source_comp] for port_name, _ in inputs)
           for port_name, source_comp in inputs
       )
       
       return data_ready and control_ready and data_available
   
   def _prepare_component_inputs(self, component_name: str, 
                               component_outputs: Dict[str, Dict[str, Any]]) -> Dict[str, Union[pd.DataFrame, dd.DataFrame]]:
       inputs = {}
       port_info = self.port_mapping[component_name]
       
       for port_name, source_component in port_info['inputs']:
           if source_component in component_outputs:
               source_outputs = component_outputs[source_component]
               
               for output_port, data in source_outputs.items():
                   if output_port == port_name or output_port == 'main':
                       if port_name in inputs:
                           if not isinstance(inputs[port_name], list):
                               inputs[port_name] = [inputs[port_name]]
                           inputs[port_name].append(data)
                       else:
                           inputs[port_name] = data
                       break
       
       return inputs
   
   async def _handle_component_error(self, component_name: str, error: Exception, 
                                    subjob_tracker: SubJobTracker) -> None:
       subjob_tracker.failed = True
       raise ExecutionError(component_name, str(error), error)