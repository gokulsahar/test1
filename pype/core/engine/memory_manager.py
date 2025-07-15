"""
Memory management for DataPY execution engine.
"""

import gc
import os
import psutil
import logging
from typing import Dict, Any, List, Union

import pandas as pd
import dask.dataframe as dd


class MemoryManager:
   """Manages memory optimization during job execution."""
   
   def __init__(self, execution_metadata: Dict[str, Any], logger: logging.Logger):
       self.logger = logger
       memory_mgmt = execution_metadata.get('memory_management', {})
       self.gc_threshold_mb = memory_mgmt.get('gc_threshold_mb', 256)
       self.rss_soft_limit_ratio = memory_mgmt.get('rss_soft_limit_ratio', 0.70)
       self.base_edge_use_counts = memory_mgmt.get('edge_use_counts', {})
       self._init_memory_tracking()
   
   def _init_memory_tracking(self) -> None:
       try:
           process = psutil.Process(os.getpid())
           self.rss_prev = process.memory_info().rss
           total_memory = psutil.virtual_memory().total
           self.rss_soft_limit = int(total_memory * self.rss_soft_limit_ratio)
           
           self.logger.info(
               "MEMORY_MANAGER_INIT",
               extra={
                   "gc_threshold_mb": self.gc_threshold_mb,
                   "rss_soft_limit_mb": self.rss_soft_limit >> 20,
                   "total_memory_mb": total_memory >> 20
               }
           )
       except Exception as e:
           self.logger.warning(
               "MEMORY_INIT_FAILED",
               extra={"error": str(e), "fallback": "basic_gc_only"}
           )
           self.rss_prev = 0
           self.rss_soft_limit = float('inf')
   
   def init_edge_reference_counts(self, component_execution_order: List[str]) -> Dict[str, int]:
       edge_remaining = {}
       subjob_components = set(component_execution_order)
       
       for edge_id, base_count in self.base_edge_use_counts.items():
           if '->' in edge_id:
               source_part, target_part = edge_id.split('->', 1)
               source_comp = source_part.split('.')[0]
               target_comp = target_part.split('.')[0]
               
               if source_comp in subjob_components and target_comp in subjob_components:
                   edge_remaining[edge_id] = base_count
       
       return edge_remaining
   
   def update_edge_reference_counts(self, component_name: str, edge_remaining: Dict[str, int],
                                  component_outputs: Dict[str, Dict[str, Any]]) -> None:
       freed_edges = []
       
       for edge_id in list(edge_remaining.keys()):
           if f"->{component_name}." in edge_id:
               edge_remaining[edge_id] -= 1
               
               if edge_remaining[edge_id] <= 0:
                   source_part = edge_id.split('->', 1)[0]
                   source_comp, source_port = source_part.split('.', 1)
                   
                   if (source_comp in component_outputs and 
                       source_port in component_outputs[source_comp]):
                       del component_outputs[source_comp][source_port]
                       freed_edges.append(edge_id)
                   
                   del edge_remaining[edge_id]
       
       if freed_edges:
           self.logger.debug(
               "MEMORY_FREED",
               extra={"freed_edges": freed_edges, "consumer": component_name}
           )
   
   def maybe_collect_garbage(self, label: str) -> None:
       try:
           process = psutil.Process(os.getpid())
           rss_now = process.memory_info().rss
           rss_delta = rss_now - self.rss_prev
           threshold_bytes = self.gc_threshold_mb << 20
           
           if rss_delta >= threshold_bytes and rss_now >= self.rss_soft_limit:
               self.logger.debug(
                   "GC_TRIGGER",
                   extra={"where": label, "rss_mb": rss_now >> 20, "delta_mb": rss_delta >> 20}
               )
               gc.collect()
               self.rss_prev = process.memory_info().rss
               
       except Exception as e:
           self.logger.debug("GC_CHECK_FAILED", extra={"error": str(e)})
   
   def cleanup_subjob_memory(self, component_outputs: Dict[str, Dict[str, Any]], subjob_id: str) -> None:
       component_outputs.clear()
       gc.collect()
       
       self.logger.debug(
           "SUBJOB_MEMORY_CLEANUP",
           extra={"subjob_id": subjob_id, "gc_forced": True}
       )
   
   def calculate_total_rows(self, outputs: Dict[str, Union[pd.DataFrame, dd.DataFrame]]) -> int:
       total_rows = 0
       for df in outputs.values():
           if isinstance(df, pd.DataFrame):
               total_rows += len(df)
           elif isinstance(df, dd.DataFrame):
               try:
                   total_rows += len(df)
               except:
                   total_rows += 0
       return total_rows