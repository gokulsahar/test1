"""
DataPY Execution Manager - Component lifecycle coordination and memory management.
"""

import asyncio
import time
import logging
from pathlib import Path
from typing import Dict, Any, Union, Optional, List
from concurrent.futures import ThreadPoolExecutor

import pandas as pd
import dask.dataframe as dd

from pype.core.engine.global_store import GlobalStore, BufferedStore
from pype.core.engine.subjob_tracker import SubJobTracker, ComponentState
from pype.core.engine.component_invoker import ComponentInvoker, ComponentResult, ExecutionError
from pype.core.engine.memory_manager import MemoryManager
from pype.core.engine.foreach_executor import ForEachExecutor
from pype.core.engine.regular_executor import RegularExecutor


class ExecutionManager:
   def __init__(self, execution_metadata: Dict[str, Any], global_store: GlobalStore, 
                threadpool: ThreadPoolExecutor, logger: logging.Logger, job_folder: Path):
       self.logger = logger
       self.global_store = global_store
       self.threadpool = threadpool
       self.job_folder = job_folder
       
       self.execution_metadata = execution_metadata
       self.subjob_boundaries = execution_metadata['subjob_boundaries']
       
       self.memory_manager = MemoryManager(execution_metadata, logger)
       self.component_invoker = ComponentInvoker(execution_metadata, threadpool, logger, job_folder)
       self.foreach_executor = ForEachExecutor(self.component_invoker, logger)
       self.regular_executor = RegularExecutor(
           self.component_invoker, self.memory_manager, execution_metadata, logger
       )
   
   async def execute_subjob(self, subjob_id: str, context: Dict[str, Any], 
                          subjob_tracker: SubJobTracker, fired_tokens: set) -> None:
       self.logger.info(
           "SUBJOB_START",
           extra={"subjob_id": subjob_id, "context": context}
       )
       
       subjob_metadata = self.subjob_boundaries[subjob_id]
       component_execution_order = subjob_metadata['component_execution_order']
       
       start_time = time.perf_counter()
       
       try:
           has_iterators = subjob_metadata.get('has_iterators', False)
           
           if has_iterators:
               await self.foreach_executor.execute_forEach_subjob(
                   subjob_id, component_execution_order, context, subjob_tracker, 
                   fired_tokens, subjob_metadata
               )
           else:
               await self.regular_executor.execute_regular_subjob(
                   subjob_id, component_execution_order, context, subjob_tracker,
                   fired_tokens, self.global_store
               )
       
       finally:
           execution_time = (time.perf_counter() - start_time) * 1000
           self.memory_manager.cleanup_subjob_memory({}, subjob_id)
           
           self.logger.info(
               "SUBJOB_COMPLETE",
               extra={
                   "subjob_id": subjob_id,
                   "execution_time_ms": execution_time,
                   "components_executed": len(component_execution_order)
               }
           )