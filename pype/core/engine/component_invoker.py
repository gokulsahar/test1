"""
Component invoker for DataPY execution engine.
"""

import asyncio
import importlib
import time
import threading
import logging
import ast
import operator
from typing import Dict, Any, Union, List, Optional
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field

import pandas as pd
import dask.dataframe as dd

from pype.core.engine.global_store import GlobalStore, BufferedStore
from pype.core.engine.templater import RuntimeTemplater, MissingGlobalVar
from pype.components.base import BaseComponent


@dataclass(frozen=True)
class ComponentResult:
   component_name: str
   success: bool
   outputs: Dict[str, Union[pd.DataFrame, dd.DataFrame]] = field(default_factory=dict)
   error: Optional[Exception] = None
   execution_time_ms: float = 0.0
   row_count: int = 0
   tokens_generated: List[str] = field(default_factory=list)


class ExecutionError(Exception):
   def __init__(self, component: str, message: str, original_error: Exception = None):
       self.component = component
       self.original_error = original_error
       super().__init__(f"Component '{component}': {message}")


class ComponentInvoker:
   def __init__(self, execution_metadata: Dict[str, Any], threadpool: ThreadPoolExecutor, 
                logger: logging.Logger, job_folder):
       self.logger = logger
       self.threadpool = threadpool
       self.job_folder = job_folder
       self.node_metadata = execution_metadata['node_metadata']
       self.runtime_templater = RuntimeTemplater(job_folder)
       self._component_cache: Dict[str, BaseComponent] = {}
       self._cache_lock = threading.RLock()
   
   async def execute_component(self, component_name: str, context: Dict[str, Any],
                             inputs: Dict[str, Union[pd.DataFrame, dd.DataFrame]], 
                             store: Union[GlobalStore, BufferedStore]) -> ComponentResult:
       start_time = time.perf_counter()
       
       try:
           globals_snapshot = self._dump_globals_snapshot(store)
           raw_config = self.node_metadata[component_name]["config"]
           
           try:
               resolved_config = self.runtime_templater.resolve_global_variables(
                   raw_config, globals_snapshot
               )
           except MissingGlobalVar as e:
               raise ExecutionError(
                   component_name, 
                   f"Missing global variable: {e.global_key}",
                   e
               )
           
           component = self._get_component_instance(component_name, resolved_config, store)
           loop = asyncio.get_event_loop()
           
           if asyncio.iscoroutinefunction(component.run):
               outputs = await component.run(context, inputs)
           else:
               outputs = await loop.run_in_executor(
                   self.threadpool, component.run, context, inputs
               )
           
           execution_time = (time.perf_counter() - start_time) * 1000
           row_count = self._calculate_total_rows(outputs)
           tokens_generated = self._generate_completion_tokens(component_name, True, context, globals_snapshot)
           
           self.logger.info(
               "COMPONENT_EXECUTED",
               extra={
                   "component": component_name,
                   "success": True,
                   "execution_time_ms": execution_time,
                   "row_count": row_count,
                   "output_ports": list(outputs.keys()),
                   "tokens_generated": tokens_generated
               }
           )
           
           return ComponentResult(
               component_name=component_name,
               success=True,
               outputs=outputs,
               execution_time_ms=execution_time,
               row_count=row_count,
               tokens_generated=tokens_generated
           )
           
       except (ExecutionError, TimeoutError) as e:
           execution_time = (time.perf_counter() - start_time) * 1000
           globals_snapshot = self._dump_globals_snapshot(store)
           tokens_generated = self._generate_completion_tokens(component_name, False, context, globals_snapshot)
           
           self.logger.error(
               "COMPONENT_FAILED",
               extra={
                   "component": component_name,
                   "error": str(e),
                   "error_type": type(e).__name__,
                   "execution_time_ms": execution_time,
                   "tokens_generated": tokens_generated
               }
           )
           
           return ComponentResult(
               component_name=component_name,
               success=False,
               error=e,
               execution_time_ms=execution_time,
               tokens_generated=tokens_generated
           )
   
   def _get_component_instance(self, component_name: str, resolved_config: Dict[str, Any], 
                             store: Union[GlobalStore, BufferedStore]) -> BaseComponent:
       config_hash = hash(str(sorted(resolved_config.items())))
       cache_key = f"{component_name}_{config_hash}"
       
       with self._cache_lock:
           if cache_key in self._component_cache:
               return self._component_cache[cache_key]
           
           metadata = self.node_metadata[component_name]
           registry_metadata = metadata['registry_metadata']
           module_path = registry_metadata['module_path']
           class_name = registry_metadata['class_name']
           
           try:
               module = importlib.import_module(module_path)
               component_class = getattr(module, class_name)
               
               component = component_class(
                   name=component_name,
                   config=resolved_config,
                   global_store=store
               )
               
               self._component_cache[cache_key] = component
               return component
               
           except Exception as e:
               raise ExecutionError(component_name, f"Failed to instantiate component: {e}", e)
   
   def _generate_completion_tokens(self, component_name: str, success: bool, 
                                 context: Dict[str, Any], globals_snapshot: Dict[str, Any]) -> List[str]:
       tokens: List[str] = [f"{component_name}::{'ok' if success else 'error'}"]
       
       if success:
           if_edges = self.node_metadata[component_name].get("if_edges", [])
           if if_edges:
               env = {**context, "globals": globals_snapshot}
               for edge in if_edges:
                   trig = edge["trigger"]
                   cond = edge["condition"]
                   try:
                       if self._safe_eval_bool(cond, env):
                           tokens.append(f"{component_name}::{trig}")
                   except ValueError as exc:
                       self.logger.error(
                           "COND_EVAL_FAILED",
                           extra={
                               "component": component_name,
                               "trigger": trig,
                               "condition": cond,
                               "error": str(exc),
                           },
                       )
       return tokens
   
   def _dump_globals_snapshot(self, store: Union[GlobalStore, BufferedStore]) -> Dict[str, Any]:
       try:
           dump_data = store.dump()
           import json
           state = json.loads(dump_data.decode('utf-8'))
           return state.get("data", {})
       except Exception as e:
           self.logger.warning(
               "GLOBALS_SNAPSHOT_FAILED",
               extra={"error": str(e), "fallback": "empty_dict"}
           )
           return {}
   
   def _safe_eval_bool(self, condition: str, env: Dict[str, Any]) -> bool:
       allowed_ops = {
           ast.Eq: operator.eq, ast.NotEq: operator.ne, ast.Lt: operator.lt,
           ast.LtE: operator.le, ast.Gt: operator.gt, ast.GtE: operator.ge,
           ast.And: operator.and_, ast.Or: operator.or_, ast.Not: operator.not_,
           ast.Is: operator.is_, ast.IsNot: operator.is_not,
           ast.In: lambda x, y: x in y, ast.NotIn: lambda x, y: x not in y,
       }
       
       def eval_node(node):
           if isinstance(node, ast.Constant):
               return node.value
           elif isinstance(node, ast.Num):
               return node.n
           elif isinstance(node, ast.Str):
               return node.s
           elif isinstance(node, ast.Name):
               if node.id in env:
                   return env[node.id]
               else:
                   raise ValueError(f"Unknown variable: {node.id}")
           elif isinstance(node, ast.Subscript):
               value = eval_node(node.value)
               slice_val = eval_node(node.slice) if hasattr(node.slice, 'value') else eval_node(node.slice.value)
               return value[slice_val]
           elif isinstance(node, ast.Compare):
               left = eval_node(node.left)
               for op, right in zip(node.ops, node.comparators):
                   if type(op) not in allowed_ops:
                       raise ValueError(f"Unsupported operator: {type(op).__name__}")
                   right_val = eval_node(right)
                   if not allowed_ops[type(op)](left, right_val):
                       return False
                   left = right_val
               return True
           elif isinstance(node, ast.BoolOp):
               if type(node.op) not in allowed_ops:
                   raise ValueError(f"Unsupported boolean operator: {type(node.op).__name__}")
               if isinstance(node.op, ast.And):
                   return all(eval_node(val) for val in node.values)
               elif isinstance(node.op, ast.Or):
                   return any(eval_node(val) for val in node.values)
           elif isinstance(node, ast.UnaryOp):
               if type(node.op) not in allowed_ops:
                   raise ValueError(f"Unsupported unary operator: {type(node.op).__name__}")
               return allowed_ops[type(node.op)](eval_node(node.operand))
           else:
               raise ValueError(f"Unsupported AST node: {type(node).__name__}")
       
       try:
           tree = ast.parse(condition, mode='eval')
           result = eval_node(tree.body)
           
           if not isinstance(result, bool):
               raise ValueError(f"Condition must evaluate to boolean, got {type(result).__name__}")
           
           return result
           
       except SyntaxError as e:
           raise ValueError(f"Invalid syntax in condition: {e}")
   
   def _calculate_total_rows(self, outputs: Dict[str, Union[pd.DataFrame, dd.DataFrame]]) -> int:
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