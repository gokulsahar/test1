"""
DataPY Execution Manager - Component lifecycle with memory management and forEach iteration.

Handles component execution, data flow routing, memory management with reference counting,
and forEach iteration with proper BufferedStore integration.
"""

import asyncio
import gc
import threading
import os
import psutil
import importlib
import time
import logging
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, Any, Union, Optional, List, Tuple
from concurrent.futures import ThreadPoolExecutor

import pandas as pd
import dask.dataframe as dd

from pype.core.engine.global_store import GlobalStore, BufferedStore
from pype.core.engine.subjob_tracker import SubJobTracker, ComponentState
from pype.core.engine.templater import RuntimeTemplater, MissingGlobalVar
from pype.components.base import BaseComponent


@dataclass(frozen=True)
class ComponentResult:
    """Immutable result from component execution for orchestrator."""
    component_name: str
    success: bool
    outputs: Dict[str, Union[pd.DataFrame, dd.DataFrame]] = field(default_factory=dict)
    error: Optional[Exception] = None
    execution_time_ms: float = 0.0
    row_count: int = 0
    tokens_generated: List[str] = field(default_factory=list)


class ExecutionError(Exception):
    """Component execution error with structured metadata."""
    
    def __init__(self, component: str, message: str, original_error: Exception = None):
        self.component = component
        self.original_error = original_error
        super().__init__(f"Component '{component}': {message}")


class ExecutionManager:
    """
    Manages component lifecycle, data flow, and memory optimization.
    
    Implements topological execution within subjobs, reference counting for memory management,
    and forEach iteration with BufferedStore coordination.
    """
    
    def __init__(self, execution_metadata: Dict[str, Any], global_store: GlobalStore, 
                 threadpool: ThreadPoolExecutor, logger: logging.Logger, job_folder: Path):
        """
        Initialize ExecutionManager with metadata and resources.
        
        Args:
            execution_metadata: Complete execution metadata from planner
            global_store: Job-wide global variable store
            threadpool: Shared threadpool for blocking operations
            logger: Structured logger for execution events
            job_folder: Path to job folder for template resolution
        """
        self.logger = logger
        self.global_store = global_store
        self.threadpool = threadpool
        self.job_folder = job_folder
        
        # Initialize runtime templater for global variable resolution
        self.runtime_templater = RuntimeTemplater(job_folder)
        
        # Extract metadata for execution
        self.node_metadata = execution_metadata['node_metadata']
        self.component_dependencies = execution_metadata['component_dependencies']
        self.port_mapping = execution_metadata['port_mapping']
        self.subjob_boundaries = execution_metadata['subjob_boundaries']
        
        # Memory management configuration
        memory_mgmt = execution_metadata.get('memory_management', {})
        self.edge_use_counts = memory_mgmt.get('edge_use_counts', {})
        self.gc_threshold_mb = memory_mgmt.get('gc_threshold_mb', 256)
        self.rss_soft_limit_ratio = memory_mgmt.get('rss_soft_limit_ratio', 0.70)
        
        # Component instance cache
        self._component_cache: Dict[str, BaseComponent] = {}
        
        # Memory management state
        self._init_memory_management()
        
        # Component instance cache with size limit
        self._component_cache: Dict[str, BaseComponent] = {}
        self._cache_access_times: Dict[str, float] = {}  # Track access times for LRU
        self._max_cache_size = 100  # Configurable cache size limit
        self._cache_lock = threading.RLock()
        
        
        self._globals_snapshot_cache = {}
        self._globals_cache_revision = -1
        
        
        # Safety limits
        self.max_iterations = 10000  # Prevent infinite forEach loops
    
    def _init_memory_management(self) -> None:
        """Initialize memory management tracking."""
        try:
            process = psutil.Process(os.getpid())
            self.rss_prev = process.memory_info().rss
            
            # Calculate soft limit (70% of available memory)
            total_memory = psutil.virtual_memory().total
            self.rss_soft_limit = int(total_memory * self.rss_soft_limit_ratio)
            
            self.logger.info(
                "MEMORY_MANAGEMENT_INIT",
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
    
    async def execute_subjob(self, subjob_id: str, context: Dict[str, Any], 
                           subjob_tracker: SubJobTracker, fired_tokens: set) -> None:
        """
        Execute all components in a subjob using topological order with memory management.
        
        Args:
            subjob_id: Subjob identifier
            context: Execution context from orchestrator
            subjob_tracker: SubJobTracker for state management
            fired_tokens: Set of fired control tokens
        """
        self.logger.info(
            "SUBJOB_START",
            extra={"subjob_id": subjob_id, "context": context}
        )
        
        subjob_metadata = self.subjob_boundaries[subjob_id]
        component_execution_order = subjob_metadata['component_execution_order']
        
        # Initialize subjob data store and memory management
        component_outputs: Dict[str, Dict[str, Union[pd.DataFrame, dd.DataFrame]]] = {}
        edge_remaining: Dict[str, int] = self._init_edge_reference_counts(component_execution_order)
        
        start_time = time.perf_counter()
        
        try:
            # Check if this is a forEach subjob
            has_iterators = subjob_metadata.get('has_iterators', False)
            
            if has_iterators:
                await self._execute_forEach_subjob(
                    subjob_id, component_execution_order, context, subjob_tracker, 
                    fired_tokens, component_outputs, edge_remaining
                )
            else:
                await self._execute_regular_subjob(
                    subjob_id, component_execution_order, context, subjob_tracker,
                    fired_tokens, component_outputs, edge_remaining
                )
        
        finally:
            # End-of-subjob cleanup
            execution_time = (time.perf_counter() - start_time) * 1000
            self._cleanup_subjob_memory(component_outputs, subjob_id)
            
            self.logger.info(
                "SUBJOB_COMPLETE",
                extra={
                    "subjob_id": subjob_id,
                    "execution_time_ms": execution_time,
                    "components_executed": len(component_execution_order)
                }
            )
    
    async def _execute_regular_subjob(self, subjob_id: str, component_execution_order: List[str],
                                    context: Dict[str, Any], subjob_tracker: SubJobTracker,
                                    fired_tokens: set, component_outputs: Dict[str, Dict[str, Any]],
                                    edge_remaining: Dict[str, int]) -> None:
        """Execute regular subjob with topological order and readiness checks."""
        for component_name in component_execution_order:
            # Check if component is ready to execute
            if not self._can_component_execute(component_name, component_outputs, fired_tokens):
                self.logger.warning(
                    "COMPONENT_NOT_READY",
                    extra={"component": component_name, "subjob_id": subjob_id}
                )
                continue
            
            # Update tracker state
            subjob_tracker.notify(component_name, ComponentState.RUNNING)
            
            try:
                # Prepare component inputs
                inputs = self._prepare_component_inputs(component_name, component_outputs)
                
                # Execute component
                result = await self._execute_component(component_name, context, inputs)
                
                # Store outputs and manage memory
                if result.success:
                    component_outputs[component_name] = result.outputs
                    self._update_edge_reference_counts(component_name, edge_remaining, component_outputs)
                    subjob_tracker.notify(component_name, ComponentState.SUCCEEDED)
                    
                    fired_tokens.update(result.tokens_generated)
                    
                else:
                    subjob_tracker.notify(component_name, ComponentState.FAILED)
                    
                    fired_tokens.update(result.tokens_generated)
                    
                    # Handle error flow - abort subjob immediately
                    await self._handle_component_error(component_name, result.error, subjob_tracker)
            
            # Specific exception handling by category
            except MissingGlobalVar as e:
                self.logger.error(
                    "MISSING_GLOBAL_VARIABLE",
                    extra={
                        "component": component_name,
                        "subjob_id": subjob_id,
                        "global_key": e.global_key,
                        "error_type": "MissingGlobalVar"
                    }
                )
                subjob_tracker.notify(component_name, ComponentState.FAILED)
                await self._handle_component_error(component_name, e, subjob_tracker)
            
            except ImportError as e:
                self.logger.error(
                    "COMPONENT_IMPORT_ERROR",
                    extra={
                        "component": component_name,
                        "subjob_id": subjob_id,
                        "error": str(e),
                        "error_type": "ImportError"
                    }
                )
                subjob_tracker.notify(component_name, ComponentState.FAILED)
                await self._handle_component_error(component_name, e, subjob_tracker)
            
            except MemoryError as e:
                self.logger.error(
                    "COMPONENT_MEMORY_ERROR",
                    extra={
                        "component": component_name,
                        "subjob_id": subjob_id,
                        "error": str(e),
                        "error_type": "MemoryError"
                    }
                )
                # Force garbage collection on memory error
                self._maybe_collect_garbage("memory_error")
                subjob_tracker.notify(component_name, ComponentState.FAILED)
                await self._handle_component_error(component_name, e, subjob_tracker)
            
            except asyncio.TimeoutError as e:
                self.logger.error(
                    "COMPONENT_TIMEOUT_ERROR",
                    extra={
                        "component": component_name,
                        "subjob_id": subjob_id,
                        "error": str(e),
                        "error_type": "TimeoutError"
                    }
                )
                subjob_tracker.notify(component_name, ComponentState.FAILED)
                await self._handle_component_error(component_name, e, subjob_tracker)
            
            except Exception as e:
                # Catch-all for unexpected errors
                self.logger.error(
                    "COMPONENT_UNEXPECTED_ERROR",
                    extra={
                        "component": component_name,
                        "subjob_id": subjob_id,
                        "error": str(e),
                        "error_type": type(e).__name__
                    }
                )
                subjob_tracker.notify(component_name, ComponentState.FAILED)
                await self._handle_component_error(component_name, e, subjob_tracker)
            
            # Memory management checkpoint
            self._maybe_collect_garbage("component_execution")
    
    async def _execute_forEach_subjob(self, subjob_id: str, component_execution_order: List[str],
                                    context: Dict[str, Any], subjob_tracker: SubJobTracker,
                                    fired_tokens: set, component_outputs: Dict[str, Dict[str, Any]],
                                    edge_remaining: Dict[str, int]) -> None:
        """Execute forEach subjob with iteration support."""
        subjob_metadata = self.subjob_boundaries[subjob_id]
        iterator_components = subjob_metadata.get('iterator_components', {})
        
        # Find root forEach component
        root_forEach = self._find_root_forEach_component(iterator_components)
        
        if not root_forEach:
            self.logger.error(
                "NO_ROOT_FOREACH",
                extra={"subjob_id": subjob_id, "iterator_components": list(iterator_components.keys())}
            )
            return
        
        # Execute forEach with recursive iteration
        await self._execute_forEach_iteration(
            root_forEach, iterator_components, component_execution_order,
            context, subjob_tracker, fired_tokens, component_outputs, edge_remaining
        )
    
    async def _execute_forEach_iteration(self, forEach_component: str, iterator_metadata: Dict[str, Any],
                                    component_execution_order: List[str], context: Dict[str, Any],
                                    subjob_tracker: SubJobTracker, fired_tokens: set,
                                    component_outputs: Dict[str, Dict[str, Any]],
                                    edge_remaining: Dict[str, int]) -> None:
        """Execute forEach component with recursive iteration support."""
        import copy
        
        iteration_count = 0
        forEach_metadata = iterator_metadata[forEach_component]
        iteration_scope = forEach_metadata.get('iteration_scope', [])
        
        # Create BufferedStore for forEach iteration
        buffered_store = BufferedStore(self.global_store, self.logger, forEach_component)
        
        while iteration_count < self.max_iterations:
            # Start new iteration
            buffered_store.start_iteration({"iteration": iteration_count})
            
            # Create fresh isolated state for each iteration
            iteration_outputs = {}
            iteration_edge_remaining = {}
            
            # Only copy data that's explicitly needed for this iteration
            for comp_name in iteration_scope:
                if comp_name in component_outputs:
                    # Deep copy only the outputs we need
                    iteration_outputs[comp_name] = copy.deepcopy(component_outputs[comp_name])
            
            # Initialize edge counts only for iteration scope
            for edge_id, count in edge_remaining.items():
                if any(comp in edge_id for comp in iteration_scope):
                    iteration_edge_remaining[edge_id] = count
            
            try:
                # Execute forEach component to get next item
                inputs = self._prepare_component_inputs(forEach_component, iteration_outputs)
                forEach_result = await self._execute_component_with_buffered_store(
                    forEach_component, context, inputs, buffered_store
                )
                
                if not forEach_result.success:
                    break
                
                # Check if iteration is complete (no more data)
                if not forEach_result.outputs or forEach_result.outputs.get("item") is None:
                    self.logger.info(
                        "FOREACH_ITERATION_COMPLETE",
                        extra={"forEach_component": forEach_component, "iterations": iteration_count}
                    )
                    break
                
                # Store forEach output for iteration scope
                iteration_outputs[forEach_component] = forEach_result.outputs
                
                # Execute iteration scope components
                for component_name in iteration_scope:
                    if component_name == forEach_component:
                        continue  # Already executed
                    
                    if self._can_component_execute(component_name, iteration_outputs, fired_tokens):
                        inputs = self._prepare_component_inputs(component_name, iteration_outputs)
                        result = await self._execute_component_with_buffered_store(
                            component_name, context, inputs, buffered_store
                        )
                        
                        if result.success:
                            iteration_outputs[component_name] = result.outputs
                            self._update_edge_reference_counts(component_name, iteration_edge_remaining, iteration_outputs)
                
                # Handle nested forEach if present
                nested_iterators = forEach_metadata.get('nested_iterators', [])
                for nested_forEach in nested_iterators:
                    if nested_forEach in iterator_metadata:
                        await self._execute_forEach_iteration(
                            nested_forEach, iterator_metadata, iteration_scope,
                            context, subjob_tracker, fired_tokens, iteration_outputs, iteration_edge_remaining
                        )
                
                buffered_store.end_iteration()
                iteration_count += 1
                
            except Exception as e:
                self.logger.error(
                    "FOREACH_ITERATION_ERROR",
                    extra={"forEach_component": forEach_component, "iteration": iteration_count, "error": str(e)}
                )
                break
        
        # Flush BufferedStore at forEach completion
        flush_results = buffered_store.flush()
        self.logger.info(
            "FOREACH_BUFFERED_STORE_FLUSHED",
            extra={"forEach_component": forEach_component, "variables_flushed": len(flush_results)}
        )
    
    def _can_component_execute(self, component_name: str, component_outputs: Dict[str, Dict[str, Any]], 
                            fired_tokens: set) -> bool:
        """
        Check if component can execute based on data and control dependencies.
        
        Implements: "Components wait for ALL incoming control & data edges before starting"
        """
        deps = self.component_dependencies[component_name]
        
        # Check data dependencies
        data_deps = deps['data']
        data_ready = all(dep in component_outputs for dep in data_deps)
        
        # Check control dependencies
        control_deps = deps['control']
        control_ready = all(f"{dep}::ok" in fired_tokens for dep in control_deps)
        
        port_info = self.port_mapping[component_name]
        inputs = port_info['inputs']
        
        # Check that for each required input port, the source component has the output
        data_available = all(
            source_comp in component_outputs and port_name in component_outputs[source_comp]
            for port_name, source_comp in inputs
        )
        
        return data_ready and control_ready and data_available
    
    def _prepare_component_inputs(self, component_name: str, 
                                component_outputs: Dict[str, Dict[str, Any]]) -> Dict[str, Union[pd.DataFrame, dd.DataFrame]]:
        """
        Prepare inputs for component execution using port-based routing.
        
        Fan-in handling: Multiple DataFrames for same port are passed as-is,
        component decides how to merge them.
        """
        inputs = {}
        port_info = self.port_mapping[component_name]
        
        for port_name, source_component in port_info['inputs']:
            if source_component in component_outputs:
                source_outputs = component_outputs[source_component]
                
                # Find matching output port (may have different name)
                for output_port, data in source_outputs.items():
                    if output_port == port_name or output_port == 'main':
                        if port_name in inputs:
                            # Handle fan-in: multiple inputs to same port
                            if not isinstance(inputs[port_name], list):
                                inputs[port_name] = [inputs[port_name]]
                            inputs[port_name].append(data)
                        else:
                            inputs[port_name] = data
                        break
        
        return inputs
    
    async def _execute_component(self, component_name: str, context: Dict[str, Any], 
                               inputs: Dict[str, Union[pd.DataFrame, dd.DataFrame]]) -> ComponentResult:
        """Execute component with proper lifecycle and error handling."""
        return await self._execute_component_with_store(component_name, context, inputs, self.global_store)
    
    async def _execute_component_with_buffered_store(self, component_name: str, context: Dict[str, Any],
                                                   inputs: Dict[str, Union[pd.DataFrame, dd.DataFrame]],
                                                   buffered_store: BufferedStore) -> ComponentResult:
        """Execute component with BufferedStore for forEach iterations."""
        return await self._execute_component_with_store(component_name, context, inputs, buffered_store)
    
    async def _execute_component_with_store(self, component_name: str, context: Dict[str, Any],
                                          inputs: Dict[str, Union[pd.DataFrame, dd.DataFrame]],
                                          store: Union[GlobalStore, BufferedStore]) -> ComponentResult:
        """Core component execution with configurable store and global variable resolution."""
        start_time = time.perf_counter()
        
        try:
            # 1️⃣ Snapshot globals (fast, lock-free)
            globals_snapshot = self._dump_globals_snapshot()
            
            # 2️⃣ Resolve component config with global variables
            raw_config = self.node_metadata[component_name]["config"]
            try:
                resolved_config = self.runtime_templater.resolve_global_variables(
                    raw_config, globals_snapshot
                )
            except MissingGlobalVar as e:
                # Treat missing global variable as component error
                raise ExecutionError(
                    component_name, 
                    f"Missing global variable: {e.global_key}",
                    e
                )
            
            # 3️⃣ Get or create component instance with resolved config
            component = self._get_component_instance(component_name, resolved_config, store)
            
            # Execute with asyncio concurrency guard
            loop = asyncio.get_event_loop()
            
            if asyncio.iscoroutinefunction(component.run):
                # Native async component
                outputs = await component.run(context, inputs)
            else:
                # Blocking component - use threadpool
                outputs = await loop.run_in_executor(
                    self.threadpool, component.run, context, inputs
                )
            
            # Calculate metrics
            execution_time = (time.perf_counter() - start_time) * 1000
            row_count = self._calculate_total_rows(outputs)
            
            # Generate tokens for component completion
            tokens_generated = self._generate_completion_tokens(component_name, True, context)
            
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
            
        except Exception as e:
            execution_time = (time.perf_counter() - start_time) * 1000
            
            # Generate error tokens
            tokens_generated = self._generate_completion_tokens(component_name, False, context)
            
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
    
    
    
    def _init_edge_reference_counts(self, component_execution_order: List[str]) -> Dict[str, int]:
        """Initialize edge reference counts for memory management."""
        edge_remaining = {}
        
        # Only initialize edges for components in this subjob
        subjob_components = set(component_execution_order)
        
        for edge_id, use_count in self.edge_use_counts.items():
            # Parse edge_id: "source.port->target.port"
            if '->' in edge_id:
                source_part, target_part = edge_id.split('->', 1)
                source_comp = source_part.split('.')[0]
                target_comp = target_part.split('.')[0]
                
                # Only track edges within this subjob
                if source_comp in subjob_components and target_comp in subjob_components:
                    edge_remaining[edge_id] = use_count
        
        return edge_remaining
    
    def _update_edge_reference_counts(self, component_name: str, edge_remaining: Dict[str, int],
                                    component_outputs: Dict[str, Dict[str, Any]]) -> None:
        """Update edge reference counts and free memory when count reaches zero."""
        freed_edges = []
        
        # Find edges where this component is the consumer
        for edge_id in list(edge_remaining.keys()):
            if f"->{component_name}." in edge_id:
                edge_remaining[edge_id] -= 1
                
                if edge_remaining[edge_id] <= 0:
                    # Free the DataFrame
                    source_part = edge_id.split('->', 1)[0]
                    source_comp, source_port = source_part.split('.', 1)
                    
                    if source_comp in component_outputs and source_port in component_outputs[source_comp]:
                        del component_outputs[source_comp][source_port]
                        freed_edges.append(edge_id)
                    
                    del edge_remaining[edge_id]
        
        if freed_edges:
            self.logger.debug(
                "MEMORY_FREED",
                extra={"freed_edges": freed_edges, "consumer": component_name}
            )
    
    def _generate_completion_tokens(self, component_name: str, success: bool, context: Dict[str, Any]) -> List[str]:
        """
        Emit dependency tokens for the orchestrator.
        * always "comp::ok" or "comp::error"
        * on success, additionally evaluate every IF-edge attached to the
          component (any number, any order). Metadata is provided by the
          planner and stored under node_metadata[comp]['if_edges']:
              [{'trigger': 'if1', 'condition': '<expr>'}, …]
        The expression language:
          • can reference any key in *context* directly, e.g. attempt == 1
          • can reference globals via globals["row_count"]
          • admits only boolean / comparison operators (see _safe_eval_bool)
        """
        tokens: List[str] = [f"{component_name}::{'ok' if success else 'error'}"]
        if success:
            if_edges = self.node_metadata[component_name].get("if_edges", [])
            if if_edges:
                # point-in-time snapshot of the GlobalStore
                globals_snapshot = self._dump_globals_snapshot()  # cheap, lock-free
                env = {**context, "globals": globals_snapshot}
                for edge in if_edges:
                    trig = edge["trigger"]          # "if1", "if2", …
                    cond = edge["condition"]
                    try:
                        if self._safe_eval_bool(cond, env):
                            tokens.append(f"{component_name}::{trig}")
                    except ValueError as exc:
                        # Broken condition => log & skip, never crash engine
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
    
    async def _handle_component_error(self, component_name: str, error: Exception, 
                                     subjob_tracker: SubJobTracker) -> None:
        """
        Handle component error with immediate subjob abort.
        
        Follows Talend "OnComponentError" semantics:
        - Mark subjob as failed immediately
        - Let orchestrator handle error token dispatch
        """
        # Mark subjob as failed to abort remaining components
        subjob_tracker.notify_component_failure(component_name, "execution_error")
        
        # Raise error to stop current subjob execution
        # Orchestrator will handle error token generation and clearing
        raise ExecutionError(component_name, str(error), error)
    
    def _calculate_total_rows(self, outputs: Dict[str, Union[pd.DataFrame, dd.DataFrame]]) -> int:
        """Calculate total row count across all output DataFrames with safe Dask handling."""
        total_rows = 0
        
        for df in outputs.values():
            if isinstance(df, pd.DataFrame):
                total_rows += len(df)
            elif isinstance(df, dd.DataFrame):

                try:
                    # Only compute length if divisions are known (cheap operation)
                    if df.known_divisions:
                        df_len = len(df)
                        # Soft cap to prevent massive computations
                        if df_len < 1_000_000:  # 1M row soft limit
                            total_rows += df_len
                        else:
                            total_rows += 0  # Skip expensive counts
                    else:
                        # Use npartitions as rough estimate when divisions unknown
                        total_rows += df.npartitions * 1000  # Rough estimate
                except Exception:
                    total_rows += 0  # Fallback on any error
        
        return total_rows
    
    def _find_root_forEach_component(self, iterator_components: Dict[str, Any]) -> Optional[str]:
        """Find root forEach component (depth 0) in iterator hierarchy."""
        for forEach_comp, metadata in iterator_components.items():
            if metadata.get('iterator_depth', 0) == 0:
                return forEach_comp
        return None
    
    def _maybe_collect_garbage(self, label: str) -> None:
        """Smart garbage collection based on memory thresholds."""
        try:
            process = psutil.Process(os.getpid())
            rss_now = process.memory_info().rss
            
            # Check thresholds
            rss_delta = rss_now - self.rss_prev
            threshold_bytes = self.gc_threshold_mb << 20  # Convert MB to bytes
            
            # Use OR logic instead of AND for GC triggers
            should_gc = (rss_delta >= threshold_bytes) or (rss_now >= self.rss_soft_limit)
            
            if should_gc:
                self.logger.debug(
                    "GC_TRIGGER",
                    extra={
                        "where": label, 
                        "rss_mb": rss_now >> 20, 
                        "delta_mb": rss_delta >> 20,
                        "trigger_reason": "delta" if rss_delta >= threshold_bytes else "soft_limit"
                    }
                )
                gc.collect()
                
                # Always reset rss_prev after collection
                self.rss_prev = process.memory_info().rss
                
        except Exception as e:
            self.logger.debug("GC_CHECK_FAILED", extra={"error": str(e)})
    
    def _dump_globals_snapshot(self) -> Dict[str, Any]:
        """Get point-in-time snapshot of GlobalStore with caching for condition evaluation."""
        try:
            current_revision = self.global_store.revision()
            
            #  Cache snapshot based on revision
            if current_revision == self._globals_cache_revision and self._globals_snapshot_cache:
                return self._globals_snapshot_cache
            
            # Update cache
            dump_data = self.global_store.dump()
            import json
            state = json.loads(dump_data.decode('utf-8'))
            snapshot = state.get("data", {})
            
            self._globals_snapshot_cache = snapshot
            self._globals_cache_revision = current_revision
            
            return snapshot
            
        except Exception as e:
            self.logger.warning(
                "GLOBALS_SNAPSHOT_FAILED",
                extra={"error": str(e), "fallback": "empty_dict"}
            )
            return {}
    
    def _safe_eval_bool(self, condition: str, env: Dict[str, Any], max_depth: int = 10) -> bool:
        """
        Safe boolean expression evaluation with restricted operators and depth limiting.
        """
        import ast
        import operator
        
        # Allowed operations for safety
        allowed_ops = {
            ast.Eq: operator.eq,
            ast.NotEq: operator.ne,
            ast.Lt: operator.lt,
            ast.LtE: operator.le,
            ast.Gt: operator.gt,
            ast.GtE: operator.ge,
            ast.And: operator.and_,
            ast.Or: operator.or_,
            ast.Not: operator.not_,
            ast.Is: operator.is_,
            ast.IsNot: operator.is_not,
            ast.In: lambda x, y: x in y,
            ast.NotIn: lambda x, y: x not in y,
        }
        
        def eval_node(node, depth=0):
            #  Prevent infinite recursion
            if depth > max_depth:
                raise ValueError(f"Expression too deeply nested (max depth: {max_depth})")
            
            if isinstance(node, ast.Constant):
                return node.value
            elif isinstance(node, ast.Num):  # Python < 3.8 compatibility
                return node.n
            elif isinstance(node, ast.Str):  # Python < 3.8 compatibility
                return node.s
            elif isinstance(node, ast.Name):
                if node.id in env:
                    return env[node.id]
                else:
                    raise ValueError(f"Unknown variable: {node.id}")
            elif isinstance(node, ast.Subscript):
                try:
                    value = eval_node(node.value, depth + 1)
                    # Handle different Python versions safely
                    if hasattr(node.slice, 'value'):
                        slice_val = eval_node(node.slice.value, depth + 1)
                    elif hasattr(node, 'slice') and hasattr(node.slice, 'id'):
                        slice_val = node.slice.id
                    else:
                        slice_val = eval_node(node.slice, depth + 1)
                    return value[slice_val]
                except (KeyError, IndexError, TypeError) as e:
                    raise ValueError(f"Subscript access failed: {e}")
            elif isinstance(node, ast.Compare):
                left = eval_node(node.left, depth + 1)
                for op, right in zip(node.ops, node.comparators):
                    if type(op) not in allowed_ops:
                        raise ValueError(f"Unsupported operator: {type(op).__name__}")
                    right_val = eval_node(right, depth + 1)
                    if not allowed_ops[type(op)](left, right_val):
                        return False
                    left = right_val
                return True
            elif isinstance(node, ast.BoolOp):
                if type(node.op) not in allowed_ops:
                    raise ValueError(f"Unsupported boolean operator: {type(node.op).__name__}")
                if isinstance(node.op, ast.And):
                    return all(eval_node(val, depth + 1) for val in node.values)
                elif isinstance(node.op, ast.Or):
                    return any(eval_node(val, depth + 1) for val in node.values)
            elif isinstance(node, ast.UnaryOp):
                if type(node.op) not in allowed_ops:
                    raise ValueError(f"Unsupported unary operator: {type(node.op).__name__}")
                return allowed_ops[type(node.op)](eval_node(node.operand, depth + 1))
            else:
                raise ValueError(f"Unsupported AST node: {type(node).__name__}")
        
        try:
            # Parse the condition into AST
            tree = ast.parse(condition, mode='eval')
            result = eval_node(tree.body)
            
            # Ensure result is boolean
            if not isinstance(result, bool):
                raise ValueError(f"Condition must evaluate to boolean, got {type(result).__name__}")
            
            return result
            
        except SyntaxError as e:
            raise ValueError(f"Invalid syntax in condition: {e}")
        except Exception as e:
            #  Return False with warning instead of crashing on edge cases
            self.logger.warning(
                "SAFE_EVAL_FALLBACK",
                extra={"condition": condition, "error": str(e)}
            )
            return False
    
    def _cleanup_subjob_memory(self, component_outputs: Dict[str, Dict[str, Any]], subjob_id: str) -> None:
        """Clean up subjob memory and force garbage collection."""
        # Clear all component outputs
        component_outputs.clear()
        
        # Force garbage collection at subjob boundary
        gc.collect()
        
        self.logger.debug(
            "SUBJOB_MEMORY_CLEANUP",
            extra={"subjob_id": subjob_id, "gc_forced": True}
        )
        
        

        
        
    def _get_component_instance(self, component_name: str, resolved_config: Dict[str, Any], 
                            store: Union[GlobalStore, BufferedStore]) -> BaseComponent:
        """Thread-safe component instance retrieval with cache management."""
        import json
        import hashlib
        
        #  Stable JSON-based cache key
        try:
            config_json = json.dumps(resolved_config, sort_keys=True, default=str)
            config_hash = hashlib.blake2s(config_json.encode('utf-8'), digest_size=8).hexdigest()
            cache_key = f"{component_name}_{config_hash}"
        except (TypeError, ValueError) as e:
            # Fallback for non-serializable configs
            self.logger.warning("CACHE_KEY_FALLBACK", extra={"component": component_name, "error": str(e)})
            cache_key = f"{component_name}_fallback_{id(resolved_config)}"
        
        with self._cache_lock:
            if cache_key in self._component_cache:
                #  Update access time AFTER cache hit check
                self._cache_access_times[cache_key] = time.time()
                return self._component_cache[cache_key]
            
            # Clean up cache if it's getting too large
            self._cleanup_component_cache_locked()
        
        # Create component outside of lock to minimize lock time
        component = self._create_component_instance(component_name, resolved_config, store)
        
        # Store in cache with lock
        with self._cache_lock:
            self._component_cache[cache_key] = component
            self._cache_access_times[cache_key] = time.time()
        
        return component

    def _create_component_instance(self, component_name: str, resolved_config: Dict[str, Any], 
                                store: Union[GlobalStore, BufferedStore]) -> BaseComponent:
        """Create new component instance (called outside of lock)."""
        # Get component metadata
        metadata = self.node_metadata[component_name]
        registry_metadata = metadata['registry_metadata']
        
        # Import component class
        module_path = registry_metadata['module_path']
        class_name = registry_metadata['class_name']
        
        try:
            module = importlib.import_module(module_path)
            component_class = getattr(module, class_name)
            
            # Create instance with resolved config
            component = component_class(
                name=component_name,
                config=resolved_config,
                global_store=store
            )
            
            return component
            
        except Exception as e:
            raise ExecutionError(component_name, f"Failed to instantiate component: {e}", e)

    def _cleanup_component_cache_locked(self) -> None:
        """Thread-safe component cache cleanup (must be called within lock)."""
        if len(self._component_cache) <= self._max_cache_size:
            return
        
        # Sort by access time (LRU first)
        sorted_items = sorted(
            self._cache_access_times.items(), 
            key=lambda x: x[1]
        )
        
        # Remove oldest 25% of cached items
        items_to_remove = len(sorted_items) // 4
        
        # Collect components to cleanup BEFORE releasing lock
        components_to_cleanup = []
        
        for cache_key, _ in sorted_items[:items_to_remove]:
            if cache_key in self._component_cache:
                components_to_cleanup.append((cache_key, self._component_cache[cache_key]))
                del self._component_cache[cache_key]
                del self._cache_access_times[cache_key]
        
        # Release lock before calling cleanup methods
        cache_lock = self._cache_lock
        cache_lock.release()
        
        try:
            #  Call cleanup outside of lock to prevent deadlock
            for cache_key, component in components_to_cleanup:
                if hasattr(component, 'cleanup') and callable(component.cleanup):
                    try:
                        component.cleanup({})
                    except Exception as e:
                        self.logger.warning(
                            "COMPONENT_CLEANUP_ERROR",
                            extra={"cache_key": cache_key, "error": str(e)}
                        )
        finally:
            # Re-acquire lock
            cache_lock.acquire()
        
        self.logger.debug(
            "COMPONENT_CACHE_CLEANUP",
            extra={
                "removed_items": len(components_to_cleanup),
                "remaining_items": len(self._component_cache)
            }
        )

    def cleanup_execution_manager(self) -> None:
        """Thread-safe cleanup of all cached components and resources."""
        with self._cache_lock:
            for cache_key, component in self._component_cache.items():
                if hasattr(component, 'cleanup') and callable(component.cleanup):
                    try:
                        component.cleanup({})
                    except Exception as e:
                        self.logger.warning(
                            "COMPONENT_CLEANUP_ERROR",
                            extra={"cache_key": cache_key, "error": str(e)}
                        )
            
            self._component_cache.clear()
            self._cache_access_times.clear()
        
        self.logger.info("EXECUTION_MANAGER_CLEANUP_COMPLETE")