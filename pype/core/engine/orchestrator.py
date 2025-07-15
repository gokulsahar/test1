"""
DataPY Orchestrator - Main control brain with ready-queue algorithm and token-based scheduling.

Implements event-driven scheduling with dependency tokens, subjob coordination,
memory management, and proper error flow handling.
"""

import asyncio
import json
import logging
import time
from collections import defaultdict, deque
from pathlib import Path
from typing import Dict, Any, List, Set, Optional, Tuple

from pype.core.engine.execution_manager import ExecutionManager, ComponentResult, ExecutionError
from pype.core.engine.global_store import GlobalStore
from pype.core.engine.subjob_tracker import SubJobTracker, ComponentState
from pype.core.engine.templater import RuntimeTemplater, MissingGlobalVar
from pype.core.engine.job_config_handler import JobConfigHandler


class OrchestrationError(Exception):
    """Orchestration-level error with structured metadata."""
    
    def __init__(self, message: str, subjob_id: str = None, component: str = None):
        self.subjob_id = subjob_id
        self.component = component
        super().__init__(message)


class JobOrchestrator:
    """
    Main control brain implementing ready-queue algorithm and job coordination.
    
    Coordinates subjob execution using dependency tokens, manages parallel execution,
    handles error flow, and provides checkpointing integration points.
    """
    
    def __init__(self, job_folder: Path, context_name: str = None):
        """
        Initialize orchestrator with job folder and context.
        
        Args:
            job_folder: Path to job folder with JSON artifacts
            context_name: Optional context name for template resolution
        """
        self.job_folder = Path(job_folder)
        self.context_name = context_name
        
        # Core components (initialized in setup)
        self.logger: Optional[logging.Logger] = None
        self.job_config_handler: Optional[JobConfigHandler] = None
        self.global_store: Optional[GlobalStore] = None
        self.execution_manager: Optional[ExecutionManager] = None
        self.runtime_templater: Optional[RuntimeTemplater] = None
        
        # Execution metadata (loaded from job folder)
        self.execution_metadata: Dict[str, Any] = {}
        self.subjob_components: Dict[str, List[str]] = {}
        self.subjob_execution_order: List[str] = []
        self.dependency_tokens: Dict[str, List[str]] = {}
        
        # Runtime state
        self.subjob_trackers: Dict[str, SubJobTracker] = {}
        self.fired_tokens: Set[str] = set()
        self.running_subjobs: Set[str] = set()
        self.completed_subjobs: Set[str] = set()
        self.failed_subjobs: Set[str] = set()
        
        # Task management for subjob coordination
        self.subjob_tasks: Dict[str, asyncio.Task] = {}
        self.completion_queue: asyncio.Queue[str] = asyncio.Queue()
        
        # Token dependency tracking for ready-queue algorithm
        self.subjob_waiting_tokens: Dict[str, Set[str]] = {}
        
    async def execute_job(self) -> Dict[str, Any]:
        """
        Execute complete job using ready-queue algorithm.
        
        Returns:
            Job execution summary with metrics and status
        """
        job_start_time = time.perf_counter()
        
        try:
            # Setup phase
            await self._setup_execution_environment()
            
            # Resume support (placeholder for checkpoint recovery)
            await self._handle_resume_if_needed()
            
            # Initialize ready queue
            self._initialize_ready_queue()
            
            # Main execution loop
            execution_result = await self._execute_ready_queue_algorithm()
            
            # Success cleanup
            await self._cleanup_successful_execution()
            
            job_duration = (time.perf_counter() - job_start_time) * 1000
            
            return {
                'status': 'SUCCESS',
                'execution_time_ms': job_duration,
                'subjobs_executed': len(self.completed_subjobs),
                'total_subjobs': len(self.subjob_components),
                'final_tokens': list(self.fired_tokens)
            }
            
        except Exception as e:
            job_duration = (time.perf_counter() - job_start_time) * 1000
            
            self.logger.error(
                "JOB_EXECUTION_FAILED",
                extra={
                    "error": str(e),
                    "error_type": type(e).__name__,
                    "execution_time_ms": job_duration,
                    "completed_subjobs": list(self.completed_subjobs),
                    "failed_subjobs": list(self.failed_subjobs)
                }
            )
            
            await self._cleanup_failed_execution()
            
            return {
                'status': 'FAILED',
                'error': str(e),
                'execution_time_ms': job_duration,
                'subjobs_completed': len(self.completed_subjobs),
                'subjobs_failed': len(self.failed_subjobs)
            }
    
    async def _setup_execution_environment(self) -> None:
        """Setup all execution components with template resolution."""
        job_name = self.job_folder.name
        
        # Load metadata from job folder
        await self._load_job_metadata()
        
        # Setup logging (placeholder - would integrate with observability module)
        self.logger = logging.getLogger(f"datapy.orchestrator.{job_name}")
        
        # Initialize JobConfigHandler
        job_metadata = self.execution_metadata['job_config']
        self.job_config_handler = JobConfigHandler(
            job_config=job_metadata,
            job_metadata={'name': job_name},
            logger=self.logger
        )
        
        # Setup template resolution
        self.runtime_templater = RuntimeTemplater(self.job_folder, self.context_name)
        
        # Resolve templates in execution metadata
        self.execution_metadata = self.runtime_templater.resolve_execution_metadata_templates(
            self.execution_metadata
        )
        
        # Initialize GlobalStore
        self.global_store = GlobalStore(self.logger)
        
        # Initialize ExecutionManager
        self.execution_manager = ExecutionManager(
            execution_metadata=self.execution_metadata,
            global_store=self.global_store,
            threadpool=self.job_config_handler.get_threadpool(),
            logger=self.logger,
            job_folder=self.job_folder
        )
        
        # Initialize SubJobTrackers
        self._initialize_subjob_trackers()
        
        self.logger.info(
            "ORCHESTRATOR_SETUP_COMPLETE",
            extra={
                "job_name": job_name,
                "context": self.context_name,
                "total_subjobs": len(self.subjob_components),
                "total_dependency_tokens": sum(len(tokens) for tokens in self.dependency_tokens.values())
            }
        )
    
    async def _load_job_metadata(self) -> None:
        """Load execution metadata and subjob information from job folder."""
        job_name = self.job_folder.name
        
        # Load execution metadata
        exec_metadata_file = self.job_folder / f"{job_name}_execution_metadata.json"
        with open(exec_metadata_file, 'r') as f:
            self.execution_metadata = json.load(f)
        
        # Load subjob metadata
        subjob_metadata_file = self.job_folder / f"{job_name}_subjob_metadata.json"
        with open(subjob_metadata_file, 'r') as f:
            subjob_data = json.load(f)
            self.subjob_components = subjob_data['components']
            self.subjob_execution_order = subjob_data['execution_order']
        
        # Extract dependency tokens
        self.dependency_tokens = self.execution_metadata.get('dependency_tokens', {})
    
    def _initialize_subjob_trackers(self) -> None:
        """Initialize SubJobTracker for each subjob."""
        job_config = self.execution_metadata.get('job_config', {})
        fail_strategy = job_config.get('fail_strategy', 'halt')
        
        for subjob_id, components in self.subjob_components.items():
            self.subjob_trackers[subjob_id] = SubJobTracker(
                subjob_id=subjob_id,
                components=set(components),  # Convert to set
                fail_strategy=fail_strategy,
                logger=self.logger
            )
            
        
    
    def _initialize_ready_queue(self) -> None:
        """Initialize ready queue with subjobs that have no dependencies."""
        # Copy dependency tokens for runtime tracking
        self.subjob_waiting_tokens = {
            subjob_id: set(tokens) for subjob_id, tokens in self.dependency_tokens.items()
        }
        
        # Find subjobs with no dependencies (ready to start)
        ready_subjobs = [
            subjob_id for subjob_id, tokens in self.subjob_waiting_tokens.items()
            if not tokens
        ]
        
        self.logger.info(
            "READY_QUEUE_INITIALIZED",
            extra={
                "ready_subjobs": ready_subjobs,
                "waiting_subjobs": [
                    subjob_id for subjob_id, tokens in self.subjob_waiting_tokens.items() if tokens
                ],
                "total_tokens": sum(len(tokens) for tokens in self.subjob_waiting_tokens.values())
            }
        )
    
    async def _execute_ready_queue_algorithm(self) -> Dict[str, Any]:
        """
        Execute ready-queue algorithm with event-driven scheduling.
        
        Algorithm:
        1. Launch all ready subjobs in parallel
        2. Wait for subjob completion events
        3. Clear tokens generated by completed subjobs
        4. Move newly-ready subjobs to ready queue
        5. Repeat until all subjobs complete or error
        """
        ready_queue = deque([
            subjob_id for subjob_id, tokens in self.subjob_waiting_tokens.items()
            if not tokens
        ])
        
        while ready_queue or self.running_subjobs:
            # Launch all ready subjobs
            while ready_queue:
                subjob_id = ready_queue.popleft()
                await self._launch_subjob(subjob_id)
            
            # Wait for at least one subjob to complete
            if self.running_subjobs:
                completed_subjob_id = await self._wait_for_subjob_completion()
                
                # Process subjob completion
                tokens_generated = await self._process_subjob_completion(completed_subjob_id)
                
                # Clear tokens and find newly ready subjobs
                newly_ready = self._clear_tokens_and_find_ready(tokens_generated)
                ready_queue.extend(newly_ready)
        
        return {'ready_queue_complete': True}
    
    async def _launch_subjob(self, subjob_id: str) -> None:
        """Launch subjob execution as async task with proper tracking."""
        self.running_subjobs.add(subjob_id)
        
        # Get context for this subjob
        context = self.job_config_handler.update_context(subjob_id=subjob_id, attempt=1)
        
        # Create async task for subjob execution
        task = asyncio.create_task(
            self._execute_subjob_with_error_handling(subjob_id, context)
        )
        
        # Store task reference for lifecycle management
        self.subjob_tasks[subjob_id] = task
        
        # Register completion callback with captured subjob_id
        task.add_done_callback(
            lambda t, sid=subjob_id: asyncio.create_task(self._on_subjob_done(sid, t))
        )
        
        self.logger.info(
            "SUBJOB_LAUNCHED",
            extra={"subjob_id": subjob_id, "running_subjobs": len(self.running_subjobs)}
        )
    
    async def _on_subjob_done(self, subjob_id: str, task: asyncio.Task) -> None:
        """Handle subjob task completion with proper resource cleanup."""
        try:
            # Retrieve result to silence "Task exception was never retrieved" warning
            _ = task.result()
            
            self.logger.debug(
                "SUBJOB_TASK_COMPLETED",
                extra={"subjob_id": subjob_id, "success": True}
            )
            
        except Exception as exc:
            self.logger.error(
                "SUBJOB_TASK_FAILED", 
                extra={
                    "subjob_id": subjob_id, 
                    "error": str(exc),
                    "error_type": type(exc).__name__
                }
            )
        
        finally:
            # Clean up task reference
            self.subjob_tasks.pop(subjob_id, None)
            
            # Notify completion queue (regardless of success/failure)
            await self.completion_queue.put(subjob_id)
    
    
    async def _execute_subjob_with_error_handling(self, subjob_id: str, context: Dict[str, Any]) -> None:
        """Execute subjob with comprehensive error handling."""
        subjob_tracker = self.subjob_trackers[subjob_id]
        
        try:
            await self.execution_manager.execute_subjob(
                subjob_id, context, subjob_tracker, self.fired_tokens
            )
            
            # Mark subjob as completed
            self.completed_subjobs.add(subjob_id)
            
        except ExecutionError as e:
            # Handle component execution error (including MissingGlobalVar)
            if isinstance(e.original_error, MissingGlobalVar):
                # Specific handling for missing global variables
                self.logger.error(
                    "MISSING_GLOBAL_VARIABLE",
                    extra={
                        "subjob_id": subjob_id,
                        "component": e.component,
                        "global_key": e.original_error.global_key,
                        "error_type": "MissingGlobalVar"
                    }
                )
            else:
                # General component error logging
                self.logger.error(
                    "COMPONENT_EXECUTION_ERROR",
                    extra={
                        "subjob_id": subjob_id,
                        "component": e.component,
                        "error": str(e.original_error or e),
                        "error_type": type(e.original_error or e).__name__
                    }
                )
            
            # Generate component error tokens
            component_error_tokens = await self._handle_execution_error(subjob_id, e)
            
            # Check if component error is handled by downstream subjobs
            if self._has_error_handler(component_error_tokens):
                self.logger.info(
                    "COMPONENT_ERROR_HANDLED",
                    extra={
                        "subjob_id": subjob_id,
                        "component": e.component,
                        "error_tokens": component_error_tokens
                    }
                )
                # Add component error tokens to fired tokens
                self.fired_tokens.update(component_error_tokens)
            else:
                # Component error not handled - subjob fails
                self.failed_subjobs.add(subjob_id)
                subjob_error_tokens = await self._handle_subjob_error(subjob_id, e)
                
                # Check if subjob error is handled
                if self._has_error_handler(subjob_error_tokens):
                    self.logger.info(
                        "SUBJOB_ERROR_HANDLED",
                        extra={
                            "subjob_id": subjob_id,
                            "component": e.component,
                            "subjob_error_tokens": subjob_error_tokens
                        }
                    )
                    # Add subjob error tokens to fired tokens
                    self.fired_tokens.update(subjob_error_tokens)
                    self.fired_tokens.update(component_error_tokens)  # Also fire component error
                else:
                    # No subjob error handler - propagate error to stop job
                    if isinstance(e.original_error, MissingGlobalVar):
                        # Special message for missing global variables
                        raise OrchestrationError(
                            f"Critical error: Missing global variable '{e.original_error.global_key}' "
                            f"in component '{e.component}' of subjob '{subjob_id}'. "
                            f"This indicates a dependency issue in job execution order.",
                            subjob_id=subjob_id,
                            component=e.component
                        )
                    else:
                        # General unhandled subjob error
                        raise OrchestrationError(
                            f"Unhandled subjob error in {subjob_id}: {e}",
                            subjob_id=subjob_id,
                            component=e.component
                        )
        
        except Exception as e:
            # Unexpected error
            self.failed_subjobs.add(subjob_id)
            raise OrchestrationError(
                f"Unexpected error in subjob {subjob_id}: {e}",
                subjob_id=subjob_id
            )
        
        finally:
            # Ensure subjob is removed from running set regardless of outcome
            self.running_subjobs.discard(subjob_id)
    
    async def _handle_execution_error(self, subjob_id: str, error: ExecutionError) -> List[str]:
        """
        Handle execution error and generate error tokens.
        
        Returns:
            List of error tokens generated
        """
        error_tokens = [f"{error.component}::error"]
        
        self.logger.error(
            "COMPONENT_ERROR",
            extra={
                "subjob_id": subjob_id,
                "component": error.component,
                "error": str(error.original_error or error),
                "error_tokens": error_tokens
            }
        )
        
        return error_tokens
    
    async def _handle_subjob_error(self, subjob_id: str, error: ExecutionError) -> List[str]:
        """
        Handle subjob error and generate SUBJOB_ERR token.
        
        Called when a component error is not handled within the subjob.
        
        Returns:
            List of subjob error tokens generated
        """
        subjob_error_tokens = [f"SUBJOB_ERR::{subjob_id}"]
        
        # Mark subjob tracker as failed and fire error token
        subjob_tracker = self.subjob_trackers[subjob_id]
        if subjob_tracker.fire_err():
            self.logger.error(
                "SUBJOB_ERROR",
                extra={
                    "subjob_id": subjob_id,
                    "component": error.component,
                    "error": str(error.original_error or error),
                    "subjob_error_tokens": subjob_error_tokens
                }
            )
        return subjob_error_tokens
        
    def _has_error_handler(self, error_tokens: List[str]) -> bool:
        """Check if any subjob is waiting for the error tokens."""
        for token in error_tokens:
            for subjob_tokens in self.subjob_waiting_tokens.values():
                if token in subjob_tokens:
                    return True
        return False
    
    async def _wait_for_subjob_completion(self) -> str:
        """Wait for at least one subjob to complete and return its ID."""
        return await self.completion_queue.get()
    
    async def _process_subjob_completion(self, subjob_id: str) -> List[str]:
        """
        Process subjob completion and generate appropriate tokens.
        
        Returns:
            List of tokens generated (SUBJOB_OK or SUBJOB_ERR)
        """
        self.running_subjobs.discard(subjob_id)
        subjob_tracker = self.subjob_trackers[subjob_id]
        
        tokens_generated = []
        
        # Check if subjob fires SUBJOB_OK
        if subjob_tracker.fire_ok():
            token = f"SUBJOB_OK::{subjob_id}"
            tokens_generated.append(token)
            self.fired_tokens.add(token)
            
            self.logger.info(
                "SUBJOB_OK_FIRED",
                extra={"subjob_id": subjob_id, "token": token}
            )
        
        # Check if subjob fires SUBJOB_ERR
        if subjob_tracker.fire_err():
            token = f"SUBJOB_ERR::{subjob_id}"
            tokens_generated.append(token)
            self.fired_tokens.add(token)
            
            self.logger.info(
                "SUBJOB_ERR_FIRED",
                extra={"subjob_id": subjob_id, "token": token}
            )
        
        return tokens_generated
    
    def _clear_tokens_and_find_ready(self, tokens_generated: List[str]) -> List[str]:
        """
        Clear generated tokens from waiting sets and find newly ready subjobs.
        
        Args:
            tokens_generated: List of tokens that were just fired
            
        Returns:
            List of newly ready subjob IDs
        """
        newly_ready = []
        
        for token in tokens_generated:
            # Remove token from all subjob waiting sets
            for subjob_id, waiting_tokens in self.subjob_waiting_tokens.items():
                if token in waiting_tokens:
                    waiting_tokens.discard(token)
                    
                    # Check if subjob is now ready (no more waiting tokens)
                    if not waiting_tokens and subjob_id not in self.running_subjobs and subjob_id not in self.completed_subjobs:
                        newly_ready.append(subjob_id)
        
        if newly_ready:
            self.logger.info(
                "TOKENS_CLEARED",
                extra={
                    "tokens_cleared": tokens_generated,
                    "newly_ready_subjobs": newly_ready
                }
            )
        
        return newly_ready
    
    
    async def _handle_resume_if_needed(self) -> None:
        """Handle job resume from checkpoint if needed."""
        # Placeholder for checkpoint recovery
        # Would check for existing checkpoint files and restore state
        pass
    
    async def _cleanup_successful_execution(self) -> None:
        """Cleanup after successful job execution."""
        # Clean up ExecutionManager
        if self.execution_manager:
            self.execution_manager.cleanup_execution_manager()
        
        if self.job_config_handler:
            self.job_config_handler.shutdown()
        
        self.logger.info(
            "JOB_EXECUTION_SUCCESS",
            extra={
                "subjobs_completed": len(self.completed_subjobs),
                "total_tokens_fired": len(self.fired_tokens)
            }
        )
    
    async def _cleanup_failed_execution(self) -> None:
        """Cleanup after failed job execution."""
        # Cancel any running subjob tasks
        for subjob_id, task in self.subjob_tasks.items():
            if not task.done():
                task.cancel()
                self.logger.debug(
                    "SUBJOB_TASK_CANCELLED",
                    extra={"subjob_id": subjob_id}
                )
        
        # Clear task references
        self.subjob_tasks.clear()
        
        if self.job_config_handler:
            self.job_config_handler.shutdown()
        
        self.logger.error(
            "JOB_EXECUTION_CLEANUP",
            extra={
                "completed_subjobs": list(self.completed_subjobs),
                "failed_subjobs": list(self.failed_subjobs),
                "running_subjobs": list(self.running_subjobs)
            }
        )


async def execute_job_folder(job_folder: Path, context_name: str = None) -> Dict[str, Any]:
    """
    Convenience function to execute a job folder.
    
    Args:
        job_folder: Path to job folder with JSON artifacts
        context_name: Optional context name for template resolution
        
    Returns:
        Job execution result summary
    """
    orchestrator = JobOrchestrator(job_folder, context_name)
    return await orchestrator.execute_job()