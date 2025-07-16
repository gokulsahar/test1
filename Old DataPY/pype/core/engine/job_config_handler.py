"""
Job Configuration Handler for DataPY engine.

Manages all job_config settings including threadpool, dask client,
timeouts, and context creation with comprehensive dynamic configuration.
"""

import os
import logging
from concurrent.futures import ThreadPoolExecutor
from typing import Dict, Any, Optional, Union
from types import MappingProxyType

from pype.core.utils.util_methods import (
    parse_memory_size, 
    validate_positive_integer,
    validate_string_enum,
    generate_run_id,
    get_system_info,
    UtilityError
)

from pype.core.utils.constants import (
    DEFAULT_MAX_WORKERS,
    DEFAULT_TIMEOUT, 
    DEFAULT_RETRIES,
    DEFAULT_CHUNK_SIZE,
    DEFAULT_MEMORY_PER_WORKER,
    DEFAULT_THREADS_PER_WORKER,
    DEFAULT_DASK_WORKERS
)




class JobConfigError(Exception):
    """Exception for job configuration errors."""
    pass


class JobConfigHandler:
    """
    Handles job configuration setup and resource management.
    
    Creates and manages threadpool, dask client, validation, and context
    creation with full support for dynamic job_config fields.
    """
    
    def __init__(self, job_config: Dict[str, Any], job_metadata: Dict[str, Any], logger: logging.Logger):
        """
        Initialize JobConfigHandler with configuration and logging.
        
        Args:
            job_config: Complete job_config dictionary from job folder
            job_metadata: Job metadata (name, version, team, owner, created)
            logger: Logger instance for structured logging
        """
        self.logger = logger
        self.job_config = job_config.copy()
        self.job_metadata = job_metadata.copy()
        
        # Resource containers
        self._threadpool: Optional[ThreadPoolExecutor] = None
        self._dask_client = None
        self._context: Optional[MappingProxyType] = None
        self._system_info = get_system_info()
        
        # Run identifier
        self.run_id = generate_run_id(job_metadata.get('name', 'unknown_job'))
        
        # Validate configuration
        self._validate_configuration()
        
        # Initialize resources
        self._initialize_resources()
        
        # Create immutable context
        self._create_context()
        
        self.logger.info(
            "JOB_CONFIG_INITIALIZED",
            extra={
                "run_id": self.run_id,
                "execution_mode": self.job_config.get('execution_mode', 'pandas'),
                "job_name": job_metadata.get('name'),
                "job_version": job_metadata.get('version'),
                "config_fields": list(self.job_config.keys())
            }
        )
    
    def get_execution_mode(self) -> str:
        """Get job execution mode (pandas or dask)."""
        return self.job_config.get('execution_mode', 'pandas')

    
    def _validate_configuration(self) -> None:
        """Validate job configuration with extensible validation methods."""
        validation_errors = []
        
        try:
            self._validate_basic_fields(validation_errors)
            self._validate_execution_config(validation_errors)
            self._validate_memory_sizes(validation_errors)
            # TODO: Add more validation methods here as needed and move validations to separate py?
            
        except Exception as e:
            validation_errors.append(f"Validation system error: {str(e)}")
        
        if validation_errors:
            error_msg = f"Job configuration validation failed: {'; '.join(validation_errors)}"
            self.logger.error(
                "JOB_CONFIG_VALIDATION_FAILED",
                extra={
                    "run_id": self.run_id,
                    "validation_errors": validation_errors,
                    "config": self.job_config
                }
            )
            raise JobConfigError(error_msg)
    
    def _validate_basic_fields(self, errors: list) -> None:
        """Validate basic job_config fields."""
        try:
            # Retries validation
            retries = self.job_config.get('retries', DEFAULT_RETRIES)
            validate_positive_integer(retries, 'retries', min_val=0, max_val=3)
            
            # Timeout validation
            timeout = self.job_config.get('timeout', DEFAULT_TIMEOUT)
            validate_positive_integer(timeout, 'timeout', min_val=1)
            
            # Fail strategy validation
            fail_strategy = self.job_config.get('fail_strategy', 'halt')
            validate_string_enum(fail_strategy, 'fail_strategy', ['halt', 'continue'])
            
            # Execution mode validation
            execution_mode = self.job_config.get('execution_mode', 'pandas')
            validate_string_enum(execution_mode, 'execution_mode', ['pandas', 'dask'])
            
        except UtilityError as e:
            errors.append(str(e))
    
    def _validate_execution_config(self, errors: list) -> None:
        """Validate execution configuration (threadpool, dask)."""
        execution_config = self.job_config.get('execution', {})
        
        # Threadpool validation
        if 'threadpool' in execution_config:
            try:
                threadpool_config = execution_config['threadpool']
                max_workers = threadpool_config.get('max_workers', DEFAULT_MAX_WORKERS)
                validate_positive_integer(max_workers, 'max_workers', min_val=1, max_val=128)
                
                # Warn if max_workers exceeds CPU count
                cpu_count = self._system_info.get('cpu_count', 1)
                if max_workers > cpu_count * 2:
                    self.logger.warning(
                        "THREADPOOL_HIGH_WORKERS",
                        extra={
                            "max_workers": max_workers,
                            "cpu_count": cpu_count,
                            "recommendation": "Consider reducing max_workers"
                        }
                    )
            except UtilityError as e:
                errors.append(f"Threadpool config: {str(e)}")
        
        # Dask validation
        if 'dask' in execution_config:
            try:
                dask_config = execution_config['dask']
                
                if 'cluster_workers' in dask_config:
                    cluster_workers = dask_config['cluster_workers']
                    validate_positive_integer(cluster_workers, 'cluster_workers', min_val=1, max_val=1024)
                
                if 'threads_per_worker' in dask_config:
                    threads_per_worker = dask_config['threads_per_worker']
                    validate_positive_integer(threads_per_worker, 'threads_per_worker', min_val=1, max_val=32)
                
            except UtilityError as e:
                errors.append(f"Dask config: {str(e)}")
    
    def _validate_memory_sizes(self, errors: list) -> None:
        """Validate memory size configurations."""
        # Validate chunk_size
        chunk_size = self.job_config.get('chunk_size', DEFAULT_CHUNK_SIZE)
        try:
            parse_memory_size(chunk_size)
        except UtilityError as e:
            errors.append(f"chunk_size: {str(e)}")
        
        # Validate dask memory_per_worker
        execution_config = self.job_config.get('execution', {})
        if 'dask' in execution_config:
            dask_config = execution_config['dask']
            memory_per_worker = dask_config.get('memory_per_worker', DEFAULT_MEMORY_PER_WORKER)
            try:
                parse_memory_size(memory_per_worker)
            except UtilityError as e:
                errors.append(f"memory_per_worker: {str(e)}")
    
    def _initialize_resources(self) -> None:
        """Initialize threadpool and dask client based on configuration."""
        execution_mode = self.get_execution_mode()
        
        # Initialize threadpool
        self._initialize_threadpool()
        
        # Initialize dask client if in dask mode
        if execution_mode == 'dask':
            self._initialize_dask_client()
    
    def _initialize_threadpool(self) -> None:
        """Initialize shared threadpool for job execution."""
        execution_config = self.job_config.get('execution', {})
        threadpool_config = execution_config.get('threadpool', {})
        
        max_workers = threadpool_config.get('max_workers', DEFAULT_MAX_WORKERS)
        
        self._threadpool = ThreadPoolExecutor(
            max_workers=max_workers,
            thread_name_prefix=f"datapy_{self.run_id}"
        )
        
        self.logger.info(
            "THREADPOOL_INITIALIZED",
            extra={
                "run_id": self.run_id,
                "max_workers": max_workers,
                "cpu_count": self._system_info.get('cpu_count')
            }
        )
    
    def _initialize_dask_client(self) -> None:
        """Initialize dask client for distributed execution."""
        execution_config = self.job_config.get('execution', {})
        dask_config = execution_config.get('dask', {})
        
        try:
            # Check if cluster_uri is provided (remote cluster)
            cluster_uri = dask_config.get('cluster_uri')
            
            if cluster_uri:
                # Connect to remote cluster
                import dask.distributed
                self._dask_client = dask.distributed.Client(address=cluster_uri)
                
                self.logger.info(
                    "DASK_REMOTE_CLIENT_CONNECTED",
                    extra={
                        "run_id": self.run_id,
                        "cluster_uri": cluster_uri,
                        "scheduler_info": str(self._dask_client.scheduler_info())
                    }
                )
            else:
                # Create local cluster
                import dask.distributed
                
                cluster_workers = dask_config.get('cluster_workers', 2)
                threads_per_worker = dask_config.get('threads_per_worker', 2)
                memory_per_worker = dask_config.get('memory_per_worker', '4GB')
                
                # Parse memory to bytes for dask
                memory_bytes = parse_memory_size(memory_per_worker)
                
                # Pass original string to Dask, not bytes format
                # Dask accepts "4GB" but not "4294967296B"
                memory_limit = memory_per_worker  # Use original string format
                
                # Check for memory overcommit and warn
                total_memory_bytes = memory_bytes * cluster_workers
                self._check_memory_overcommit(total_memory_bytes, cluster_workers, memory_per_worker)
                
                # Create local cluster
                cluster = dask.distributed.LocalCluster(
                    n_workers=cluster_workers,
                    threads_per_worker=threads_per_worker,
                    memory_limit=memory_limit,
                    silence_logs=False
                )
                
                self._dask_client = dask.distributed.Client(cluster)
                
                self.logger.info(
                    "DASK_LOCAL_CLUSTER_CREATED",
                    extra={
                        "run_id": self.run_id,
                        "cluster_workers": cluster_workers,
                        "threads_per_worker": threads_per_worker,
                        "memory_per_worker": memory_per_worker,
                        "dashboard_link": self._dask_client.dashboard_link
                    }
                )
                
        except Exception as e:
            error_msg = f"Failed to initialize Dask client: {str(e)}"
            self.logger.error(
                "DASK_INITIALIZATION_FAILED",
                extra={
                    "run_id": self.run_id,
                    "error": str(e),
                    "dask_config": dask_config
                }
            )
            raise JobConfigError(error_msg)
    
    def _create_context(self) -> None:
        """Create immutable execution context with all job configuration."""
        context_dict = {
            # Standard spec fields
            "execution_mode": self.job_config.get('execution_mode', 'pandas'),
            "run_id": self.run_id,
            "job": self.job_metadata.copy(),
            "subjob_id": None,  # Will be updated by orchestrator
            "attempt": 1,       # Will be updated by orchestrator
            "threadpool": {
                "max_workers": self._threadpool.max_workers if self._threadpool else DEFAULT_MAX_WORKERS
            }
        }
        
        # Add dask info if in dask mode
        if self.job_config.get('execution_mode', 'pandas') == 'dask' and self._dask_client:
            context_dict["dask"] = {
                "cluster_uri": getattr(self._dask_client, 'scheduler', {}).get('address', 'local'),
                "dashboard_link": getattr(self._dask_client, 'dashboard_link', None)
            }
        
        # Dynamically include ALL job_config fields
        for key, value in self.job_config.items():
            if key not in context_dict:  # Don't override standard fields
                context_dict[key] = value
            else:
                
                self.logger.warning(
                    "CONTEXT_FIELD_COLLISION",
                    extra={
                        "run_id": self.run_id,
                        "field": key,
                        "user_value": value,
                        "system_value": context_dict[key],
                        "action": "user_value_ignored"
                    }
                )
        
        
        context_dict = self._make_deeply_immutable(context_dict)
        
        # Create immutable context
        self._context = MappingProxyType(context_dict)
    
    def get_threadpool(self) -> ThreadPoolExecutor:
        """Get configured threadpool for component execution."""
        if not self._threadpool:
            raise JobConfigError("Threadpool not initialized")
        return self._threadpool
    
    def get_dask_client(self) -> Optional[object]:
        """Get dask client if in dask mode, None otherwise."""
        return self._dask_client
    
    def get_context(self) -> MappingProxyType:
        """
        Get immutable execution context with ALL job_config fields.
        
        Context contains:
        - Standard fields: execution_mode, run_id, job, subjob_id, attempt, threadpool, dask
        - ALL job_config fields: timeout, retries, fail_strategy, chunk_size, etc.
        - User custom fields: Any additional fields from job_config
        
        Access examples:
        - context['execution_mode'] 
        - context['timeout']
        - context['custom_db_pool_size']  # User-defined field
        """
        if not self._context:
            raise JobConfigError("Context not created")
        return self._context
    
    def update_context(self, subjob_id: str = None, attempt: int = None) -> MappingProxyType:
        """
        Create updated context with new subjob_id and/or attempt.
        
        Used by orchestrator to update context for subjob execution.
        
        Args:
            subjob_id: New subjob identifier
            attempt: New attempt number
            
        Returns:
            New immutable context with updated fields
        """
        context_dict = dict(self._context)
        
        if subjob_id is not None:
            context_dict["subjob_id"] = subjob_id
        if attempt is not None:
            context_dict["attempt"] = attempt
        
        return MappingProxyType(self._make_deeply_immutable(context_dict))
    
    def shutdown(self) -> None:
        """Clean up resources - threadpool and dask client."""
        shutdown_events = []
        
        # Shutdown threadpool
        if self._threadpool:
            try:
                # Handle Python version compatibility for cancel_futures
                import sys
                if sys.version_info >= (3, 9):
                    self._threadpool.shutdown(wait=True, cancel_futures=True)
                else:
                    self._threadpool.shutdown(wait=True)
                shutdown_events.append("threadpool_shutdown")
            except Exception as e:
                self.logger.error(
                    "THREADPOOL_SHUTDOWN_ERROR",
                    extra={"run_id": self.run_id, "error": str(e)}
                )
        
        # Shutdown dask client
        if self._dask_client:
            try:
                self._dask_client.close()
                shutdown_events.append("dask_client_closed")
            except Exception as e:
                self.logger.error(
                    "DASK_SHUTDOWN_ERROR", 
                    extra={"run_id": self.run_id, "error": str(e)}
                )
        
        self.logger.info(
            "JOB_CONFIG_SHUTDOWN",
            extra={
                "run_id": self.run_id,
                "shutdown_events": shutdown_events
            }
        )
    

    
    def _make_deeply_immutable(self, data: Any) -> Any:
        """
        Make nested dictionaries deeply immutable.
        
        Recursively wraps dictionaries with MappingProxyType to prevent
        mutation of nested context fields.
        """
        if isinstance(data, dict):
            immutable_dict = {}
            for key, value in data.items():
                immutable_dict[key] = self._make_deeply_immutable(value)
            return MappingProxyType(immutable_dict)
        elif isinstance(data, list):
            return tuple(self._make_deeply_immutable(item) for item in data)
        else:
            return data
    
    def _check_memory_overcommit(self, total_memory_bytes: int, cluster_workers: int, memory_per_worker: str) -> None:
        """
        Check for memory overcommit and emit warning.
        
        Warns if total Dask memory allocation exceeds reasonable system limits.
        """
        # Get system memory (approximate check)
        try:
            import psutil
            system_memory = psutil.virtual_memory().total
            
            if total_memory_bytes > system_memory * 0.8:  # 80% threshold
                self.logger.warning(
                    "MEMORY_OVERCOMMIT_WARNING",
                    extra={
                        "run_id": self.run_id,
                        "cluster_workers": cluster_workers,
                        "memory_per_worker": memory_per_worker,
                        "total_allocated_gb": round(total_memory_bytes / (1024**3), 2),
                        "system_memory_gb": round(system_memory / (1024**3), 2),
                        "recommendation": "Consider reducing memory_per_worker or cluster_workers"
                    }
                )
        except ImportError:
            # psutil not available - skip check
            pass
        except Exception as e:
            self.logger.debug(
                "MEMORY_CHECK_FAILED",
                extra={"run_id": self.run_id, "error": str(e)}
            )
    
    def recreate_dask_client_for_retry(self) -> bool:
        """
        TODO: Recreate Dask client for retry attempts
        
        When subjob retries fail due to cluster issues, recreating the client
        can help recover from temporary network or cluster problems.
        
        Returns:
            True if client was recreated successfully, False otherwise
            
        TODO: Implement retry-aware Dask client recreation
        """
        # TODO: Close existing client
        # TODO: Wait brief interval for cluster cleanup
        # TODO: Recreate client with same configuration
        # TODO: Validate new client connectivity
        # TODO: Update context with new client info
        # TODO: Log recreation event
        
        self.logger.info(
            "DASK_CLIENT_RETRY_RECREATION_STUB",
            extra={
                "run_id": self.run_id,
                "message": "TODO: Implement Dask client recreation for retries"
            }
        )
        return False