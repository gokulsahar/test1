"""
Thread-safe GlobalStore and BufferedStore for DataPY engine.

Provides immutable global variable management with fine-grained concurrency
and forEach-aware buffering for optimal performance.
"""

import json
import threading
import time
from typing import Any, Dict, Optional, Union
from collections import defaultdict
import logging


class GlobalStoreError(Exception):
    """Base exception for GlobalStore operations."""
    pass


class GlobalStore:
    """
    Thread-safe global variable store with extension-only semantics.
    
    Supports high-concurrency access with fine-grained locking for optimal
    performance. Global variables are immutable once set - components can
    only add new variables, never replace existing ones.
    """
    
    def __init__(self, logger: logging.Logger):
        """
        Initialize GlobalStore with logger for structured logging.
        
        Args:
            logger: Logger instance for structured event logging
        """
        self.logger = logger
        self._data: Dict[str, Any] = {}
        self._revision: int = 0
        self._locks: Dict[str, threading.RLock] = defaultdict(threading.RLock)
        self._global_lock = threading.RLock()
        
    def get(self, key: str, default: Any = None) -> Any:
        """
        Get global variable value (lock-free read).
        
        Args:
            key: Global variable key (component_name__variable_name format)
            default: Default value if key not found
            
        Returns:
            Deep copy of stored value or default
        """
        value = self._data.get(key, default)
        # Return deep copy to prevent accidental mutations
        if value is not None and isinstance(value, (dict, list)):
            import copy
            return copy.deepcopy(value)
        return value
    
    def set(self, key: str, value: Any, component: str = "unknown", mode: str = "replace") -> bool:
        """
        Set global variable value (extension-only, thread-safe).
        
        Args:
            key: Global variable key
            value: Value to store (must be JSON serializable)
            component: Component name for logging
            mode: "replace" or "accumulate" (spec requirement, replace is default)
            
        Returns:
            True if value was set, False if key already exists (immutable)
        """
        # Deep copy on write to prevent mutation
        import copy
        safe_value = copy.deepcopy(value) if isinstance(value, (dict, list, set)) else value
        
        # Fine-grained locking per key for optimal concurrency
        with self._locks[key]:
            if key in self._data:
                if mode == "accumulate" and isinstance(self._data[key], list) and isinstance(safe_value, list):
                    # Special case: accumulate lists (create new list, don't mutate)
                    self._data[key] = self._data[key] + safe_value
                else:
                    # Immutable global exists - reject
                    self.logger.warning(
                        "GLOBAL_SET_IGNORED",
                        extra={
                            "key": key,
                            "component": component,
                            "reason": "immutable_global_exists",
                            "existing_value_type": type(self._data[key]).__name__
                        }
                    )
                    return False
            else:
                # New key - store safe copy
                self._data[key] = safe_value
            
            # Validate value size (warn only, don't reject per spec)
            try:
                serialized_size = len(json.dumps(safe_value).encode('utf-8'))
                if serialized_size > 65536:  # 64KB
                    self.logger.warning(
                        "GLOBAL_SET_SIZE_WARNING",
                        extra={
                            "key": key,
                            "component": component,
                            "size_bytes": serialized_size,
                            "size_limit": 65536
                        }
                    )
            except (TypeError, ValueError) as e:
                self.logger.warning(
                    "GLOBAL_SET_SERIALIZATION_WARNING",
                    extra={
                        "key": key,
                        "component": component,
                        "error": str(e)
                    }
                )
            
            # Increment revision
            with self._global_lock:
                self._revision += 1
                current_revision = self._revision
            
            # Log successful set operation (required by spec)
            self.logger.info(
                "GLOBAL_SET",
                extra={
                    "key": key,
                    "component": component,
                    "value": safe_value,
                    "revision": current_revision,
                    "value_type": type(safe_value).__name__,
                    "mode": mode
                }
            )
            
            return True
    
    def revision(self) -> int:
        """
        Get current revision number (monotonic counter).
        
        Returns:
            Current revision number
        """
        return self._revision
    
    def dump(self) -> bytes:
        """
        Serialize entire GlobalStore state to JSON bytes.
        
        Used for checkpointing at subjob boundaries.
        
        Returns:
            JSON bytes containing revision and data
        """
        with self._global_lock:
            state = {
                "revision": self._revision,
                "data": dict(self._data)  # Create snapshot
            }
        
        try:
            return json.dumps(state, ensure_ascii=False).encode('utf-8')
        except (TypeError, ValueError) as e:
            self.logger.error(
                "GLOBAL_DUMP_ERROR",
                extra={"error": str(e), "data_keys": list(self._data.keys())}
            )
            raise GlobalStoreError(f"Failed to serialize GlobalStore: {e}")
    
    @classmethod
    def from_dump(cls, data: bytes, logger: logging.Logger) -> 'GlobalStore':
        """
        Restore GlobalStore from serialized state.
        
        Used for resume from checkpoint.
        
        Args:
            data: JSON bytes from previous dump()
            logger: Logger instance for new store
            
        Returns:
            Restored GlobalStore instance
        """
        try:
            state = json.loads(data.decode('utf-8'))
            
            # Create new instance
            store = cls(logger)
            store._revision = state.get("revision", 0)
            store._data = state.get("data", {})
            
            logger.info(
                "GLOBAL_RESTORE",
                extra={
                    "revision": store._revision,
                    "variables_count": len(store._data),
                    "variables": list(store._data.keys())
                }
            )
            
            return store
            
        except (json.JSONDecodeError, UnicodeDecodeError, KeyError) as e:
            logger.error("GLOBAL_RESTORE_ERROR", extra={"error": str(e)})
            raise GlobalStoreError(f"Failed to restore GlobalStore: {e}")
    
    def keys(self) -> list:
        """Get list of all global variable keys."""
        return list(self._data.keys())
    
    def __len__(self) -> int:
        """Get number of global variables."""
        return len(self._data)


class BufferedStore:
    """
    BufferedStore for forEach components with batched GlobalStore updates.
    
    Provides the same API as GlobalStore but buffers all set() operations
    in memory during forEach iterations. At iteration completion, flush()
    writes consolidated updates to the underlying GlobalStore.
    """
    
    def __init__(self, global_store: GlobalStore, logger: logging.Logger, component_name: str = "forEach"):
        """
        Initialize BufferedStore wrapping a GlobalStore.
        
        Args:
            global_store: Underlying GlobalStore instance
            logger: Logger for buffered operations
            component_name: forEach component name for context
        """
        self.global_store = global_store
        self.logger = logger
        self.component_name = component_name
        self._buffer: Dict[str, Any] = {}
        self._buffer_lock = threading.RLock()
        self._iteration_count = 0
        self._is_flushed = False
        
        # Nested forEach support
        self._parent_buffer: Optional['BufferedStore'] = None
        self._child_buffers: Dict[str, 'BufferedStore'] = {}
    
    def end_iteration(self) -> None:
        """End current forEach iteration context."""
        self.logger.debug(
            "FOREACH_ITERATION_END",
            extra={
                "forEach_component": self.component_name,
                "iteration": self._iteration_count
            }
        )
        
    
    def get(self, key: str, default: Any = None) -> Any:
        """
        Get global variable value (thread-safe with selective locking).
        
        Args:
            key: Global variable key (component_name__variable_name format)
            default: Default value if key not found
            
        Returns:
            Deep copy of stored value or default
        """
        # Quick check without lock for non-existent keys
        if key not in self._data:
            return default
        
        # For mutable objects, acquire lock to prevent race with accumulate mode
        value = self._data[key]
        if isinstance(value, (dict, list)):
            with self._locks[key]:
                # Re-check after acquiring lock (double-checked locking pattern)
                value = self._data.get(key, default)
                if value is not None and isinstance(value, (dict, list)):
                    import copy
                    return copy.deepcopy(value)
                return value
        else:
            # Immutable types (int, str, float, bool, None) are safe to return directly
            return value
    
    def set(self, key: str, value: Any, component: str = "forEach_component", mode: str = "replace") -> bool:
        """
        Buffer set operation for later flush to GlobalStore.
        
        Args:
            key: Global variable key
            value: Value to buffer
            component: Component name for logging
            mode: "replace" or "accumulate"
            
        Returns:
            True (always succeeds in buffer)
        """
        with self._buffer_lock:
            if mode == "accumulate" and key in self._buffer and isinstance(self._buffer[key], list) and isinstance(value, list):
                # Accumulate lists in buffer
                self._buffer[key].extend(value)
            else:
                # Store/replace in buffer
                self._buffer[key] = value
            
            # Log buffered operation
            self.logger.debug(
                "GLOBAL_SET_BUFFERED",
                extra={
                    "key": key,
                    "component": component,
                    "forEach_component": self.component_name,
                    "iteration": self._iteration_count,
                    "value_type": type(value).__name__,
                    "mode": mode
                }
            )
            
            return True
    
    def flush(self) -> Dict[str, bool]:
        """
        Flush all buffered operations to GlobalStore.
        
        Called at forEach iteration completion to consolidate updates.
        
        Returns:
            Dict mapping keys to success status
        """
        if self._is_flushed:
            self.logger.warning(
                "BUFFERED_STORE_ALREADY_FLUSHED",
                extra={"forEach_component": self.component_name}
            )
            return {}
        
        results = {}
        
        with self._buffer_lock:
            # Flush nested buffers first (inner to outer)
            for child_name, child_buffer in self._child_buffers.items():
                child_buffer.flush()
            
            # Flush this buffer to parent or GlobalStore
            target_store = self._parent_buffer if self._parent_buffer else self.global_store
            
            for key, value in self._buffer.items():
                success = target_store.set(
                    key, 
                    value, 
                    component=f"{self.component_name}_iteration_{self._iteration_count}",
                    mode="replace"  # Iterator summary always replaces
                )
                results[key] = success
            
            # Log flush operation
            self.logger.info(
                "BUFFERED_STORE_FLUSHED",
                extra={
                    "forEach_component": self.component_name,
                    "iteration": self._iteration_count,
                    "variables_flushed": len(self._buffer),
                    "successful_sets": sum(results.values()),
                    "failed_sets": len(results) - sum(results.values()),
                    "target": "parent_buffer" if self._parent_buffer else "global_store"
                }
            )
            
            # Clear buffer and mark as flushed
            self._buffer.clear()
            self._is_flushed = True
        
        return results
    
    def start_iteration(self, iteration_data: Dict[str, Any] = None) -> None:
        """
        Start new forEach iteration context.
        
        Args:
            iteration_data: Optional iteration context data
        """
        with self._buffer_lock:
            # Clear any stale buffer from previous failed iterations
            self._buffer.clear()
            
            self._iteration_count += 1
            self._is_flushed = False
            
            self.logger.debug(
                "FOREACH_ITERATION_START",
                extra={
                    "forEach_component": self.component_name,
                    "iteration": self._iteration_count,
                    "context_keys": list(iteration_data.keys()) if iteration_data else [],
                    "buffer_cleared": True
                }
            )
    
    def create_nested_buffer(self, nested_component_name: str) -> 'BufferedStore':
        """
        Create nested BufferedStore for inner forEach components.
        
        Args:
            nested_component_name: Name of nested forEach component
            
        Returns:
            Nested BufferedStore instance
        """
        nested_buffer = BufferedStore(
            self.global_store, 
            self.logger, 
            nested_component_name
        )
        nested_buffer._parent_buffer = self
        
        with self._buffer_lock:
            self._child_buffers[nested_component_name] = nested_buffer
        
        self.logger.debug(
            "NESTED_BUFFER_CREATED",
            extra={
                "parent_forEach": self.component_name,
                "nested_forEach": nested_component_name,
                "nesting_depth": self._get_nesting_depth() + 1
            }
        )
        
        return nested_buffer
    
    def _get_nesting_depth(self) -> int:
        """Calculate nesting depth for this BufferedStore."""
        depth = 0
        current = self._parent_buffer
        while current is not None:
            depth += 1
            current = current._parent_buffer
        return depth
    
    def revision(self) -> int:
        """Get revision from underlying GlobalStore."""
        return self.global_store.revision()
    
    def keys(self) -> list:
        """Get combined keys from buffer and GlobalStore."""
        with self._buffer_lock:
            buffer_keys = set(self._buffer.keys())
        global_keys = set(self.global_store.keys())
        return list(buffer_keys | global_keys)
    
    def __len__(self) -> int:
        """Get total number of variables (buffer + GlobalStore)."""
        return len(self.keys())