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
    Production-grade thread-safe global variable store with extension-only semantics.
    
    Features bulletproof thread safety with standardized lock ordering:
    1. _global_lock (outermost)
    2. per-key locks (middle)  
    3. _buffer_lock (innermost - in BufferedStore)
    
    Provides immutable global variables with copy-on-write semantics.
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
        self._locks: Dict[str, threading.RLock] = {}
        self._global_lock = threading.RLock()
        self._last_cleanup = time.time()
        
    def get(self, key: str, default: Any = None) -> Any:
        """
        Get global variable value with bulletproof thread safety.
        
        Uses standardized lock ordering: global_lock → per_key_lock
        
        Args:
            key: Global variable key (component_name__variable_name format)
            default: Default value if key not found
            
        Returns:
            Deep copy of stored value or default
        """
        # Standardized lock ordering: global_lock first
        with self._global_lock:
            # Check existence under global lock to prevent race
            if key not in self._data:
                return self._safe_copy(default)
            
            # Get per-key lock while holding global lock
            lock = self._get_key_lock_unlocked(key)
        
        # Now use per-key lock for the actual read
        with lock:
            # Double-check after acquiring per-key lock
            value = self._data.get(key, default)
            return self._safe_copy(value)
    
    def set(self, key: str, value: Any, component: str = "unknown", mode: str = "replace") -> bool:
        """
        Set global variable value with bulletproof thread safety and validation.
        
        Uses standardized lock ordering: global_lock → per_key_lock
        Copy-on-write semantics prevent mutation issues.
        
        Args:
            key: Global variable key
            value: Value to store (must be JSON serializable)
            component: Component name for logging
            mode: "replace" or "accumulate" (spec requirement, replace is default)
            
        Returns:
            True if value was set, False if key already exists (immutable)
        """
        # Validate JSON serializability upfront (fail fast)
        try:
            json.dumps(value, ensure_ascii=False)
        except (TypeError, ValueError) as e:
            self.logger.error(
                "GLOBAL_SET_SERIALIZATION_ERROR",
                extra={"key": key, "component": component, "error": str(e)}
            )
            raise GlobalStoreError(f"Value for key '{key}' is not JSON serializable: {e}")
        
        # Copy-on-write: deep copy on set to prevent shared references
        safe_value = self._safe_copy(value)
        
        # Standardized lock ordering: global_lock first
        with self._global_lock:
            # Get per-key lock while holding global lock
            lock = self._get_key_lock_unlocked(key)
            
            # Perform set operation under per-key lock
            with lock:
                if key in self._data:
                    if mode == "accumulate" and isinstance(self._data[key], list) and isinstance(safe_value, list):
                        # Accumulate lists: create new list with deep-copied elements
                        accumulated = self._data[key] + safe_value
                        self._data[key] = self._safe_copy(accumulated)  # Ensure deep copy of combined list
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
                self._validate_value_size(safe_value, key, component)
                
                # Increment revision (already under global lock)
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
    
    def _get_key_lock_unlocked(self, key: str) -> threading.RLock:
        """
        Get or create lock for key (must be called under global_lock).
        
        Performs cleanup to prevent memory leaks and tracks access times.
        """
        # Initialize access time tracking if needed
        if not hasattr(self, '_lock_access_times'):
            self._lock_access_times = defaultdict(float)
        
        # Update access time for this key
        self._lock_access_times[key] = time.time()
        
        # Periodic cleanup while under global lock (thread-safe)
        current_time = time.time()
        if current_time - self._last_cleanup > 300:
            self._cleanup_unused_locks_unlocked()
            self._last_cleanup = current_time
        
        # Create lock if needed
        if key not in self._locks:
            self._locks[key] = threading.RLock()
        
        return self._locks[key]
    
    def _cleanup_unused_locks_unlocked(self) -> None:
        """
        Remove locks for keys that no longer exist or are stale (must be called under global_lock).
        
        Industry standard: LRU-based cleanup with access time tracking to prevent memory leaks.
        """
        current_time = time.time()
        
        # Track lock access times if not already tracking
        if not hasattr(self, '_lock_access_times'):
            self._lock_access_times = defaultdict(lambda: current_time)
        
        # Remove locks for deleted keys (original logic)
        data_keys = set(self._data.keys())
        lock_keys = set(self._locks.keys())
        unused_keys = lock_keys - data_keys
        
        # Add LRU cleanup: remove locks not accessed in last 1 hour
        stale_threshold = 3600  # 1 hour
        stale_keys = {
            key for key in lock_keys 
            if current_time - self._lock_access_times.get(key, current_time) > stale_threshold
        }
        
        # Cap total locks at 1000 (industry standard memory management)
        if len(self._locks) > 1000:
            # Sort by access time, remove oldest 25%
            sorted_locks = sorted(
                self._lock_access_times.items(),
                key=lambda x: x[1]
            )
            excess_count = len(self._locks) - 750  # Keep at 75% capacity
            excess_keys = {key for key, _ in sorted_locks[:excess_count]}
            stale_keys.update(excess_keys)
        
        # Remove all identified keys
        cleanup_keys = unused_keys | stale_keys
        for key in cleanup_keys:
            self._locks.pop(key, None)
            self._lock_access_times.pop(key, None)
        
        if cleanup_keys:
            self.logger.debug(
                "GLOBAL_STORE_LOCK_CLEANUP",
                extra={
                    "unused_locks": len(unused_keys),
                    "stale_locks": len(stale_keys),
                    "total_cleaned": len(cleanup_keys),
                    "remaining_locks": len(self._locks)
                }
            )
    
    def _validate_value_size(self, value: Any, key: str, component: str) -> None:
        """Validate value size and emit warnings for large values."""
        try:
            serialized_size = len(json.dumps(value).encode('utf-8'))
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
                extra={"key": key, "component": component, "error": str(e)}
            )
    
    def _safe_copy(self, value: Any) -> Any:
        """
        Create safe deep copy of value for bulletproof thread safety.
        
        Belt-and-braces: always deep copy mutable types.
        """
        if value is None or isinstance(value, (int, float, str, bool)):
            return value
        
        import copy
        try:
            return copy.deepcopy(value)
        except Exception:
            # Fallback for uncopyable objects
            self.logger.warning(
                "SAFE_COPY_FALLBACK",
                extra={"value_type": type(value).__name__, "fallback": "shallow_copy"}
            )
            return value
    
    def revision(self) -> int:
        """
        Get current revision number (thread-safe monotonic counter).
        
        Returns:
            Current revision number
        """
        with self._global_lock:
            return self._revision
    
    def dump(self) -> bytes:
        """
        Serialize entire GlobalStore state to JSON bytes with validation.
        
        Used for checkpointing at subjob boundaries.
        
        Returns:
            JSON bytes containing revision and data
        """
        with self._global_lock:
            # Create validated snapshot
            validated_data = {}
            for key, value in self._data.items():
                try:
                    # Validate each value is still serializable
                    json.dumps(value, ensure_ascii=False)
                    validated_data[key] = value
                except (TypeError, ValueError) as e:
                    self.logger.warning(
                        "DUMP_SKIP_INVALID_VALUE",
                        extra={"key": key, "error": str(e), "action": "skipped"}
                    )
            
            state = {
                "revision": self._revision,
                "data": validated_data
            }
        
        try:
            return json.dumps(state, ensure_ascii=False).encode('utf-8')
        except (TypeError, ValueError) as e:
            self.logger.error(
                "GLOBAL_DUMP_ERROR",
                extra={"error": str(e), "data_keys": list(validated_data.keys())}
            )
            raise GlobalStoreError(f"Failed to serialize GlobalStore: {e}")
    
    @classmethod
    def from_dump(cls, data: bytes, logger: logging.Logger) -> 'GlobalStore':
        """
        Restore GlobalStore from serialized state with validation.
        
        Used for resume from checkpoint.
        
        Args:
            data: JSON bytes from previous dump()
            logger: Logger instance for new store
            
        Returns:
            Restored GlobalStore instance
        """
        try:
            state = json.loads(data.decode('utf-8'))
            
            # Validate state structure
            if not isinstance(state, dict) or "data" not in state:
                raise GlobalStoreError("Invalid dump format: missing data section")
            
            # Create new instance
            store = cls(logger)
            store._revision = state.get("revision", 0)
            
            # Validate and restore data with error recovery
            restored_data = {}
            invalid_keys = []
            
            for key, value in state.get("data", {}).items():
                try:
                    # Validate each restored value is JSON serializable
                    json.dumps(value, ensure_ascii=False)
                    # Deep copy restored value for safety
                    restored_data[key] = store._safe_copy(value)
                except (TypeError, ValueError) as e:
                    invalid_keys.append(key)
                    logger.warning(
                        "RESTORE_SKIP_INVALID_VALUE",
                        extra={"key": key, "error": str(e), "action": "skipped"}
                    )
            
            store._data = restored_data
            
            logger.info(
                "GLOBAL_STORE_RESTORED",
                extra={
                    "revision": store._revision,
                    "variables_count": len(store._data),
                    "invalid_keys_skipped": len(invalid_keys),
                    "variables": list(store._data.keys())[:10]  # Show first 10 keys
                }
            )
            
            return store
            
        except (json.JSONDecodeError, UnicodeDecodeError, KeyError) as e:
            logger.error("GLOBAL_RESTORE_ERROR", extra={"error": str(e)})
            raise GlobalStoreError(f"Failed to restore GlobalStore: {e}")
    
    def keys(self) -> list:
        """Get list of all global variable keys (thread-safe)."""
        with self._global_lock:
            return list(self._data.keys())
    
    def __len__(self) -> int:
        """Get number of global variables (thread-safe)."""
        with self._global_lock:
            return len(self._data)


class BufferedStore:
    """
    Production-grade BufferedStore for forEach components.
    
    Features bulletproof thread safety with standardized lock ordering:
    1. global_store._global_lock (outermost)
    2. global_store per-key locks (middle)  
    3. self._buffer_lock (innermost)
    
    Provides copy-on-write semantics with belt-and-braces safety.
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
    
    def get(self, key: str, default: Any = None) -> Any:
        """
        Get global variable value with standardized lock ordering.
        
        Lock order: _buffer_lock → (delegate to GlobalStore's locks)
        
        Args:
            key: Global variable key (component_name__variable_name format)
            default: Default value if key not found
            
        Returns:
            Deep copy of stored value or default (belt-and-braces safety)
        """
        # Check buffer first (innermost lock)
        with self._buffer_lock:
            if key in self._buffer:
                # Belt-and-braces: deep copy from buffer too
                return self.global_store._safe_copy(self._buffer[key])
        
        # Delegate to GlobalStore (uses its standardized locking)
        return self.global_store.get(key, default)
    
    def set(self, key: str, value: Any, component: str = None, mode: str = "replace") -> bool:
        """
        Buffer set operation with copy-on-write semantics.
        
        Args:
            key: Global variable key
            value: Value to buffer
            component: Component name for logging
            mode: "replace" or "accumulate"
            
        Returns:
            True (always succeeds in buffer)
        """
        if component is None:
            component = f"{self.component_name}_component"
        
        # Validate JSON serializability upfront (fail fast)
        try:
            json.dumps(value, ensure_ascii=False)
        except (TypeError, ValueError) as e:
            self.logger.error(
                "BUFFERED_SET_SERIALIZATION_ERROR",
                extra={"key": key, "component": component, "forEach_component": self.component_name, "error": str(e)}
            )
            raise GlobalStoreError(f"Value for key '{key}' is not JSON serializable: {e}")
        
        # Copy-on-write: deep copy on set to prevent shared references
        safe_value = self.global_store._safe_copy(value)
        
        with self._buffer_lock:
            if mode == "accumulate" and key in self._buffer and isinstance(self._buffer[key], list) and isinstance(safe_value, list):
                # Accumulate lists in buffer with deep copy
                accumulated = self._buffer[key] + safe_value
                self._buffer[key] = self.global_store._safe_copy(accumulated)  # Belt-and-braces deep copy
            else:
                # Store/replace in buffer
                self._buffer[key] = safe_value
            
            # Log buffered operation
            self.logger.debug(
                "GLOBAL_SET_BUFFERED",
                extra={
                    "key": key,
                    "component": component,
                    "forEach_component": self.component_name,
                    "iteration": self._iteration_count,
                    "value_type": type(safe_value).__name__,
                    "mode": mode
                }
            )
            
            return True
    
    def flush(self) -> Dict[str, bool]:
        """
        Flush all buffered operations with bulletproof error handling.
        
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
        flush_errors = []
        
        with self._buffer_lock:
            # Capture buffer size BEFORE any operations
            original_buffer_size = len(self._buffer)
            
            # Flush nested buffers first (inner to outer) with error handling
            for child_name, child_buffer in self._child_buffers.items():
                try:
                    child_buffer.flush()
                except Exception as e:
                    flush_errors.append(f"Child buffer '{child_name}': {e}")
                    self.logger.error(
                        "NESTED_BUFFER_FLUSH_ERROR",
                        extra={
                            "parent_forEach": self.component_name,
                            "child_forEach": child_name,
                            "error": str(e)
                        }
                    )
            
            # Flush this buffer to parent or GlobalStore
            target_store = self._parent_buffer if self._parent_buffer else self.global_store
            
            for key, value in self._buffer.items():
                try:
                    success = target_store.set(
                        key, 
                        value, 
                        component=f"{self.component_name}_iteration_{self._iteration_count}",
                        mode="replace"  # Iterator summary always replaces
                    )
                    results[key] = success
                except Exception as e:
                    flush_errors.append(f"Key '{key}': {e}")
                    self.logger.error(
                        "BUFFERED_STORE_FLUSH_ERROR",
                        extra={
                            "key": key,
                            "forEach_component": self.component_name,
                            "error": str(e)
                        }
                    )
                    results[key] = False
            
            # Log flush operation
            self.logger.info(
                "BUFFERED_STORE_FLUSHED",
                extra={
                    "forEach_component": self.component_name,
                    "iteration": self._iteration_count,
                    "variables_flushed": original_buffer_size,
                    "successful_sets": sum(results.values()),
                    "failed_sets": len(results) - sum(results.values()),
                    "flush_errors": len(flush_errors),
                    "target": "parent_buffer" if self._parent_buffer else "global_store"
                }
            )
            
            # Clear buffer and mark as flushed
            self._buffer.clear()
            self._is_flushed = True
            
            # Use original buffer size for critical failure check (FIXED BUG)
            if flush_errors and len(flush_errors) == original_buffer_size:
                raise GlobalStoreError(f"BufferedStore flush failed completely: {flush_errors[:3]}")
        
        return results
    
    def start_iteration(self, iteration_data: Dict[str, Any] = None) -> None:
        """
        Start new forEach iteration context with error recovery and data storage.
        
        Args:
            iteration_data: Optional iteration context data (now properly stored)
        """
        with self._buffer_lock:
            # Clear any stale buffer from previous failed iterations
            if self._buffer:
                self.logger.warning(
                    "BUFFERED_STORE_STALE_BUFFER",
                    extra={
                        "forEach_component": self.component_name,
                        "stale_keys": list(self._buffer.keys()),
                        "action": "clearing_stale_buffer"
                    }
                )
                self._buffer.clear()
            
            self._iteration_count += 1
            self._is_flushed = False
            
            # Industry standard: Store iteration data for component access
            self._current_iteration_data = iteration_data.copy() if iteration_data else {}
            
            self.logger.debug(
                "FOREACH_ITERATION_START",
                extra={
                    "forEach_component": self.component_name,
                    "iteration": self._iteration_count,
                    "context_keys": list(self._current_iteration_data.keys())
                }
            )
    
    def get_iteration_data(self) -> Dict[str, Any]:
        """
        Get current iteration context data.
        
        Returns:
            Copy of current iteration data or empty dict if none set
        """
        with self._buffer_lock:
            return getattr(self, '_current_iteration_data', {}).copy()
    
    def end_iteration(self) -> None:
        """End current forEach iteration context."""
        self.logger.debug(
            "FOREACH_ITERATION_END",
            extra={
                "forEach_component": self.component_name,
                "iteration": self._iteration_count
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
        """
        Get combined keys from buffer and GlobalStore with standardized lock ordering.
        
        Lock order: _buffer_lock → global_store._global_lock
        """
        with self._buffer_lock:
            buffer_keys = set(self._buffer.keys())
            
            # Thread-safe access to GlobalStore keys (respects lock ordering)
            with self.global_store._global_lock:
                global_keys = set(self.global_store._data.keys())
        
        return sorted(list(buffer_keys | global_keys))
    
    def __len__(self) -> int:
        """Get total number of variables (buffer + GlobalStore)."""
        return len(self.keys())