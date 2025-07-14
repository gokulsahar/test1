"""
Utility methods for DataPY engine operations.

Provides common utility functions for memory parsing, validation,
and other shared operations across engine components.
"""

import re
import uuid
from datetime import datetime
from typing import Union, Dict, Any


class UtilityError(Exception):
    """Base exception for utility operations."""
    pass


def parse_memory_size(memory_str: str) -> int:
    """
    Parse memory size string to bytes.
    
    Supports formats like '4GB', '200MB', '1024KB', '512B'.
    
    Args:
        memory_str: Memory size string (e.g., '4GB', '200MB')
        
    Returns:
        Memory size in bytes
        
    Raises:
        UtilityError: If memory string format is invalid
        
    Examples:
        >>> parse_memory_size('4GB')
        4294967296
        >>> parse_memory_size('200MB')
        209715200
        >>> parse_memory_size('1024KB')
        1048576
    """
    if not isinstance(memory_str, str):
        raise UtilityError(f"Memory size must be string, got {type(memory_str)}")
    
    # Pattern: number + optional decimal + optional unit + B
    pattern = r'^(\d+(?:\.\d+)?)\s*([KMGT]?)B$'
    match = re.match(pattern, memory_str.strip().upper())
    
    if not match:
        raise UtilityError(
            f"Invalid memory size format: '{memory_str}'. "
            f"Use format like '4GB', '200MB', '1024KB', '512B'"
        )
    
    size_value = float(match.group(1))
    unit = match.group(2)
    
    # Convert to bytes
    multipliers = {
        '': 1,           # Bytes
        'K': 1024,       # Kilobytes
        'M': 1024**2,    # Megabytes
        'G': 1024**3,    # Gigabytes
        'T': 1024**4     # Terabytes
    }
    
    multiplier = multipliers.get(unit, 1)
    total_bytes = int(size_value * multiplier)
    
    return total_bytes


def generate_run_id(job_name: str) -> str:
    """
    Generate unique run ID with job name and timestamp.
    
    Format: {job_name}_{timestamp}_{uuid_short}
    
    Args:
        job_name: Name of the job from job metadata
        
    Returns:
        Unique run ID string
        
    Examples:
        >>> generate_run_id('data_pipeline')
        'data_pipeline_20250115_143052_a1b2c3d4'
    """
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    uuid_short = str(uuid.uuid4()).replace('-', '')[:8]
    
    return f"{job_name}_{timestamp}_{uuid_short}"


def validate_positive_integer(value: Any, field_name: str, min_val: int = 1, max_val: int = None) -> int:
    """
    Validate that value is a positive integer within bounds.
    
    Args:
        value: Value to validate
        field_name: Field name for error messages
        min_val: Minimum allowed value (default: 1)
        max_val: Maximum allowed value (optional)
        
    Returns:
        Validated integer value
        
    Raises:
        UtilityError: If validation fails
    """
    if not isinstance(value, int):
        raise UtilityError(f"{field_name} must be integer, got {type(value)}")
    
    if value < min_val:
        raise UtilityError(f"{field_name} must be >= {min_val}, got {value}")
    
    if max_val is not None and value > max_val:
        raise UtilityError(f"{field_name} must be <= {max_val}, got {value}")
    
    return value


def validate_string_enum(value: Any, field_name: str, allowed_values: list) -> str:
    """
    Validate that value is a string in allowed enum values.
    
    Args:
        value: Value to validate
        field_name: Field name for error messages
        allowed_values: List of allowed string values
        
    Returns:
        Validated string value
        
    Raises:
        UtilityError: If validation fails
    """
    if not isinstance(value, str):
        raise UtilityError(f"{field_name} must be string, got {type(value)}")
    
    if value not in allowed_values:
        raise UtilityError(
            f"{field_name} must be one of {allowed_values}, got '{value}'"
        )
    
    return value


def sanitize_component_name(name: str) -> str:
    """
    Sanitize component name for safe usage in identifiers.
    
    Args:
        name: Component name to sanitize
        
    Returns:
        Sanitized component name
    """
    if not isinstance(name, str):
        return str(name)
    
    # Replace invalid characters with underscores
    sanitized = re.sub(r'[^a-zA-Z0-9_]', '_', name)
    
    # Ensure it starts with letter or underscore
    if sanitized and not sanitized[0].isalpha() and sanitized[0] != '_':
        sanitized = f"component_{sanitized}"
    
    return sanitized or "unnamed_component"


def deep_merge_dicts(base_dict: Dict[str, Any], update_dict: Dict[str, Any]) -> Dict[str, Any]:
    """
    Deep merge two dictionaries, with update_dict values taking precedence.
    
    Args:
        base_dict: Base dictionary
        update_dict: Dictionary with updates
        
    Returns:
        Merged dictionary
    """
    result = base_dict.copy()
    
    for key, value in update_dict.items():
        if (key in result and 
            isinstance(result[key], dict) and 
            isinstance(value, dict)):
            result[key] = deep_merge_dicts(result[key], value)
        else:
            result[key] = value
    
    return result


def format_duration(seconds: float) -> str:
    """
    Format duration in seconds to human-readable string.
    
    Args:
        seconds: Duration in seconds
        
    Returns:
        Formatted duration string
        
    Examples:
        >>> format_duration(65.5)
        '1m 5.5s'
        >>> format_duration(3661)
        '1h 1m 1s'
    """
    if seconds < 60:
        return f"{seconds:.1f}s"
    
    minutes = int(seconds // 60)
    remaining_seconds = seconds % 60
    
    if minutes < 60:
        if remaining_seconds > 0:
            return f"{minutes}m {remaining_seconds:.1f}s"
        return f"{minutes}m"
    
    hours = minutes // 60
    remaining_minutes = minutes % 60
    
    parts = [f"{hours}h"]
    if remaining_minutes > 0:
        parts.append(f"{remaining_minutes}m")
    if remaining_seconds > 0:
        parts.append(f"{remaining_seconds:.1f}s")
    
    return " ".join(parts)


def get_system_info() -> Dict[str, Any]:
    """
    Get basic system information for logging and validation.
    
    Returns:
        Dictionary with system information
    """
    import os
    import platform
    
    return {
        "cpu_count": os.cpu_count(),
        "platform": platform.system(),
        "python_version": platform.python_version(),
        "machine": platform.machine(),
        "processor": platform.processor() or "unknown"
    }