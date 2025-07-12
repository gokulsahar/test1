import json
import re
from pathlib import Path
from typing import Any, Dict, List
import jsonschema
from jsonschema import ValidationError as JsonSchemaValidationError
from pype.core.utils.constants import (
    JOB_SCHEMA_FILE,
    GLOBAL_VAR_PATTERN,
    GLOBAL_VAR_DELIMITER,
    DEFAULT_ENCODING
)


class ValidationError(Exception):
    """Custom validation error for job YAML validation."""
    
    def __init__(self, message: str, field_path: str = None):
        self.field_path = field_path
        super().__init__(message)


def load_schema(schema_name: str) -> Dict[str, Any]:
    """Load JSON schema file."""
    schema_path = Path(__file__).parent.parent.parent / "schemas" / schema_name
    try:
        with open(schema_path, 'r', encoding=DEFAULT_ENCODING) as f:
            return json.load(f)
    except FileNotFoundError:
        raise ValidationError(f"Schema file not found: {schema_path}")
    except json.JSONDecodeError as e:
        raise ValidationError(f"Invalid JSON in schema file: {e}")

#user friendly json schema errors
def format_validation_error(error: JsonSchemaValidationError) -> str:
    """Format jsonschema validation error into readable message."""
    field_path = ".".join(str(p) for p in error.absolute_path) if error.absolute_path else "root"
    
    if error.validator == 'required':
        missing_field = error.message.split("'")[1] if "'" in error.message else "unknown"
        return f"Missing required field '{missing_field}' in section '{field_path}'"
    elif error.validator == 'type':
        return f"Invalid type for field '{field_path}': {error.message}"
    elif error.validator == 'enum':
        return f"Invalid value for field '{field_path}': {error.message}"
    elif error.validator == 'pattern':
        return f"Invalid format for field '{field_path}': {error.message}"
    elif error.validator == 'patternProperties':
        return f"Invalid connection syntax in '{field_path}': {error.message}"
    else:
        return f"Validation error in '{field_path}': {error.message}"


def validate_job_schema(job_data: Dict[str, Any]) -> List[str]:
    """Validate job dictionary against job.schema.json."""
    if not isinstance(job_data, dict):
        return ["Job data must be a dictionary"]
    
    try:
        schema = load_schema(JOB_SCHEMA_FILE)
        jsonschema.validate(job_data, schema)
        return []
    except JsonSchemaValidationError as e:
        return [format_validation_error(e)]
    except ValidationError as e:
        return [str(e)]
    except Exception as e:
        return [f"Unexpected validation error: {e}"]


def validate_global_variable_references(job_data: Dict[str, Any]) -> List[str]:
    """Validate global variable references point to components in the current job."""
    errors = []
    
    # Get component names
    components = job_data.get("components", [])
    component_names = {comp.get("name") for comp in components if comp.get("name")}
    
    # Find global variable references
    job_str = json.dumps(job_data)
    global_vars = re.findall(GLOBAL_VAR_PATTERN, job_str)
    
    for comp_name, var_name in global_vars:
        if comp_name not in component_names:
            errors.append(f"Global variable '{{{{{comp_name}{GLOBAL_VAR_DELIMITER}{var_name}}}}}' references unused component '{comp_name}'")
    
    return errors


def validate_executor_configuration(job_data: Dict[str, Any]) -> List[str]:
    """Validate executor configuration consistency and constraints."""
    errors = []
    
    # Extract job-level execution config
    job_config = job_data.get("job_config", {})
    execution_config = job_config.get("execution", {})
    
    # Extract components
    components = job_data.get("components", [])
    
    # Validate dask configuration consistency
    errors.extend(_validate_dask_consistency(execution_config, components))
    
    # Validate disk_based configuration consistency  
    errors.extend(_validate_disk_consistency(execution_config, components))
    
    # Validate component executor fields
    errors.extend(_validate_component_executor_fields(components))
    
    # Validate resource allocation
    errors.extend(_validate_resource_allocation(execution_config, components))
    
    return errors


def _validate_dask_consistency(execution_config: Dict[str, Any], components: List[Dict[str, Any]]) -> List[str]:
    """Validate dask executor consistency between job-level and component-level config."""
    errors = []
    
    # Find components using dask executor
    dask_components = [comp for comp in components if comp.get("executor") == "dask"]
    
    if dask_components:
        dask_config = execution_config.get("dask", {})
        
        # Check if dask is enabled at job level
        if not dask_config.get("enabled", False):
            comp_names = [comp["name"] for comp in dask_components]
            errors.append(
                f"Components {comp_names} use dask executor but job_config.execution.dask.enabled is not true"
            )
        
        # Validate dask_config fields for each component
        for comp in dask_components:
            comp_dask_config = comp.get("dask_config", {})
            if comp_dask_config:
                comp_errors = _validate_dask_config_fields(comp["name"], comp_dask_config)
                errors.extend(comp_errors)
    
    return errors


def _validate_disk_consistency(execution_config: Dict[str, Any], components: List[Dict[str, Any]]) -> List[str]:
    """Validate disk_based executor consistency between job-level and component-level config."""
    errors = []
    
    # Find components using disk_based executor
    disk_components = [comp for comp in components if comp.get("executor") == "disk_based"]
    
    if disk_components:
        disk_config = execution_config.get("disk_based", {})
        
        # Check if disk_based is enabled at job level
        if not disk_config.get("enabled", False):
            comp_names = [comp["name"] for comp in disk_components]
            errors.append(
                f"Components {comp_names} use disk_based executor but job_config.execution.disk_based.enabled is not true"
            )
        
        # Validate disk_config fields for each component
        for comp in disk_components:
            comp_disk_config = comp.get("disk_config", {})
            if comp_disk_config:
                comp_errors = _validate_disk_config_fields(comp["name"], comp_disk_config)
                errors.extend(comp_errors)
    
    return errors


def _validate_component_executor_fields(components: List[Dict[str, Any]]) -> List[str]:
    """Validate component executor field consistency."""
    errors = []
    
    for comp in components:
        comp_name = comp.get("name", "unknown")
        executor = comp.get("executor")
        
        # Validate executor type
        if executor and executor not in ["threadpool", "dask", "disk_based"]:
            errors.append(
                f"Component '{comp_name}' has invalid executor '{executor}'. "
                f"Must be one of: threadpool, dask, disk_based"
            )
        
        # Validate dask_config only exists when executor is dask
        if comp.get("dask_config") and executor != "dask":
            errors.append(
                f"Component '{comp_name}' has dask_config but executor is '{executor}'. "
                f"dask_config can only be used with executor: dask"
            )
        
        # Validate disk_config only exists when executor is disk_based
        if comp.get("disk_config") and executor != "disk_based":
            errors.append(
                f"Component '{comp_name}' has disk_config but executor is '{executor}'. "
                f"disk_config can only be used with executor: disk_based"
            )
    
    return errors


def _validate_dask_config_fields(comp_name: str, dask_config: Dict[str, Any]) -> List[str]:
    """Validate dask_config field values."""
    errors = []
    
    # Validate workers field
    workers = dask_config.get("workers")
    if workers is not None:
        if not isinstance(workers, int) or workers < 1:
            errors.append(
                f"Component '{comp_name}' dask_config.workers must be a positive integer, got: {workers}"
            )
    
    # Validate memory_per_worker field
    memory_per_worker = dask_config.get("memory_per_worker")
    if memory_per_worker is not None:
        if not _is_valid_memory_size(memory_per_worker):
            errors.append(
                f"Component '{comp_name}' dask_config.memory_per_worker must match pattern like '4GB', '2048MB', got: {memory_per_worker}"
            )
    
    # Check for unknown fields
    valid_fields = {"workers", "memory_per_worker", "threads_per_worker"}
    unknown_fields = set(dask_config.keys()) - valid_fields
    if unknown_fields:
        errors.append(
            f"Component '{comp_name}' dask_config has unknown fields: {unknown_fields}. "
            f"Valid fields: {valid_fields}"
        )
    
    return errors


def _validate_disk_config_fields(comp_name: str, disk_config: Dict[str, Any]) -> List[str]:
    """Validate disk_config field values."""
    errors = []
    
    # Validate cache_size field
    cache_size = disk_config.get("cache_size")
    if cache_size is not None:
        if not _is_valid_memory_size(cache_size):
            errors.append(
                f"Component '{comp_name}' disk_config.cache_size must match pattern like '2GB', '500MB', got: {cache_size}"
            )
    
    # Validate table_file field
    table_file = disk_config.get("table_file")
    if table_file is not None:
        if not isinstance(table_file, str) or not table_file.strip():
            errors.append(
                f"Component '{comp_name}' disk_config.table_file must be a non-empty string"
            )
    
    # Validate lookup_column field
    lookup_column = disk_config.get("lookup_column")
    if lookup_column is not None:
        if not isinstance(lookup_column, str) or not lookup_column.strip():
            errors.append(
                f"Component '{comp_name}' disk_config.lookup_column must be a non-empty string"
            )
    
    # Validate chunk_size field
    chunk_size = disk_config.get("chunk_size")
    if chunk_size is not None:
        if not isinstance(chunk_size, int) or chunk_size < 1000:
            errors.append(
                f"Component '{comp_name}' disk_config.chunk_size must be an integer >= 1000, got: {chunk_size}"
            )
    
    # Check for unknown fields
    valid_fields = {"cache_size", "table_file", "lookup_column", "chunk_size"}
    unknown_fields = set(disk_config.keys()) - valid_fields
    if unknown_fields:
        errors.append(
            f"Component '{comp_name}' disk_config has unknown fields: {unknown_fields}. "
            f"Valid fields: {valid_fields}"
        )
    
    return errors


def _validate_resource_allocation(execution_config: Dict[str, Any], components: List[Dict[str, Any]]) -> List[str]:
    """Validate resource allocation doesn't exceed job-level pools."""
    errors = []
    
    # Validate Dask worker allocation
    dask_config = execution_config.get("dask", {})
    if dask_config.get("enabled"):
        cluster_workers = dask_config.get("cluster_workers", 0)
        
        if cluster_workers > 0:
            total_requested_workers = 0
            for comp in components:
                if comp.get("executor") == "dask":
                    comp_dask_config = comp.get("dask_config", {})
                    workers = comp_dask_config.get("workers", 1)  # Default to 1 if not specified
                    total_requested_workers += workers
            
            if total_requested_workers > cluster_workers:
                errors.append(
                    f"Dask resource over-allocation: {total_requested_workers} workers requested "
                    f"but only {cluster_workers} available in cluster. "
                    f"Either increase job_config.execution.dask.cluster_workers or reduce component worker allocations."
                )
    
    # Validate reasonable limits
    disk_component_count = sum(1 for comp in components if comp.get("executor") == "disk_based")
    if disk_component_count > 10:
        errors.append(
            f"Too many disk_based components ({disk_component_count}). "
            f"Consider consolidating lookups or using different executors for better performance."
        )
    
    return errors


def _is_valid_memory_size(memory_str: str) -> bool:
    """Validate memory size string format like '2GB', '500MB'."""
    if not isinstance(memory_str, str):
        return False
    
    # Pattern: number + optional decimal + optional unit + B
    pattern = r'^\d+(\.\d+)?[KMGT]?B$'
    return bool(re.match(pattern, memory_str))


def validate_job_file(file_path: Path) -> List[str]:
    """Validate job YAML file against all rules including executor configuration."""
    try:
        import ruamel.yaml
        yaml = ruamel.yaml.YAML(typ='safe')
        
        with open(file_path, 'r', encoding=DEFAULT_ENCODING) as f:
            job_data = yaml.load(f)
            
    except FileNotFoundError:
        return [f"Job file not found: {file_path}"]
    except Exception as e:
        return [f"Error reading job file '{file_path}': {e}"]
    
    # Schema validation (includes structure, syntax, patterns)
    errors = validate_job_schema(job_data)
    
    # Business logic validation (only if schema validation passed)
    if not errors:
        errors.extend(validate_global_variable_references(job_data))
        errors.extend(validate_executor_configuration(job_data))
    
    return errors