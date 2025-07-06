import json
import re
from pathlib import Path
from typing import Any, Dict, List
import jsonschema
from jsonschema import ValidationError as JsonSchemaValidationError
from pype.core.utils.constants import (
    JOB_SCHEMA_FILE,
    COMPONENT_SCHEMA_FILE,
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
    """Validate job dictionary against enhanced job.schema.json."""
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


def validate_component_schema(component_data: Dict[str, Any]) -> List[str]:
    """Validate component dictionary against component.schema.json."""
    if not isinstance(component_data, dict):
        return ["Component data must be a dictionary"]
    
    try:
        schema = load_schema(COMPONENT_SCHEMA_FILE)
        jsonschema.validate(component_data, schema)
        return []
    except JsonSchemaValidationError as e:
        return [format_validation_error(e)]
    except ValidationError as e:
        return [str(e)]
    except Exception as e:
        return [f"Unexpected validation error: {e}"]


def validate_global_variable_references(job_data: Dict[str, Any]) -> List[str]:
    """Validate global variable references point to existing components."""
    errors = []
    
    # Get component names
    components = job_data.get("components", [])
    component_names = {comp.get("name") for comp in components if comp.get("name")}
    
    # Find global variable references
    job_str = json.dumps(job_data)
    global_vars = re.findall(GLOBAL_VAR_PATTERN, job_str)
    
    for comp_name, var_name in global_vars:
        if comp_name not in component_names:
            errors.append(f"Global variable '{{{{{comp_name}{GLOBAL_VAR_DELIMITER}{var_name}}}}}' references unknown component '{comp_name}'")
    
    return errors


def validate_job_file(file_path: Path) -> List[str]:
    """Validate job YAML file against all rules."""
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
    
    # Business logic validation
    if not errors:  # Only if schema validation passed
        errors.extend(validate_global_variable_references(job_data))
    
    return errors