import json
import re
from pathlib import Path
from typing import Any, Dict, List
import jsonschema
from jsonschema import ValidationError as JsonSchemaValidationError
from pype.core.utils.constants import (
    JOB_SCHEMA_FILE,
    COMPONENT_SCHEMA_FILE,
    COMPONENT_NAME_PATTERN,
    PORT_NAME_PATTERN,
    VALID_CONTROL_TYPES,
    IF_CONTROL_PATTERN,
    CONTROL_TYPE_PATTERN,
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
    else:
        return f"Validation error in '{field_path}': {error.message}"


def validate_job_schema(job_data: Dict[str, Any]) -> List[str]:
    """Validate job dictionary against job.schema.json."""
    errors = []
    
    if not isinstance(job_data, dict):
        return ["Job data must be a dictionary"]
    
    try:
        schema = load_schema(JOB_SCHEMA_FILE)
        jsonschema.validate(job_data, schema)
    except JsonSchemaValidationError as e:
        errors.append(format_validation_error(e))
    except ValidationError as e:
        errors.append(str(e))
    except Exception as e:
        errors.append(f"Unexpected validation error: {e}")
    
    return errors


def validate_component_schema(component_data: Dict[str, Any]) -> List[str]:
    """Validate component dictionary against component.schema.json."""
    errors = []
    
    if not isinstance(component_data, dict):
        return ["Component data must be a dictionary"]
    
    try:
        schema = load_schema(COMPONENT_SCHEMA_FILE)
        jsonschema.validate(component_data, schema)
    except JsonSchemaValidationError as e:
        errors.append(format_validation_error(e))
    except ValidationError as e:
        errors.append(str(e))
    except Exception as e:
        errors.append(f"Unexpected validation error: {e}")
    
    # Additional component name validation
    comp_name = component_data.get("name")
    if comp_name and not re.match(COMPONENT_NAME_PATTERN, comp_name):
        errors.append(f"Component name '{comp_name}' must be alphanumeric only")
    
    return errors


def validate_data_connection_syntax(connection: str) -> List[str]:
    """Validate data connection syntax: 'comp1.port -> comp2.port'."""
    errors = []
    
    if not isinstance(connection, str):
        return [f"Data connection must be string, got: {type(connection)}"]
    
    if "->" not in connection:
        return [f"Invalid data connection syntax: '{connection}' (missing '->')"]
    
    try:
        source, target = connection.split("->", 1)
        source = source.strip()
        target = target.strip()
        
        # Check source format
        if "." not in source:
            errors.append(f"Invalid source format in '{connection}': missing port (should be 'component.port')")
        else:
            source_comp, source_port = source.rsplit(".", 1)
            if not source_comp or not source_port:
                errors.append(f"Empty component or port name in source '{source}'")
            else:
                if not re.match(COMPONENT_NAME_PATTERN, source_comp):
                    errors.append(f"Invalid source component name '{source_comp}' (must be alphanumeric only)")
                if not re.match(PORT_NAME_PATTERN, source_port):
                    errors.append(f"Invalid source port name '{source_port}' (must be alphanumeric only)")
        
        # Check target format
        if "." not in target:
            errors.append(f"Invalid target format in '{connection}': missing port (should be 'component.port')")
        else:
            target_comp, target_port = target.rsplit(".", 1)
            if not target_comp or not target_port:
                errors.append(f"Empty component or port name in target '{target}'")
            else:
                if not re.match(COMPONENT_NAME_PATTERN, target_comp):
                    errors.append(f"Invalid target component name '{target_comp}' (must be alphanumeric only)")
                if not re.match(PORT_NAME_PATTERN, target_port):
                    errors.append(f"Invalid target port name '{target_port}' (must be alphanumeric only)")
                
    except ValueError:
        errors.append(f"Invalid data connection format: '{connection}'")
    
    return errors


def validate_control_connection_syntax(connection: str) -> List[str]:
    """Validate control connection syntax."""
    errors = []
    
    if not isinstance(connection, str):
        return [f"Control connection must be string, got: {type(connection)}"]
    
    # Check for basic control syntax with parentheses
    if "(" not in connection or ")" not in connection:
        return [f"Invalid control connection syntax: '{connection}' (missing control type in parentheses)"]
    
    # Extract control type
    paren_match = re.search(CONTROL_TYPE_PATTERN, connection)
    if not paren_match:
        return [f"Invalid control connection syntax: '{connection}' (malformed parentheses)"]
    
    control_type = paren_match.group(1).strip()
    
    # Validate control keywords
    if control_type not in VALID_CONTROL_TYPES and not re.match(IF_CONTROL_PATTERN, control_type):
        errors.append(f"Invalid control type '{control_type}' in '{connection}'")
    
    # Special validation for if conditions
    if re.match(IF_CONTROL_PATTERN, control_type):
        if ":" not in connection:
            errors.append(f"If condition missing ':' in '{connection}'")
        else:
            # Check for quoted condition after ':'
            colon_part = connection.split(":", 1)[1] if ":" in connection else ""
            if not ('"' in colon_part or "'" in colon_part):
                errors.append(f"If condition should be quoted in '{connection}'")
    
    # Validate component names in control connection
    comp_pattern = r'\b([a-zA-Z][a-zA-Z0-9]*)\b'
    potential_components = re.findall(comp_pattern, connection)
    referenced_components = [comp for comp in potential_components if comp not in VALID_CONTROL_TYPES]
    
    for comp_name in referenced_components:
        if not re.match(COMPONENT_NAME_PATTERN, comp_name):
            errors.append(f"Invalid component name '{comp_name}' in control connection (must be alphanumeric only)")
    
    return errors


def validate_component_references(connections: Dict[str, Any], components: List[Dict[str, Any]]) -> List[str]:
    """Validate that all connection references point to components in YAML."""
    errors = []
    
    # Get component names
    component_names = set()
    for comp in components:
        if not isinstance(comp, dict):
            errors.append("Each component must be a dictionary")
            continue
        
        comp_name = comp.get("name")
        if not comp_name:
            errors.append("Component missing 'name' field")
            continue
        
        if not re.match(COMPONENT_NAME_PATTERN, comp_name):
            errors.append(f"Component name '{comp_name}' must be alphanumeric only")
            continue
        
        component_names.add(comp_name)
    
    # Check data connections
    data_connections = connections.get("data", {})
    if isinstance(data_connections, (dict, list)):
        data_list = data_connections if isinstance(data_connections, list) else list(data_connections.keys())
        
        for connection in data_list:
            if "->" in str(connection):
                try:
                    source, target = str(connection).split("->", 1)
                    source_comp = source.strip().split(".")[0]
                    target_comp = target.strip().split(".")[0]
                    
                    if source_comp not in component_names:
                        errors.append(f"Unknown source component '{source_comp}' in connection: {connection}")
                    if target_comp not in component_names:
                        errors.append(f"Unknown target component '{target_comp}' in connection: {connection}")
                except:
                    pass  # Syntax errors handled elsewhere
    
    # Check control connections
    control_connections = connections.get("control", [])
    if isinstance(control_connections, list):
        for connection in control_connections:
            # Extract component names from control expression
            comp_pattern = r'\b([a-zA-Z][a-zA-Z0-9]*)\b'
            potential_components = re.findall(comp_pattern, str(connection))
            
            # Filter out control keywords
            referenced_components = [comp for comp in potential_components if comp not in VALID_CONTROL_TYPES]
            
            for comp_name in referenced_components:
                if comp_name not in component_names:
                    errors.append(f"Unknown component '{comp_name}' in control connection: {connection}")
    
    return errors


def validate_connections(connections: Dict[str, Any], components: List[Dict[str, Any]]) -> List[str]:
    """Validate all connection syntax and references."""
    errors = []
    
    if not isinstance(connections, dict):
        return ["Connections must be a dictionary"]
    
    if not isinstance(components, list):
        return ["Components must be a list"]
    
    # Validate data connection syntax
    data_connections = connections.get("data", {})
    if isinstance(data_connections, (dict, list)):
        data_list = data_connections if isinstance(data_connections, list) else list(data_connections.keys())
        
        for connection in data_list:
            errors.extend(validate_data_connection_syntax(str(connection)))
    
    # Validate control connection syntax
    control_connections = connections.get("control", [])
    if isinstance(control_connections, list):
        for connection in control_connections:
            errors.extend(validate_control_connection_syntax(str(connection)))
    
    # Validate component references
    errors.extend(validate_component_references(connections, components))
    
    return errors


def validate_job_file(file_path: Path) -> List[str]:
    """Validate job YAML file against all rules."""
    errors = []
    
    try:
        import ruamel.yaml
        yaml = ruamel.yaml.YAML(typ='safe')
        
        with open(file_path, 'r', encoding=DEFAULT_ENCODING) as f:
            job_data = yaml.load(f)
            
    except FileNotFoundError:
        return [f"Job file not found: {file_path}"]
    except Exception as e:
        return [f"Error reading job file '{file_path}': {e}"]
    
    # Run all validations
    errors.extend(validate_job_schema(job_data))
    
    components = job_data.get("components", [])
    for i, comp in enumerate(components):
        comp_errors = validate_component_schema(comp)
        errors.extend([f"Component {i+1}: {error}" for error in comp_errors])
    
    connections = job_data.get("connections", {})
    errors.extend(validate_connections(connections, components))
    
    return errors