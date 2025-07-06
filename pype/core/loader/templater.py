import re
from typing import Any, Dict, List
from pype.core.utils.constants import (
    GLOBAL_VAR_DELIMITER, 
    CONTEXT_VAR_PATTERN, 
    SECRET_VAR_PATTERN, 
    GLOBAL_VAR_PATTERN,
    COMPONENT_NAME_PATTERN,
    DEFAULT_ENCODING
)


class TemplateError(Exception):
    """Custom exception for template processing errors."""
    pass


def resolve_template_string(template: str, context: Dict[str, Any]) -> str:
    """Resolve context variables in a template string."""
    if not isinstance(template, str):
        return str(template)
    
    def replace_context_var(match) -> str:
        var_name = match.group(1)
        if var_name not in context:
            raise TemplateError(f"Context variable '{var_name}' not found")
        return str(context[var_name])
    
    try:
        resolved = re.sub(CONTEXT_VAR_PATTERN, replace_context_var, template)
        
        # Check for unresolved secret placeholders
        if re.search(SECRET_VAR_PATTERN, resolved):
            raise NotImplementedError("Secret resolution via CyberArk not yet implemented")
        
        return resolved
        
    except re.error as e:
        raise TemplateError(f"Invalid template pattern: {e}")


def resolve_template_dict(data: Any, context: Dict[str, Any]) -> Any:
    """Recursively resolve templates in nested structures."""
    if isinstance(data, dict):
        return {key: resolve_template_dict(value, context) for key, value in data.items()}
    elif isinstance(data, list):
        return [resolve_template_dict(item, context) for item in data]
    elif isinstance(data, str):
        return resolve_template_string(data, context)
    else:
        return data


def resolve_template_yaml(yaml_data: Dict[str, Any], context: Dict[str, Any]) -> Dict[str, Any]:
    """Main function to resolve all templates in job YAML."""
    if not isinstance(yaml_data, dict):
        raise TemplateError("YAML data must be a dictionary")
    
    if not isinstance(context, dict):
        raise TemplateError("Context must be a dictionary")
    
    if "job" not in yaml_data:
        raise TemplateError("YAML data missing required 'job' section")
    
    try:
        return resolve_template_dict(yaml_data, context)
    except TemplateError:
        raise
    except Exception as e:
        raise TemplateError(f"Unexpected error during template resolution: {e}")


def validate_template_syntax(template: str) -> List[str]:
    """Validate template syntax and return list of errors."""
    errors = []
    
    if not isinstance(template, str):
        return errors
    
    # Find all placeholder patterns
    all_placeholders = re.findall(r'\{\{[^}]+\}\}', template)
    
    for placeholder in all_placeholders:
        # Check if placeholder matches any valid pattern from constants
        if not (re.match(CONTEXT_VAR_PATTERN, placeholder) or 
                re.match(SECRET_VAR_PATTERN, placeholder) or
                re.match(GLOBAL_VAR_PATTERN, placeholder)):
            errors.append(f"Invalid template syntax: {placeholder}")
    
    return errors


def validate_global_variables(yaml_data: Dict[str, Any]) -> List[str]:
    """Validate global variable usage in templates."""
    errors = []
    
    # Get component names from YAML
    components = yaml_data.get("components", [])
    component_names = set()
    for comp in components:
        comp_name = comp.get("name")
        if comp_name:
            # Validate component name follows naming pattern
            if not re.match(COMPONENT_NAME_PATTERN, comp_name):
                errors.append(f"Component name '{comp_name}' does not follow naming pattern (alphanumeric only)")
            else:
                component_names.add(comp_name)
    
    # Find all global variable references using constants
    yaml_str = str(yaml_data)
    global_vars = re.findall(GLOBAL_VAR_PATTERN, yaml_str)
    
    for comp_name, var_name in global_vars:
        if comp_name not in component_names:
            errors.append(f"Global variable references unknown component '{comp_name}' in '{{{{{comp_name}{GLOBAL_VAR_DELIMITER}{var_name}}}}}'")
    
    return errors


def validate_all_templates(yaml_data: Dict[str, Any]) -> List[str]:
    """Validate all template syntax in YAML data."""
    errors = []
    
    # Validate template syntax
    yaml_str = str(yaml_data)
    errors.extend(validate_template_syntax(yaml_str))
    
    # Validate global variables
    errors.extend(validate_global_variables(yaml_data))
    
    return errors