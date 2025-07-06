import re
from typing import Any, Dict
from pype.core.utils.constants import (
    CONTEXT_VAR_PATTERN, 
    SECRET_VAR_PATTERN, 
    GLOBAL_VAR_PATTERN
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
        
        # Check for unresolved global variables (should be resolved at runtime)
        if re.search(GLOBAL_VAR_PATTERN, resolved):
            # Global variables are resolved at runtime, not compile time
            pass
        
        return resolved
        
    except re.error as e:
        raise TemplateError(f"Invalid template pattern: {e}")

#implemented a recursive search and substitute
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
    """Main function to resolve all context templates in job YAML."""
    if not isinstance(yaml_data, dict):
        raise TemplateError("YAML data must be a dictionary")
    
    if not isinstance(context, dict):
        raise TemplateError("Context must be a dictionary")
    
    try:
        return resolve_template_dict(yaml_data, context)
    except TemplateError:
        raise
    except Exception as e:
        raise TemplateError(f"Unexpected error during template resolution: {e}")