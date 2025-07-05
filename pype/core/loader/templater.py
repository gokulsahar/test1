import re
from typing import Any, Dict, Union


class TemplateError(Exception):
    """Custom exception for template processing errors."""
    pass


def resolve_template_string(template: str, context: Dict[str, Any]) -> str:
    
    if not isinstance(template, str):
        return str(template)
    
    # Pattern matches {{context.VAR_NAME}} with optional whitespace
    pattern = r'\{\{\s*context\.([a-zA-Z_][a-zA-Z0-9_]*)\s*\}\}'
    
    def replace_placeholder(match) -> str:
        var_name = match.group(1)
        if var_name not in context:
            raise TemplateError(f"Context variable '{var_name}' not found in provided context")
        
        # Convert all values to strings for consistent YAML processing
        return str(context[var_name])
    
    try:
        resolved = re.sub(pattern, replace_placeholder, template)
        
        # Check for unresolved secret placeholders (future CyberArk integration)
        secret_pattern = r'\{\{\s*secret\.[^}]+\s*\}\}'
        if re.search(secret_pattern, resolved):
            # TODO: Implement CyberArk secret resolution in future phase
            # Will integrate with pype.core.utils.secrets module
            raise NotImplementedError("Secret resolution via CyberArk not yet implemented")
        
        return resolved
        
    except re.error as e:
        raise TemplateError(f"Invalid template pattern: {e}")

#initial job dict is passed to this method, this method will recurrsivly search dict to find the str and teh sstrs will be passed
def resolve_template_dict(data: Dict[str, Any], context: Dict[str, Any]) -> Dict[str, Any]:
    
    if isinstance(data, dict):
        return {key: resolve_template_dict(value, context) for key, value in data.items()}
    elif isinstance(data, list):
        return [resolve_template_dict(item, context) for item in data]
    elif isinstance(data, str):
        return resolve_template_string(data, context)
    else:
        # Preserve non-string values (int, bool, None, etc.)
        return data


def resolve_template_yaml(yaml_data: Dict[str, Any], context: Dict[str, Any]) -> Dict[str, Any]:
    
    if not isinstance(yaml_data, dict):
        raise TemplateError("YAML data must be a dictionary structure")
    
    if not isinstance(context, dict):
        raise TemplateError("Context must be a dictionary")
    
    # Validate yaml_data has expected job structure
    if "job" not in yaml_data:
        raise TemplateError("YAML data missing required 'job' section")
    
    try:
        return resolve_template_dict(yaml_data, context)
    except TemplateError:
        # Re-raise template errors as-is
        raise
    except Exception as e:
        raise TemplateError(f"Unexpected error during template resolution: {e}")



#validation to check if there are no typos and braces are closed etc.
def validate_template_syntax(template: str) -> bool:
    
    if not isinstance(template, str):
        return True  # Non-strings don't need template validation
    
    # Find all placeholder patterns
    all_placeholders = re.findall(r'\{\{[^}]+\}\}', template)
    
    # Valid patterns: context.VAR_NAME or secret.VAR_NAME
    valid_context_pattern = r'^\s*context\.[a-zA-Z_][a-zA-Z0-9_]*\s*$'
    valid_secret_pattern = r'^\s*secret\.[a-zA-Z_][a-zA-Z0-9_]*\s*$'
    
    for placeholder in all_placeholders:
        # Remove {{ and }} brackets
        inner_content = placeholder[2:-2]
        
        if not (re.match(valid_context_pattern, inner_content) or 
                re.match(valid_secret_pattern, inner_content)):
            return False
    
    return True