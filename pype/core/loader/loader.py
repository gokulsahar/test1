import json
from pathlib import Path
from typing import Any, Dict, List, Optional
from pydantic import BaseModel, Field, validator
import ruamel.yaml

from .templater import resolve_template_yaml, validate_all_templates
from .validator import validate_job_schema, validate_component_schema, validate_connections
from pype.core.utils.constants import (
    COMPONENT_NAME_PATTERN,
    DEFAULT_RETRIES,
    DEFAULT_TIMEOUT,
    DEFAULT_FAIL_STRATEGY,
    DEFAULT_DASK_MEMORY_LIMIT,
    DEFAULT_DASK_PARALLELISM,
    DEFAULT_DASK_USE_CLUSTER,
    DEFAULT_ENCODING
)


class LoaderError(Exception):
    """Main loader error with collected validation errors."""
    
    def __init__(self, errors: List[str]):
        self.errors = errors
        super().__init__(f"Validation failed with {len(errors)} errors:\n" + "\n".join(f"  - {error}" for error in errors))


class JobMetadata(BaseModel):
    """Job metadata section model."""
    name: str = Field(..., regex=COMPONENT_NAME_PATTERN)
    desc: Optional[str] = None
    version: str = Field(..., min_length=1)
    team: str = Field(..., min_length=1)
    owner: str = Field(..., regex=r'^[^@]+@[^@]+\.[^@]+$')
    created: str = Field(..., regex=r'^\d{4}-\d{2}-\d{2}$')


class DaskConfig(BaseModel):
    """Dask configuration model."""
    memory_limit: Optional[str] = DEFAULT_DASK_MEMORY_LIMIT
    parallelism: Optional[int] = Field(DEFAULT_DASK_PARALLELISM, ge=1)
    use_cluster: Optional[bool] = DEFAULT_DASK_USE_CLUSTER


class JobConfig(BaseModel):
    """Job configuration section model."""
    retries: int = Field(DEFAULT_RETRIES, ge=0)
    timeout: int = Field(DEFAULT_TIMEOUT, gt=0)
    fail_strategy: str = Field(DEFAULT_FAIL_STRATEGY, regex=r'^(halt|continue)$')
    dask_config: Optional[DaskConfig] = None


class ComponentModel(BaseModel):
    """Single component model."""
    name: str = Field(..., regex=COMPONENT_NAME_PATTERN)
    type: str = Field(..., min_length=1)
    params: Dict[str, Any] = Field(default_factory=dict)


class ConnectionsModel(BaseModel):
    """Connections section model."""
    data: Optional[Dict[str, Any]] = Field(default_factory=dict)
    control: Optional[List[str]] = Field(default_factory=list)


class JobModel(BaseModel):
    """Complete job model."""
    job: JobMetadata
    job_config: JobConfig
    components: List[ComponentModel]
    connections: ConnectionsModel
    
    @validator('components')
    def validate_unique_component_names(cls, components):
        """Ensure component names are unique."""
        names = [comp.name for comp in components]
        duplicates = [name for name in names if names.count(name) > 1]
        if duplicates:
            raise ValueError(f"Duplicate component names: {set(duplicates)}")
        return components


def load_context_from_file(context_path: Path) -> Dict[str, Any]:
    """Load context variables from JSON file."""
    if not context_path.exists():
        raise LoaderError([f"Context file not found: {context_path}"])
    
    try:
        with open(context_path, 'r', encoding=DEFAULT_ENCODING) as f:
            context = json.load(f)
        
        if not isinstance(context, dict):
            raise LoaderError(["Context file must contain a JSON object"])
        
        return context
        
    except json.JSONDecodeError as e:
        raise LoaderError([f"Invalid JSON in context file: {e}"])
    except Exception as e:
        raise LoaderError([f"Error reading context file: {e}"])


def load_yaml_file(yaml_path: Path) -> Dict[str, Any]:
    """Load YAML file into dictionary."""
    if not yaml_path.exists():
        raise LoaderError([f"Job file not found: {yaml_path}"])
    
    try:
        yaml = ruamel.yaml.YAML(typ='safe')
        with open(yaml_path, 'r', encoding=DEFAULT_ENCODING) as f:
            data = yaml.load(f)
        
        if not isinstance(data, dict):
            raise LoaderError(["Job file must contain a YAML object"])
        
        return data
        
    except Exception as e:
        raise LoaderError([f"Error reading job file: {e}"])


def validate_job_data(yaml_data: Dict[str, Any]) -> List[str]:
    """Run all validations and collect errors."""
    errors = []
    
    # Template validation
    errors.extend(validate_all_templates(yaml_data))
    
    # Schema validation
    errors.extend(validate_job_schema(yaml_data))
    
    # Component validation
    components = yaml_data.get("components", [])
    for i, comp in enumerate(components):
        comp_errors = validate_component_schema(comp)
        errors.extend([f"Component {i+1}: {error}" for error in comp_errors])
    
    # Connection validation
    connections = yaml_data.get("connections", {})
    errors.extend(validate_connections(connections, components))
    
    return errors


def load_job_file(job_path: Path, context_path: Optional[Path] = None) -> JobModel:
    """Load and validate job file with optional context."""
    yaml_data = load_yaml_file(job_path)
    context = load_context_from_file(context_path) if context_path else {}
    
    return load_job_dict(yaml_data, context)


def load_job_dict(yaml_data: Dict[str, Any], context: Optional[Dict[str, Any]] = None) -> JobModel:
    """Load job from dictionary with optional context."""
    context = context or {}
    
    try:
        resolved_data = resolve_template_yaml(yaml_data, context)
    except Exception as e:
        raise LoaderError([f"Template resolution failed: {e}"])
    
    errors = validate_job_data(resolved_data)
    if errors:
        raise LoaderError(errors)
    
    try:
        return JobModel(**resolved_data)
    except Exception as e:
        raise LoaderError([f"Model creation failed: {e}"])