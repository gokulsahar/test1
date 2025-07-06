from pathlib import Path
from typing import Any, Dict, List, Optional
import ruamel.yaml
from pydantic import BaseModel, Field, field_validator
from pype.core.loader.validator import validate_job_file
from pype.core.loader.templater import resolve_template_yaml, TemplateError
from pype.core.utils.constants import DEFAULT_ENCODING


class JobConfigModel(BaseModel):
    """Pydantic model for job configuration."""
    retries: int = Field(default=1, ge=0, le=10)
    timeout: int = Field(default=3600, ge=1)
    fail_strategy: str = Field(default="halt", regex="^(halt|continue)$")
    dask_config: Optional[Dict[str, Any]] = None


class JobMetadataModel(BaseModel):
    """Pydantic model for job metadata."""
    name: str = Field(regex=r"^[a-zA-Z][a-zA-Z0-9_-]*$", max_length=128)
    desc: Optional[str] = Field(default="", max_length=512)
    version: str = Field(regex=r"^\d+\.\d+\.\d+$")
    team: str = Field(regex=r"^[a-zA-Z][a-zA-Z0-9_-]*$")
    owner: str = Field(regex=r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$")
    created: str = Field(regex=r"^\d{4}-\d{2}-\d{2}$")


class ComponentModel(BaseModel):
    """Pydantic model for component definition."""
    name: str = Field(regex=r"^[a-zA-Z][a-zA-Z0-9]*$", max_length=64)
    type: str = Field(regex=r"^[a-zA-Z][a-zA-Z0-9_]*$")
    params: Dict[str, Any] = Field(default_factory=dict)


class ConnectionsModel(BaseModel):
    """Pydantic model for connections definition."""
    data: Dict[str, Any] = Field(default_factory=dict)
    control: List[str] = Field(default_factory=list)


class JobModel(BaseModel):
    """Complete Pydantic model for job definition."""
    job: JobMetadataModel
    job_config: JobConfigModel = Field(default_factory=JobConfigModel)
    components: List[ComponentModel] = Field(min_items=1)
    connections: ConnectionsModel

    @field_validator('components')
    @classmethod
    def validate_unique_component_names(cls, v):
        """Ensure component names are unique."""
        names = [comp.name for comp in v]
        if len(names) != len(set(names)):
            duplicates = [name for name in names if names.count(name) > 1]
            raise ValueError(f"Duplicate component names found: {list(set(duplicates))}")
        return v


class LoaderError(Exception):
    """Custom exception for loader errors."""
    
    def __init__(self, message: str, errors: List[str] = None):
        self.errors = errors or []
        super().__init__(message)


def load_job_yaml(file_path: Path, context: Optional[Dict[str, Any]] = None) -> JobModel:
    """
    Load and validate job YAML file with optimized single-pass validation.
    
    Args:
        file_path: Path to job YAML file
        context: Optional context variables for template resolution
        
    Returns:
        Validated JobModel instance
        
    Raises:
        LoaderError: If validation fails or file cannot be loaded
    """
    if not file_path.exists():
        raise LoaderError(f"Job file not found: {file_path}")
    
    # Step 1: Schema validation (includes syntax, patterns, structure)
    validation_errors = validate_job_file(file_path)
    if validation_errors:
        raise LoaderError("Job validation failed", validation_errors)
    
    # Step 2: Load YAML (we know it's valid from schema validation)
    try:
        yaml = ruamel.yaml.YAML(typ='safe')
        with open(file_path, 'r', encoding=DEFAULT_ENCODING) as f:
            job_data = yaml.load(f)
    except Exception as e:
        raise LoaderError(f"Error loading YAML file: {e}")
    
    # Step 3: Template resolution (if context provided)
    if context:
        try:
            job_data = resolve_template_yaml(job_data, context)
        except TemplateError as e:
            raise LoaderError(f"Template resolution failed: {e}")
        except Exception as e:
            raise LoaderError(f"Unexpected error during template resolution: {e}")
    
    # Step 4: Create Pydantic model (final validation layer)
    try:
        return JobModel(**job_data)
    except Exception as e:
        raise LoaderError(f"Job model validation failed: {e}")


def load_job_from_dict(job_data: Dict[str, Any], context: Optional[Dict[str, Any]] = None) -> JobModel:
    """
    Load job from dictionary data with validation.
    
    Args:
        job_data: Job data as dictionary
        context: Optional context variables for template resolution
        
    Returns:
        Validated JobModel instance
        
    Raises:
        LoaderError: If validation fails
    """
    # Template resolution (if context provided)
    if context:
        try:
            job_data = resolve_template_yaml(job_data, context)
        except TemplateError as e:
            raise LoaderError(f"Template resolution failed: {e}")
        except Exception as e:
            raise LoaderError(f"Unexpected error during template resolution: {e}")
    
    # Create and validate Pydantic model
    try:
        return JobModel(**job_data)
    except Exception as e:
        raise LoaderError(f"Job model validation failed: {e}")