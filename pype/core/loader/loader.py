from pathlib import Path
from typing import Any, Dict, List, Optional
import ruamel.yaml
from pydantic import BaseModel, Field, field_validator, ConfigDict
from pype.core.loader.validator import validate_job_file
from pype.core.loader.templater import resolve_template_yaml, TemplateError
from pype.core.utils.constants import DEFAULT_ENCODING


class ThreadpoolConfigModel(BaseModel):
    """Pydantic model for threadpool execution configuration."""
    max_workers: int = Field(default=8, ge=1, le=128)


class DaskConfigModel(BaseModel):
    """Pydantic model for dask execution configuration."""
    cluster_workers: Optional[int] = Field(default=None, ge=1, le=1024)
    threads_per_worker: int = Field(default=2, ge=1, le=32)
    memory_per_worker: str = Field(default="4GB")


class ExecutionConfigModel(BaseModel):
    """Pydantic model for execution configuration."""
    threadpool: Optional[ThreadpoolConfigModel] = None
    dask: Optional[DaskConfigModel] = None


class JobConfigModel(BaseModel):
    """Pydantic model for job configuration with flexible additional fields."""
    
    # Core fields with defaults (all optional)
    retries: int = Field(default=1, ge=0, le=3)
    timeout: int = Field(default=3600, ge=1)
    fail_strategy: str = Field(default="halt")
    execution_mode: str = Field(default="pandas")
    chunk_size: str = Field(default="200MB")
    execution: Optional[ExecutionConfigModel] = None

    # Allow arbitrary additional fields
    model_config = ConfigDict(extra="allow")

    @field_validator('fail_strategy')
    @classmethod
    def validate_fail_strategy(cls, v):
        if v not in ["halt", "continue"]:
            raise ValueError("fail_strategy must be 'halt' or 'continue'")
        return v

    @field_validator('execution_mode')
    @classmethod
    def validate_execution_mode(cls, v):
        if v not in ["pandas", "dask"]:
            raise ValueError("execution_mode must be 'pandas' or 'dask'")
        return v


class JobMetadataModel(BaseModel):
    """Pydantic model for job metadata."""
    name: str = Field(max_length=128)
    desc: Optional[str] = Field(default="", max_length=512)
    version: str
    team: str
    owner: str
    created: str


class ComponentModel(BaseModel):
    """Pydantic model for component definition."""
    name: str = Field(max_length=64)
    type: str
    params: Dict[str, Any] = Field(default_factory=dict)


class ConnectionsModel(BaseModel):
    """Pydantic model for connections definition."""
    data: List[str] = Field(default_factory=list)
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