from pathlib import Path

# Database configuration
DB_PATH = str(Path(__file__).resolve().parents[2] / "sql_db" / "datapy_metadata.db")

# Schema file names
JOB_SCHEMA_FILE = "job.schema.json"

# Build artifact configuration - UPDATED FOR FOLDER FORMAT
JOB_FOLDER_SUFFIX = ""  # No suffix needed, using job name directly
ORIGINAL_YAML_PRESERVED = True  # Keep original filename
DAG_FILE_SUFFIX = "_dag.json"
MANIFEST_FILE_SUFFIX = "_manifest.json"
EXECUTION_METADATA_SUFFIX = "_execution_metadata.json"
SUBJOB_METADATA_SUFFIX = "_subjob_metadata.json"
ASSETS_DIR = "assets"

# Component execution states
COMPONENT_STATE_WAIT = "WAIT"
COMPONENT_STATE_RUNNING = "RUNNING"
COMPONENT_STATE_OK = "OK"
COMPONENT_STATE_ERROR = "ERROR"
COMPONENT_STATE_SKIPPED = "SKIPPED"

# Standard component ports
STANDARD_INPUT_PORT = "main"
STANDARD_OUTPUT_PORT = "main"

# File encoding
DEFAULT_ENCODING = "utf-8"

# Engine metadata
ENGINE_VERSION = "0.0.1"
FRAMEWORK_NAME = "DataPY"

# Template patterns (for resolution only, validation now in schema)
GLOBAL_VAR_DELIMITER = "__"
CONTEXT_VAR_PATTERN = r'\{\{\s*context\.([a-zA-Z_][a-zA-Z0-9_]*)\s*\}\}'
SECRET_VAR_PATTERN = r'\{\{\s*secret\.([a-zA-Z_][a-zA-Z0-9_]*)\s*\}\}'
GLOBAL_VAR_PATTERN = rf'\{{\{{\s*([a-zA-Z_][a-zA-Z0-9_]*){GLOBAL_VAR_DELIMITER}([a-zA-Z_][a-zA-Z0-9_]*)\s*\}}\}}'

# forEach Component Constants
FOREACH_COMPONENT_TYPE = "forEach"

JOBLET_ROOT = Path(__file__).resolve().parents[3] / "joblets"
JOBLET_EXTENSION = ".joblet.yaml"

PORT_NAME_RE = r'^[a-zA-Z0-9_]+$'


DEFAULT_MAX_WORKERS = 8
DEFAULT_TIMEOUT = 3600  
DEFAULT_RETRIES = 1
DEFAULT_CHUNK_SIZE = "200MB"
DEFAULT_MEMORY_PER_WORKER = "4GB"
DEFAULT_THREADS_PER_WORKER = 2
DEFAULT_DASK_WORKERS = 2