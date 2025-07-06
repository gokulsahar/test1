from pathlib import Path

# Database configuration
DB_PATH = str(Path(__file__).resolve().parents[2] / "sql_db" / "components.db")

# Template variable delimiters and patterns
GLOBAL_VAR_DELIMITER = "__"

# Template regex patterns
CONTEXT_VAR_PATTERN = r'\{\{\s*context\.([a-zA-Z_][a-zA-Z0-9_]*)\s*\}\}'
SECRET_VAR_PATTERN = r'\{\{\s*secret\.([a-zA-Z_][a-zA-Z0-9_]*)\s*\}\}'
GLOBAL_VAR_PATTERN = rf'\{{\{{\s*([a-zA-Z_][a-zA-Z0-9_]*){GLOBAL_VAR_DELIMITER}([a-zA-Z_][a-zA-Z0-9_]*)\s*\}}\}}'

# Component naming patterns
COMPONENT_NAME_PATTERN = r'^[a-zA-Z_][a-zA-Z0-9_]*$'
PORT_NAME_PATTERN = r'^[a-zA-Z_][a-zA-Z0-9_]*$'

# Connection syntax patterns
DATA_CONNECTION_PATTERN = r'^(.+?)\.(.+?)\s*->\s*(.+?)\.(.+?)$'
CONTROL_TYPE_PATTERN = r'\(([^)]+)\)'
IF_CONTROL_PATTERN = r'^if\d+$'

# Valid control flow keywords
VALID_CONTROL_TYPES = {
    "ok", 
    "error", 
    "parallelize", 
    "synchronise", 
    "subjob_ok", 
    "subjob_error"
}

# Schema file names
JOB_SCHEMA_FILE = "job.schema.json"
COMPONENT_SCHEMA_FILE = "component.schema.json"

# Build artifact configuration
PJOB_EXTENSION = ".pjob"
MANIFEST_FILE = "manifest.json"
ORIGINAL_YAML_FILE = "job_original.yaml"
DAG_FILE = "dag.msgpack"
ASSETS_DIR = "assets"

# Validation limits
MAX_COMPONENT_NAME_LENGTH = 64
MAX_JOB_NAME_LENGTH = 128
MAX_DESCRIPTION_LENGTH = 512

# Default job configuration values
DEFAULT_RETRIES = 1
DEFAULT_TIMEOUT = 3600  # seconds
DEFAULT_FAIL_STRATEGY = "halt"
DEFAULT_DASK_MEMORY_LIMIT = "4GB"
DEFAULT_DASK_PARALLELISM = 4
DEFAULT_DASK_USE_CLUSTER = False

# Component execution states
COMPONENT_STATE_WAIT = "WAIT"
COMPONENT_STATE_RUNNING = "RUNNING"
COMPONENT_STATE_OK = "OK"
COMPONENT_STATE_ERROR = "ERROR"
COMPONENT_STATE_SKIPPED = "SKIPPED"

# Standard component ports (used as defaults)
STANDARD_INPUT_PORT = "main"
STANDARD_OUTPUT_PORT = "main"

# File encoding
DEFAULT_ENCODING = "utf-8"

# Engine metadata
ENGINE_VERSION = "0.0.1"
FRAMEWORK_NAME = "DataPY"