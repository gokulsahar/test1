from pathlib import Path

# Database configuration
DB_PATH = str(Path(__file__).resolve().parents[2] / "sql_db" / "components.db")

# Schema file names
JOB_SCHEMA_FILE = "job.schema.json"

# Build artifact configuration
PJOB_EXTENSION = ".pjob"
MANIFEST_FILE = "manifest.json"
ORIGINAL_YAML_FILE = "job_original.yaml"
DAG_FILE = "dag.msgpack"
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




JOBLET_ROOT = Path(__file__).resolve().parents[3] / "joblets"
JOBLET_EXTENSION = ".joblet.yaml"



PORT_NAME_RE = r'^[a-zA-Z0-9_]+$'