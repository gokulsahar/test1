import sqlite3
import json
from datetime import datetime
from pathlib import Path
from typing import Any


class BaseSQLBackend:
    """Base class for SQL database operations with shared utilities."""
    
    def __init__(self, db_path: str = None):
        if db_path is None:
            from pype.core.utils.constants import DB_PATH
            db_path = DB_PATH
        self.db_path = Path(db_path)
        # Create directory if it doesn't exist
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._init_db()
    
    def _init_db(self):
        """Initialize database tables."""
        with sqlite3.connect(self.db_path) as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS components (
                    name TEXT PRIMARY KEY,
                    version TEXT NOT NULL DEFAULT '0.1.0',
                    class_name TEXT NOT NULL,
                    module_path TEXT NOT NULL,
                    category TEXT NOT NULL DEFAULT 'unknown',
                    description TEXT,
                    input_ports TEXT NOT NULL DEFAULT '[]',
                    output_ports TEXT NOT NULL DEFAULT '[]',
                    required_params TEXT NOT NULL DEFAULT '{}',
                    optional_params TEXT NOT NULL DEFAULT '{}',
                    output_globals TEXT NOT NULL DEFAULT '[]',
                    dependencies TEXT NOT NULL DEFAULT '[]',
                    startable INTEGER NOT NULL DEFAULT 0,
                    allow_multi_in INTEGER NOT NULL DEFAULT 0,
                    idempotent INTEGER DEFAULT 1,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            
            # Joblets table
            conn.execute("""
                CREATE TABLE IF NOT EXISTS joblets (
                    name TEXT PRIMARY KEY,
                    file_name TEXT NOT NULL,
                    hash TEXT NOT NULL,
                    input_ports TEXT DEFAULT '[]',
                    output_ports TEXT DEFAULT '[]',
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            conn.commit()
    
    def _serialize_field(self, value: Any) -> str:
        """Serialize complex objects to JSON strings for database storage."""
        if isinstance(value, (list, dict)):
            return json.dumps(value)
        return str(value) if value is not None else ""
    
    def _deserialize_field(self, value: str, default: Any) -> Any:
        """Deserialize JSON strings back to Python objects."""
        if not value:
            return default
        try:
            return json.loads(value)
        except (json.JSONDecodeError, TypeError):
            return default
    
    def _get_current_timestamp(self) -> str:
        """Get current timestamp in ISO format."""
        return datetime.now().isoformat()