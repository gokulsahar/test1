import sqlite3
from typing import Dict, List, Optional, Any
from .sqlite_backend import BaseSQLBackend


class JobletRegistry(BaseSQLBackend):
    """Registry for managing joblet metadata in SQLite database."""
    
    def _validate_joblet(self, joblet: Dict[str, Any]) -> bool:
        """Validate joblet data has required fields and correct types."""
        required_fields = ['name', 'file_name', 'hash']
        
        # Check required fields exist
        if not all(field in joblet and joblet[field] is not None for field in required_fields):
            return False
        
        try:
            # Name validation (same pattern as components)
            import re
            name_pattern = r'^[a-zA-Z][a-zA-Z0-9_]*$'
            if not re.match(name_pattern, joblet['name']):
                return False
            
            # File name validation
            if not isinstance(joblet['file_name'], str) or not joblet['file_name']:
                return False
            
            # Hash validation (should be non-empty string)
            if not isinstance(joblet['hash'], str) or not joblet['hash']:
                return False
            
            # Optional port lists validation
            for port_field in ['input_ports', 'output_ports']:
                if port_field in joblet and not isinstance(joblet[port_field], list):
                    return False
            
            return True
            
        except (TypeError, KeyError, AttributeError):
            return False
    
    def add_joblet(self, joblet: Dict[str, Any]) -> bool:
        """Add a new joblet to the registry."""
        if not self._validate_joblet(joblet):
            return False
        
        with sqlite3.connect(self.db_path) as conn:
            try:
                conn.execute("""
                    INSERT INTO joblets (
                        name, file_name, hash, input_ports, output_ports, created_at, updated_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?)
                """, (
                    joblet['name'],
                    joblet['file_name'],
                    joblet['hash'],
                    self._serialize_field(joblet.get('input_ports', [])),
                    self._serialize_field(joblet.get('output_ports', [])),
                    self._get_current_timestamp(),
                    self._get_current_timestamp()
                ))
                conn.commit()
                return True
            except sqlite3.IntegrityError:
                # Primary key violation (joblet already exists)
                return False
    
    def update_joblet(self, joblet: Dict[str, Any]) -> bool:
        """Update an existing joblet in the registry."""
        if not self._validate_joblet(joblet):
            return False
        
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.execute("""
                UPDATE joblets SET 
                    file_name = ?, hash = ?, input_ports = ?, output_ports = ?, updated_at = ?
                WHERE name = ?
            """, (
                joblet['file_name'],
                joblet['hash'],
                self._serialize_field(joblet.get('input_ports', [])),
                self._serialize_field(joblet.get('output_ports', [])),
                self._get_current_timestamp(),
                joblet['name']
            ))
            conn.commit()
            
            # Return True if a row was actually updated
            return cursor.rowcount > 0
    
    def get_joblet(self, name: str) -> Optional[Dict[str, Any]]:
        """Get joblet details by name."""
        with sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.execute(
                "SELECT * FROM joblets WHERE name = ?", (name,)
            )
            row = cursor.fetchone()
            
            if not row:
                return None
            
            return {
                'name': row['name'],
                'file_name': row['file_name'],
                'hash': row['hash'],
                'input_ports': self._deserialize_field(row['input_ports'], []),
                'output_ports': self._deserialize_field(row['output_ports'], []),
                'created_at': row['created_at'],
                'updated_at': row['updated_at']
            }
    
    def list_joblets(self) -> List[Dict[str, Any]]:
        """List all registered joblets."""
        with sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.execute("SELECT * FROM joblets ORDER BY name")
            
            joblets = []
            for row in cursor.fetchall():
                joblets.append({
                    'name': row['name'],
                    'file_name': row['file_name'],
                    'hash': row['hash'],
                    'input_ports': self._deserialize_field(row['input_ports'], []),
                    'output_ports': self._deserialize_field(row['output_ports'], []),
                    'created_at': row['created_at'],
                    'updated_at': row['updated_at']
                })
            
            return joblets
    
    def delete_joblet(self, name: str) -> bool:
        """Delete a joblet from the registry."""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.execute("DELETE FROM joblets WHERE name = ?", (name,))
            conn.commit()
            
            # Return True if a row was actually deleted
            return cursor.rowcount > 0