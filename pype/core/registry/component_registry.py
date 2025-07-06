import sqlite3
import pkgutil
import importlib
from typing import Dict, List, Optional, Any
from .sqlite_backend import BaseSQLBackend


class ComponentRegistry(BaseSQLBackend):
    """Registry for managing component metadata in SQLite database."""
    
    def _validate_component(self, component: Dict[str, Any]) -> bool:
        """Validate component data has required fields and correct types."""
        required_fields = [
            'name', 'class_name', 'module_path', 'category', 'description',
            'input_ports', 'output_ports', 'required_params', 'optional_params',
            'output_globals', 'dependencies', 'startable', 'allow_multi_in',
            'idempotent'
        ]
        
        # Check required fields exist and are not None
        if not all(field in component and component[field] is not None for field in required_fields):
            return False
        
        # Type and format validation
        try:
            # String fields with pattern validation
            import re
            name_pattern = r'^[a-zA-Z][a-zA-Z0-9_]*$'
            if not re.match(name_pattern, component['name']):
                return False
            if not re.match(name_pattern, component['class_name']):
                return False
            if not isinstance(component['module_path'], str) or not component['module_path']:
                return False
            if not isinstance(component['category'], str) or not component['category']:
                return False
            if not isinstance(component['description'], str):
                return False
            
            # List fields
            for list_field in ['input_ports', 'output_ports', 'output_globals', 'dependencies']:
                if not isinstance(component[list_field], list):
                    return False
            
            # Dict fields
            for dict_field in ['required_params', 'optional_params']:
                if not isinstance(component[dict_field], dict):
                    return False
            
            # Boolean fields
            for bool_field in ['startable', 'allow_multi_in', 'idempotent']:
                if not isinstance(component[bool_field], bool):
                    return False
            
            return True
            
        except (TypeError, KeyError, AttributeError):
            return False
    
    def _prepare_component_data(self, component: Dict[str, Any]) -> tuple:
        """Prepare component data for database insertion with defaults."""
        defaults = {
            'category': 'unknown',
            'description': '',
            'input_ports': [],
            'output_ports': [],
            'required_params': {},
            'optional_params': {},
            'output_globals': [],
            'dependencies': [],
            'startable': 0,
            'allow_multi_in': 0,
            'idempotent': 1
        }
        
        data = {**defaults, **component}
        
        return (
            data['name'],
            data['class_name'],
            data['module_path'],
            data['category'],
            data['description'],
            self._serialize_field(data['input_ports']),
            self._serialize_field(data['output_ports']),
            self._serialize_field(data['required_params']),
            self._serialize_field(data['optional_params']),
            self._serialize_field(data['output_globals']),
            self._serialize_field(data['dependencies']),
            int(data['startable']),
            int(data['allow_multi_in']),
            int(data['idempotent']),
            self._get_current_timestamp()
        )
    
    def register_component(self, component: Dict[str, Any]) -> bool:
        """Register component in database (INSERT OR REPLACE)."""
        if not self._validate_component(component):
            return False
        
        data = self._prepare_component_data(component)
        
        with sqlite3.connect(self.db_path) as conn:
            conn.execute("""
                INSERT OR REPLACE INTO components (
                    name, class_name, module_path, category, description,
                    input_ports, output_ports, required_params, optional_params,
                    output_globals, dependencies, startable, allow_multi_in,
                    idempotent, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, data)
            conn.commit()
        
        return True
    
    def get_component(self, name: str) -> Optional[Dict[str, Any]]:
        """Get component details by name."""
        with sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.execute(
                "SELECT * FROM components WHERE name = ?", (name,)
            )
            row = cursor.fetchone()
            
            if not row:
                return None
            
            return {
                'name': row['name'],
                'class_name': row['class_name'],
                'module_path': row['module_path'],
                'category': row['category'],
                'description': row['description'],
                'input_ports': self._deserialize_field(row['input_ports'], []),
                'output_ports': self._deserialize_field(row['output_ports'], []),
                'required_params': self._deserialize_field(row['required_params'], {}),
                'optional_params': self._deserialize_field(row['optional_params'], {}),
                'output_globals': self._deserialize_field(row['output_globals'], []),
                'dependencies': self._deserialize_field(row['dependencies'], []),
                'startable': bool(row['startable']),
                'allow_multi_in': bool(row['allow_multi_in']),
                'idempotent': bool(row['idempotent']),
                'created_at': row['created_at'],
                'updated_at': row['updated_at']
            }
    
    def list_components(self) -> List[Dict[str, Any]]:
        """List all registered components."""
        with sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.execute("SELECT * FROM components ORDER BY name")
            
            components = []
            for row in cursor.fetchall():
                components.append({
                    'name': row['name'],
                    'class_name': row['class_name'],
                    'module_path': row['module_path'],
                    'category': row['category'],
                    'description': row['description'],
                    'input_ports': self._deserialize_field(row['input_ports'], []),
                    'output_ports': self._deserialize_field(row['output_ports'], []),
                    'required_params': self._deserialize_field(row['required_params'], {}),
                    'optional_params': self._deserialize_field(row['optional_params'], {}),
                    'output_globals': self._deserialize_field(row['output_globals'], []),
                    'dependencies': self._deserialize_field(row['dependencies'], []),
                    'startable': bool(row['startable']),
                    'allow_multi_in': bool(row['allow_multi_in']),
                    'idempotent': bool(row['idempotent']),
                    'created_at': row['created_at'],
                    'updated_at': row['updated_at']
                })
            
            return components
    
    def delete_component(self, name: str) -> bool:
        """Delete component from registry."""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.execute("DELETE FROM components WHERE name = ?", (name,))
            conn.commit()
            
            # Return True if a row was actually deleted
            return cursor.rowcount > 0
    
    def _extract_component_metadata(self, cls, class_name: str, module_path: str) -> Dict[str, Any]:
        """Extract component metadata from Python class."""
        config_schema = getattr(cls, 'CONFIG_SCHEMA', {"required": {}, "optional": {}})
        
        return {
            'name': getattr(cls, 'COMPONENT_NAME', class_name.lower()),
            'class_name': class_name,
            'module_path': module_path,
            'category': getattr(cls, 'CATEGORY', 'unknown'),
            'description': getattr(cls, '__doc__', '').strip() if getattr(cls, '__doc__') else '',
            'input_ports': getattr(cls, 'INPUT_PORTS', []),
            'output_ports': getattr(cls, 'OUTPUT_PORTS', []),
            'required_params': config_schema.get('required', {}),
            'optional_params': config_schema.get('optional', {}),
            'output_globals': getattr(cls, 'OUTPUT_GLOBALS', []),
            'dependencies': getattr(cls, 'DEPENDENCIES', []),
            'startable': getattr(cls, 'STARTABLE', False),
            'allow_multi_in': getattr(cls, 'ALLOW_MULTI_IN', False),
            'idempotent': getattr(cls, 'IDEMPOTENT', True)
        }
    
    def _extract_component_from_file(self, component_name: str) -> Optional[Dict[str, Any]]:
        """Extract component data from package by scanning modules."""
        try:
            components_package = importlib.import_module('pype.components')
            
            for importer, modname, ispkg in pkgutil.walk_packages(
                components_package.__path__, components_package.__name__ + "."
            ):
                if modname.endswith('.base'):
                    continue
                    
                try:
                    module = importlib.import_module(modname)
                    
                    for attr_name in dir(module):
                        attr = getattr(module, attr_name)
                        
                        if (isinstance(attr, type) and 
                            hasattr(attr, 'COMPONENT_NAME') and
                            (attr.COMPONENT_NAME == component_name or 
                                attr_name.lower() == component_name.lower())):
                            
                            return self._extract_component_metadata(attr, attr_name, modname)
                            
                except (ImportError, AttributeError):
                    continue
                    
        except ImportError:
            pass
        
        return None
    
    def register_component_by_name(self, component_name: str) -> bool:
        """Register component by scanning for it in the package."""
        component_data = self._extract_component_from_file(component_name)
        if not component_data:
            return False
        
        return self.register_component(component_data)
    
    def register_components_from_package(self, package: str = 'pype.components') -> int:
        """Register all components found in the specified package."""
        registered_count = 0
        
        try:
            pkg = importlib.import_module(package)
            
            for importer, modname, ispkg in pkgutil.walk_packages(
                pkg.__path__, pkg.__name__ + "."
            ):
                if modname.endswith('.base'):
                    continue
                    
                try:
                    module = importlib.import_module(modname)
                    
                    for attr_name in dir(module):
                        attr = getattr(module, attr_name)
                        
                        # Skip if it's a base component
                        if (isinstance(attr, type) and 
                            hasattr(attr, 'COMPONENT_NAME') and
                            attr.COMPONENT_NAME != 'base'):
                            
                            component_data = self._extract_component_metadata(attr, attr_name, modname)
                            
                            if self.register_component(component_data):
                                registered_count += 1
                                
                except (ImportError, AttributeError):
                    continue
                    
        except ImportError:
            pass
        
        return registered_count