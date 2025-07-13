import re
import networkx as nx
from typing import Dict, List, Tuple, Any, Optional, Set
from collections import defaultdict
from pype.core.loader.loader import JobModel, ComponentModel
from pype.core.registry.component_registry import ComponentRegistry


class GraphBuildError(Exception):
    """Raised when graph construction fails."""
    pass


class ComponentNotFoundError(GraphBuildError):
    """Raised when referenced component not found in registry."""
    pass


class InvalidConnectionError(GraphBuildError):
    """Raised when connection syntax is invalid."""
    pass


class GraphBuilder:
    """Core graph builder for converting JobModel to NetworkX DiGraph."""
    
    def __init__(self, registry: ComponentRegistry):
        """Initialize with component registry for metadata lookup."""
        self.registry = registry
    
    def build_graph(self, job_model: JobModel) -> nx.DiGraph:
        """
        Build NetworkX DiGraph from JobModel with rich metadata.
        
        Args:
            job_model: Validated JobModel from loader
            
        Returns:
            NetworkX DiGraph with rich node and edge attributes
            
        Raises:
            GraphBuildError: If graph construction fails
        """
        dag = nx.DiGraph()
        errors = []
        
        # Create component nodes with validation
        try:
            self._create_component_nodes(dag, job_model.components)
        except GraphBuildError as e:
            errors.append(str(e))
        
        # Validate data edges
        data_errors = self._validate_data_connections(dag, job_model.connections.data)
        errors.extend(data_errors)
        
        # Validate control edges
        control_errors = self._validate_control_connections(dag, job_model.connections.control)
        errors.extend(control_errors)
        
        # Stop if any errors found
        if errors:
            raise GraphBuildError("Graph construction failed:\n" + "\n".join(errors))
        
        # Create edges after validation passes
        self._create_data_edges(dag, job_model.connections.data)
        self._create_control_edges(dag, job_model.connections.control)
        
        return dag
    
    def _create_component_nodes(self, dag: nx.DiGraph, components: List[ComponentModel]) -> None:
        """Create nodes with rich metadata."""
        for component in components:
            registry_metadata = self._validate_component_exists(component.type)
            
            # Check for duplicate component names
            if dag.has_node(component.name):
                raise GraphBuildError(f"Duplicate component name: '{component.name}'")
            
            # Validate required parameters are populated
            self._validate_required_parameters(component.name, component.type, 
                                            registry_metadata.get('required_params', {}), 
                                            component.params)
            
            # Filter out timestamp columns to reduce metadata size
            filtered_metadata = {k: v for k, v in registry_metadata.items() 
                                if k not in ['created_at', 'updated_at']}
            
            dag.add_node(component.name, **{
                'component_type': component.type,
                'startable': registry_metadata['startable'],
                'allow_multi_in': registry_metadata['allow_multi_in'],
                'idempotent': registry_metadata['idempotent'],
                'config': component.params,
                'registry_metadata': filtered_metadata,
                'input_ports': registry_metadata['input_ports'],
                'output_ports': registry_metadata['output_ports'],
                'dependencies': registry_metadata['dependencies']
            })

    def _validate_required_parameters(self, component_name: str, component_type: str, 
                                    required_params: Dict[str, Any], config: Dict[str, Any]) -> None:
        """Validate that all required parameters are populated with values."""
        for param_name in required_params:
            if param_name not in config:
                raise GraphBuildError(
                    f"Component '{component_name}' (type: {component_type}) missing required parameter: {param_name}"
                )
            
            param_value = config[param_name]
            
            if param_value is None:
                raise GraphBuildError(
                    f"Component '{component_name}' (type: {component_type}) required parameter '{param_name}' cannot be None"
                )
            
            # Recursive validation for nested structures
            self._validate_parameter_recursively(component_name, component_type, param_name, param_value)

    def _validate_parameter_recursively(self, component_name: str, component_type: str, 
                                    param_path: str, value: Any) -> None:
        """Recursively validate parameter values, checking for empty strings in nested structures."""
        if isinstance(value, str) and value.strip() == "":
            raise GraphBuildError(
                f"Component '{component_name}' (type: {component_type}) parameter '{param_path}' cannot be empty string"
            )
        elif isinstance(value, dict):
            if len(value) == 0:
                raise GraphBuildError(
                    f"Component '{component_name}' (type: {component_type}) parameter '{param_path}' cannot be empty dict"
                )
            for key, nested_value in value.items():
                nested_path = f"{param_path}.{key}"
                self._validate_parameter_recursively(component_name, component_type, nested_path, nested_value)
        elif isinstance(value, list):
            if len(value) == 0:
                raise GraphBuildError(
                    f"Component '{component_name}' (type: {component_type}) parameter '{param_path}' cannot be empty list"
                )
            for idx, item in enumerate(value):
                item_path = f"{param_path}[{idx}]"
                self._validate_parameter_recursively(component_name, component_type, item_path, item)
    
    def _validate_component_exists(self, component_type: str) -> Dict[str, Any]:
        """
        Validate component exists in registry and return full metadata.
        
        Returns:
            Full component metadata from registry
            
        Raises:
            ComponentNotFoundError: If component type not in registry
        """
        metadata = self.registry.get_component(component_type)
        if not metadata:
            raise ComponentNotFoundError(f"Component type '{component_type}' not found in registry")
        return metadata
    
    def _create_data_edges(self, dag: nx.DiGraph, data_connections: List[str]) -> None:
        """Create data edges from connections.data section."""
        for connection_str in data_connections:
            source_comp, source_port, target_comp, target_port = self._parse_data_connection(connection_str)
            
            # Check for duplicate connections between same components
            if dag.has_edge(source_comp, target_comp):
                raise GraphBuildError(f"Multiple connections between '{source_comp}' and '{target_comp}' not allowed")
            
            dag.add_edge(source_comp, target_comp, **{
                'edge_type': 'data',
                'source_port': source_port,
                'target_port': target_port
            })
    
    def _create_control_edges(self, dag: nx.DiGraph, control_connections: List[str]) -> None:
        """Create control edges from connections.control section."""
        for connection_str in control_connections:
            source_comp, target_comp, edge_attrs = self._parse_control_connection(connection_str)
            
            # Check for duplicate connections between same components
            if dag.has_edge(source_comp, target_comp):
                raise GraphBuildError(f"Multiple connections between '{source_comp}' and '{target_comp}' not allowed")
            
            dag.add_edge(source_comp, target_comp, **{
                'edge_type': 'control',
                **edge_attrs
            })
    
    def _parse_control_connection(self, connection_str: str) -> Tuple[str, str, Dict[str, Any]]:
        """
        Parse control connection strings.
        
        Returns:
            (source_component, target_component, edge_attributes)
        """
        # Pattern for: source (if[order]): "condition" target
        if_pattern = r'^(\w+)\s*\(if(\d+)\):\s*"([^"]+)"\s+(\w+)$'
        if_match = re.match(if_pattern, connection_str.strip())
        
        if if_match:
            source, order, condition, target = if_match.groups()
            return source, target, {
                'trigger': 'if',
                'condition': condition,
                'order': int(order)
            }
        
        # Pattern for: source (trigger) target
        general_pattern = r'^(\w+)\s*\((\w+)\)\s+(\w+)$'
        general_match = re.match(general_pattern, connection_str.strip())
        
        if general_match:
            source, trigger, target = general_match.groups()
            return source, target, {'trigger': trigger}
        
        raise InvalidConnectionError(f"Invalid control connection syntax: {connection_str}")
    
    def _parse_data_connection(self, connection_str: str) -> Tuple[str, str, str, str]:
        """
        Parse data connection strings like 'extract_crm.main -> clean_data.input'.
        
        Returns:
            (source_component, source_port, target_component, target_port)
        """
        pattern = r'^(\w+)\.(\w+)\s*->\s*(\w+)\.(\w+)$'
        match = re.match(pattern, connection_str.strip())
        
        if not match:
            raise InvalidConnectionError(f"Invalid data connection syntax: {connection_str}")
        
        return match.groups()
    
    def _validate_data_connections(self, dag: nx.DiGraph, data_connections: List[str]) -> List[str]:
        """Validate data connections reference existing components."""
        errors = []
        
        for connection_str in data_connections:
            try:
                source_comp, source_port, target_comp, target_port = self._parse_data_connection(connection_str)
                
                if source_comp not in dag.nodes:
                    errors.append(f"Data connection references unknown source component: {source_comp}")
                if target_comp not in dag.nodes:
                    errors.append(f"Data connection references unknown target component: {target_comp}")
                    
            except InvalidConnectionError as e:
                errors.append(str(e))
        
        return errors
    
    def _validate_control_connections(self, dag: nx.DiGraph, control_connections: List[str]) -> List[str]:
        """Validate control connections."""
        errors = []
        
        # Track control edges by source component to detect multi control edges of same type
        control_edge_tracker = defaultdict(lambda: defaultdict(set))  # {source: {trigger_type: {targets or orders}}}
        
        for connection_str in control_connections:
            try:
                source_comp, target_comp, edge_attrs = self._parse_control_connection(connection_str)
                trigger = edge_attrs.get('trigger')
                
                # Validate components exist
                if source_comp not in dag.nodes:
                    errors.append(f"Control connection references unknown source component: {source_comp}")
                    continue
                if target_comp not in dag.nodes:
                    errors.append(f"Control connection references unknown target component: {target_comp}")
                    continue
                
                # Validate control edge constraints
                validation_error = self._validate_control_edge_constraints(
                    source_comp, target_comp, trigger, edge_attrs, control_edge_tracker
                )
                if validation_error:
                    errors.append(validation_error)
                    
            except InvalidConnectionError as e:
                errors.append(str(e))
        
        return errors
    
    def _validate_control_edge_constraints(self, source_comp: str, target_comp: str, 
                                         trigger: str, edge_attrs: Dict[str, Any],
                                         control_edge_tracker: Dict[str, Dict[str, Set]]) -> Optional[str]:
        """
        Validate control edge constraints based on trigger type.
        
        Rules:
        - ok/subjob_ok: Multiple allowed to different targets (implicit parallelization)
        - error/subjob_error: Only one per component
        - if: Multiple allowed but unique order numbers per component
        
        Returns:
            Error message if constraint violated, None if valid
        """
        tracker = control_edge_tracker[source_comp]
        
        if trigger in ['ok', 'subjob_ok']:
            # Allow multiple edges to different targets (implicit parallelization)
            tracker[trigger].add(target_comp)
            
        elif trigger in ['error', 'subjob_error']:
            # Only one error edge per component
            if trigger in tracker:
                existing_targets = list(tracker[trigger])
                return (f"Component '{source_comp}' already has a '{trigger}' control edge to '{existing_targets[0]}'. "
                       f"Only one '{trigger}' edge allowed per component.")
            tracker[trigger].add(target_comp)
            
        elif trigger == 'if':
            # If edges can have multiple per component, but only one per order
            order = edge_attrs.get('order')
            if order is None:
                return f"If control edge from '{source_comp}' missing order number"
            
            if order in tracker[trigger]:
                return (f"Component '{source_comp}' already has an 'if{order}' control edge. "
                       f"Only one 'if' edge allowed per order number.")
            tracker[trigger].add(order)
            
        else:
            return f"Unknown control edge trigger: '{trigger}'"
        
        return None