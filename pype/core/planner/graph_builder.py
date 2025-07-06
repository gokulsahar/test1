import re
import networkx as nx
from typing import Dict, List, Tuple, Any, Optional
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
        
        # Create data edges with validation
        data_errors = self._validate_data_connections(dag, job_model.connections.data)
        errors.extend(data_errors)
        
        # Create control edges with validation
        control_errors = self._validate_control_connections(dag, job_model.connections.control)
        errors.extend(control_errors)
        
        # Stop if any errors found
        if errors:
            raise GraphBuildError(f"Graph construction failed: {'; '.join(errors)}")
        
        # Create edges after validation passes
        self._create_data_edges(dag, job_model.connections.data)
        self._create_control_edges(dag, job_model.connections.control)
        
        return dag
    
    def _create_component_nodes(self, dag: nx.DiGraph, components: List[ComponentModel]) -> None:
        """Create nodes with rich metadata for runtime performance."""
        for component in components:
            registry_metadata = self._validate_component_exists(component.type)
            
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
    
    def _create_data_edges(self, dag: nx.DiGraph, data_connections: Dict[str, Any]) -> None:
        """Create data edges from connections.data section."""
        connections = data_connections if isinstance(data_connections, list) else list(data_connections.keys())
        
        for connection_str in connections:
            source_comp, source_port, target_comp, target_port = self._parse_data_connection(connection_str)
            dag.add_edge(source_comp, target_comp, **{
                'edge_type': 'data',
                'source_port': source_port,
                'target_port': target_port
            })
    
    def _create_control_edges(self, dag: nx.DiGraph, control_connections: List[str]) -> None:
        """Create control edges from connections.control section."""
        for connection_str in control_connections:
            source_comp, targets, edge_attrs = self._parse_control_connection(connection_str)
            
            # Handle multiple targets (e.g., parallelise)
            target_list = [t.strip() for t in targets.split(',')]
            for target_comp in target_list:
                dag.add_edge(source_comp, target_comp, **{
                    'edge_type': 'control',
                    **edge_attrs
                })
    
    def _parse_control_connection(self, connection_str: str) -> Tuple[str, str, Dict[str, Any]]:
        """
        Parse control connection strings.
        
        Returns:
            (source_component, target_components, edge_attributes)
        """
        # Pattern for: source (trigger[order]): "condition" targets
        if_pattern = r'^(\w+)\s*\(if(\d+)\):\s*"([^"]+)"\s+(.+)$'
        if_match = re.match(if_pattern, connection_str.strip())
        
        if if_match:
            source, order, condition, targets = if_match.groups()
            # TODO: Add expression syntax validation for condition
            return source, targets, {
                'trigger': 'if',
                'condition': condition,
                'order': int(order)
            }
        
        # Pattern for: source (trigger) targets
        general_pattern = r'^(\w+)\s*\((\w+)\)\s+(.+)$'
        general_match = re.match(general_pattern, connection_str.strip())
        
        if general_match:
            source, trigger, targets = general_match.groups()
            return source, targets, {'trigger': trigger}
        
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
    
    def _validate_data_connections(self, dag: nx.DiGraph, data_connections: Dict[str, Any]) -> List[str]:
        """Validate data connections reference existing components."""
        errors = []
        connections = data_connections if isinstance(data_connections, list) else list(data_connections.keys())
        
        for connection_str in connections:
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
        """Validate control connections reference existing components."""
        errors = []
        
        for connection_str in control_connections:
            try:
                source_comp, targets, _ = self._parse_control_connection(connection_str)
                
                if source_comp not in dag.nodes:
                    errors.append(f"Control connection references unknown source component: {source_comp}")
                
                # Check all target components
                target_list = [t.strip() for t in targets.split(',')]
                for target_comp in target_list:
                    if target_comp not in dag.nodes:
                        errors.append(f"Control connection references unknown target component: {target_comp}")
                        
            except InvalidConnectionError as e:
                errors.append(str(e))
        
        return errors