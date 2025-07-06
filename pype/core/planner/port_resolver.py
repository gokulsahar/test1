import re
import networkx as nx
from typing import Dict, List, Tuple, Any, Optional
from pype.core.registry.component_registry import ComponentRegistry


class PortResolutionError(Exception):
    """Base class for port resolution errors."""
    pass


class WildcardLimitExceededError(PortResolutionError):
    """Raised when wildcard expansion exceeds maximum limit."""
    pass


class InvalidPortConnectionError(PortResolutionError):
    """Raised when port connection is invalid."""
    pass


class MultiInputViolationError(PortResolutionError):
    """Raised when allow_multi_in constraint is violated."""
    pass


class PortResolver:
    """Resolves wildcard ports and validates port connectivity."""
    
    def __init__(self, registry: ComponentRegistry, max_wildcard_ports: int = 100):
        """Initialize with component registry and wildcard expansion limits."""
        self.registry = registry
        self.max_wildcard_ports = max_wildcard_ports
    
    def resolve_ports(self, dag: nx.DiGraph) -> Tuple[nx.DiGraph, List[str]]:
        """
        Resolve wildcard ports and validate connectivity.
        
        Args:
            dag: NetworkX DiGraph with potential wildcard ports
            
        Returns:
            (updated_dag, validation_errors)
        """
        errors = []
        
        # Step 1: Expand wildcard ports based on actual usage
        port_mapping = self._expand_wildcard_ports(dag)
        wildcard_errors = self._validate_wildcard_expansions(port_mapping)
        errors.extend(wildcard_errors)
        
        # Step 2: Update DAG with concrete ports
        self._update_dag_with_concrete_ports(dag, port_mapping)
        
        # Step 3: Validate all port connectivity
        connectivity_errors = self._validate_port_connectivity(dag)
        errors.extend(connectivity_errors)
        
        # Step 4: Validate multi-input constraints
        multi_input_errors = self._validate_multi_input_constraints(dag)
        errors.extend(multi_input_errors)
        
        # Step 5: Add port mapping metadata for runtime optimization
        if port_mapping:
            self._add_port_mapping_metadata(dag, port_mapping)
        
        return dag, errors
    
    def _expand_wildcard_ports(self, dag: nx.DiGraph) -> Dict[str, Dict[str, List[str]]]:
        """Expand wildcard ports to concrete ports based on actual connections."""
        port_mapping = {}
        
        for component_name in dag.nodes():
            concrete_ports = self._find_concrete_ports_for_component(dag, component_name)
            
            if concrete_ports['input_ports'] or concrete_ports['output_ports']:
                port_mapping[component_name] = concrete_ports
                
        return port_mapping
    
    def _find_concrete_ports_for_component(self, dag: nx.DiGraph, component_name: str) -> Dict[str, List[str]]:
        """Find all concrete ports used by a component in data edges."""
        input_ports = set()
        output_ports = set()
        
        # Scan all data edges to find concrete port usage
        for source, target, edge_data in dag.edges(data=True):
            if edge_data.get('edge_type') == 'data':
                source_port = edge_data.get('source_port')
                target_port = edge_data.get('target_port')
                
                if source == component_name and source_port:
                    output_ports.add(source_port)
                if target == component_name and target_port:
                    input_ports.add(target_port)
        
        return {
            'input_ports': sorted(list(input_ports)),
            'output_ports': sorted(list(output_ports))
        }
    
    def _validate_wildcard_expansions(self, port_mapping: Dict[str, Dict[str, List[str]]]) -> List[str]:
        """Validate wildcard expansions don't exceed limits."""
        errors = []
        
        for component_name, ports in port_mapping.items():
            for port_type, concrete_ports in ports.items():
                if len(concrete_ports) > self.max_wildcard_ports:
                    errors.append(
                        f"Component '{component_name}' exceeds wildcard limit "
                        f"({len(concrete_ports)} > {self.max_wildcard_ports}) for {port_type}"
                    )
        
        return errors
    
    def _validate_port_connectivity(self, dag: nx.DiGraph) -> List[str]:
        """Validate all data edges reference valid ports."""
        errors = []
        
        # Track connections per component per port for duplicate validation
        port_connections = {}
        
        for source, target, edge_data in dag.edges(data=True):
            if edge_data.get('edge_type') == 'data':
                source_port = edge_data.get('source_port')
                target_port = edge_data.get('target_port')
                
                # Track connections for duplicate validation
                self._track_port_connection(port_connections, target, target_port, 'input')
                self._track_port_connection(port_connections, source, source_port, 'output')
                
                # Validate source port exists and format
                source_errors = self._validate_component_port(
                    source, source_port, 'output', dag
                )
                errors.extend(source_errors)
                
                # Validate target port exists and format
                target_errors = self._validate_component_port(
                    target, target_port, 'input', dag
                )
                errors.extend(target_errors)
        
        # Validate connection constraints
        constraint_errors = self._validate_connection_constraints(dag, port_connections)
        errors.extend(constraint_errors)
        
        return errors
    
    def _validate_component_port(self, component_name: str, port_name: str, 
                                port_type: str, dag: nx.DiGraph) -> List[str]:
        """Validate a specific port exists in component's registry metadata."""
        errors = []
        
        # Get component registry metadata
        component_info = self._get_component_port_info(dag, component_name)
        if not component_info:
            errors.append(f"Component '{component_name}' metadata not found")
            return errors
        
        # Validate port name format first
        format_errors = self._validate_port_name_syntax(port_name, component_info, port_type)
        errors.extend(format_errors)
        
        # Check if port exists in appropriate port list
        port_list_key = f"{port_type}_ports"
        valid_ports = component_info.get(port_list_key, [])
        
        # Check for exact match or wildcard match
        if not self._port_exists_in_list(port_name, valid_ports):
            errors.append(
                f"Component '{component_name}' does not have {port_type} port '{port_name}'. "
                f"Available {port_type} ports: {valid_ports}"
            )
        
        return errors
    
    def _port_exists_in_list(self, port_name: str, valid_ports: List[str]) -> bool:
        """Check if port exists in list, considering wildcards."""
        # Direct match
        if port_name in valid_ports:
            return True
        
        # Check wildcard patterns
        for valid_port in valid_ports:
            if self._is_wildcard_port(valid_port):
                prefix = self._extract_wildcard_prefix(valid_port)
                if prefix and port_name.startswith(prefix):
                    return True
        
        return False
    
    def _validate_multi_input_constraints(self, dag: nx.DiGraph) -> List[str]:
        """Validate allow_multi_in constraints are respected."""
        # This method is now replaced by _validate_connection_constraints
        # Keeping empty for backward compatibility
        return []
    
    def _get_component_port_info(self, dag: nx.DiGraph, component_name: str) -> Optional[Dict[str, Any]]:
        """Extract port information from component node metadata."""
        node_data = dag.nodes.get(component_name, {})
        
        return {
            'input_ports': node_data.get('input_ports', []),
            'output_ports': node_data.get('output_ports', []),
            'allow_multi_in': node_data.get('allow_multi_in', False)
        }
    
    def _update_dag_with_concrete_ports(self, dag: nx.DiGraph, 
                                      port_mapping: Dict[str, Dict[str, List[str]]]) -> None:
        """Update DAG nodes with resolved concrete ports."""
        for component_name, ports in port_mapping.items():
            if component_name in dag.nodes:
                node_data = dag.nodes[component_name]
                
                # Get original ports from registry metadata
                original_input_ports = node_data.get('input_ports', [])
                original_output_ports = node_data.get('output_ports', [])
                
                # Update with concrete ports, removing unused wildcards
                updated_input_ports = self._merge_concrete_ports(
                    original_input_ports, ports.get('input_ports', [])
                )
                updated_output_ports = self._merge_concrete_ports(
                    original_output_ports, ports.get('output_ports', [])
                )
                
                dag.nodes[component_name]['input_ports'] = updated_input_ports
                dag.nodes[component_name]['output_ports'] = updated_output_ports
    
    def _merge_concrete_ports(self, original_ports: List[str], 
                            concrete_ports: List[str]) -> List[str]:
        """Merge original ports with concrete ports, handling wildcards."""
        result = []
        
        for port in original_ports:
            if self._is_wildcard_port(port):
                # Replace wildcard with concrete ports that match the prefix
                prefix = self._extract_wildcard_prefix(port)
                matching_concrete = [
                    cp for cp in concrete_ports 
                    if cp.startswith(prefix) if prefix
                ]
                # Only add if there are actual connections (simplest fix)
                if matching_concrete:
                    result.extend(sorted(matching_concrete))
            else:
                # Keep non-wildcard ports as-is
                result.append(port)
        
        return result
    
    def _add_port_mapping_metadata(self, dag: nx.DiGraph, 
                                 port_mapping: Dict[str, Dict[str, List[str]]]) -> None:
        """Add port mapping metadata to DAG for runtime optimization."""
        if not hasattr(dag.graph, 'port_mapping'):
            dag.graph['port_mapping'] = {}
        
        dag.graph['port_mapping'].update(port_mapping)
        
        # Add merge requirements for engine
        merge_requirements = self._identify_merge_requirements(dag)
        if merge_requirements:
            dag.graph['merge_requirements'] = merge_requirements
    
    def _validate_port_name_format(self, port_name: str) -> bool:
        """Validate port name follows allowed patterns."""
        if not port_name:
            return False
        
        # Allow alphanumeric characters and underscores
        pattern = r'^[a-zA-Z0-9_]+$'
        return bool(re.match(pattern, port_name))
    
    def _is_wildcard_port(self, port_name: str) -> bool:
        """Check if port name is a wildcard pattern."""
        return port_name.endswith('*')
    
    def _extract_wildcard_prefix(self, port_name: str) -> Optional[str]:
        """Extract prefix from wildcard port name."""
        if self._is_wildcard_port(port_name):
            return port_name[:-1]  # Remove the '*'
        return None
        return bool(re.match(pattern, port_name))
    
    def _validate_port_name_syntax(self, port_name: str, component_info: Dict[str, Any], 
                                 port_type: str) -> List[str]:
        """Validate port name syntax including wildcard patterns."""
        errors = []
        
        # Basic format validation
        if not self._validate_port_name_format(port_name):
            errors.append(f"Invalid port name format: '{port_name}' (use alphanumeric + underscore)")
            return errors
        
        # Check if this port should match a wildcard pattern
        port_list_key = f"{port_type}_ports"
        valid_ports = component_info.get(port_list_key, [])
        
        for valid_port in valid_ports:
            if self._is_wildcard_port(valid_port):
                prefix = self._extract_wildcard_prefix(valid_port)
                if prefix and port_name.startswith(prefix):
                    # This is a wildcard port, validate syntax
                    if not self._validate_wildcard_port_syntax(port_name, valid_port):
                        errors.append(
                            f"Wildcard port '{port_name}' must follow pattern '{valid_port}' "
                            f"(requires underscore: {prefix}_suffix)"
                        )
                    break
        
        return errors
    
    def _validate_wildcard_port_syntax(self, port_name: str, wildcard_pattern: str) -> bool:
        """Validate wildcard port follows required underscore syntax."""
        prefix = self._extract_wildcard_prefix(wildcard_pattern)
        if not prefix:
            return True  # Not a wildcard
        
        # Must be prefix_suffix (requires underscore)
        expected_pattern = f"^{re.escape(prefix)}_[a-zA-Z0-9_]+$"
        return bool(re.match(expected_pattern, port_name))
    
    def _track_port_connection(self, port_connections: Dict, component_name: str, 
                             port_name: str, port_type: str) -> None:
        """Track port connections for duplicate validation."""
        if component_name not in port_connections:
            port_connections[component_name] = {'input': {}, 'output': {}}
        
        if port_name not in port_connections[component_name][port_type]:
            port_connections[component_name][port_type][port_name] = 0
        
        port_connections[component_name][port_type][port_name] += 1
    
    def _validate_connection_constraints(self, dag: nx.DiGraph, 
                                       port_connections: Dict) -> List[str]:
        """Validate connection constraints (multi-input, non-wildcard rules)."""
        errors = []
        
        for component_name, connections in port_connections.items():
            component_info = self._get_component_port_info(dag, component_name)
            if not component_info:
                continue
            
            # Validate input connections only
            for port_name, count in connections['input'].items():
                if count > 1:  # Multiple connections to same input port
                    is_wildcard = self._is_wildcard_used(port_name, component_info['input_ports'])
                    
                    if not is_wildcard:
                        # Non-wildcard ports cannot have multiple connections
                        errors.append(
                            f"Non-wildcard input port '{component_name}.{port_name}' "
                            f"cannot have multiple connections ({count} found)"
                        )
                    elif not component_info.get('allow_multi_in', False):
                        # Wildcard ports need allow_multi_in=True for multiple connections
                        errors.append(
                            f"Wildcard input port '{component_name}.{port_name}' has {count} "
                            f"connections but component has allow_multi_in=False"
                        )
            
            # Note: Output connections (fan-out) are always allowed for both wildcard and non-wildcard ports
        
        return errors
    
    def _is_wildcard_used(self, port_name: str, valid_ports: List[str]) -> bool:
        """Check if port name is being used as a wildcard expansion."""
        for valid_port in valid_ports:
            if self._is_wildcard_port(valid_port):
                prefix = self._extract_wildcard_prefix(valid_port)
                if prefix and port_name.startswith(prefix):
                    return True
        return False
    
    def _identify_merge_requirements(self, dag: nx.DiGraph) -> Dict[str, Dict[str, str]]:
        """Identify which ports need data merging for engine preparation."""
        merge_requirements = {}
        
        # Count connections to each input port
        input_counts = {}
        for source, target, edge_data in dag.edges(data=True):
            if edge_data.get('edge_type') == 'data':
                target_port = edge_data.get('target_port')
                key = f"{target}.{target_port}"
                
                if key not in input_counts:
                    input_counts[key] = 0
                input_counts[key] += 1
        
        # Mark ports that need merging
        for key, count in input_counts.items():
            if count > 1:
                component_name, port_name = key.split('.', 1)
                component_info = self._get_component_port_info(dag, component_name)
                
                if (component_info and 
                    component_info.get('allow_multi_in', False) and
                    self._is_wildcard_used(port_name, component_info['input_ports'])):
                    
                    if component_name not in merge_requirements:
                        merge_requirements[component_name] = {}
                    merge_requirements[component_name][port_name] = 'merge_required'
        
        return merge_requirements
    
    def _is_wildcard_port(self, port_name: str) -> bool:
        """Check if port name is a wildcard pattern."""
        return port_name.endswith('*')
    
    def _extract_wildcard_prefix(self, port_name: str) -> Optional[str]:
        """Extract prefix from wildcard port name."""
        if self._is_wildcard_port(port_name):
            return port_name[:-1]  # Remove the '*'
        return None