import re
import networkx as nx
from typing import Dict, List, Tuple, Any, Optional
from collections import defaultdict, Counter
from pype.core.utils.constants import PORT_NAME_RE

# Pre-compiled regex patterns for performance
_PORT_NAME_RE = re.compile(PORT_NAME_RE)


class PortResolutionError(Exception):
    """Fatal port resolution error that prevents execution."""
    pass


class WildcardMatcher:
    """Efficient wildcard pattern matcher with strict validation semantics."""
    
    def __init__(self, patterns: List[str]):
        """Initialize with list of patterns (wildcard and exact)."""
        self.exact_ports = set()
        self.wildcard_patterns = []
        self._validation_errors = []
        
        for pattern in patterns:
            if pattern.endswith('*'):
                prefix = pattern[:-1]
                if not prefix:  # Collect bare "*" as validation error
                    self._validation_errors.append(f"Invalid wildcard pattern: '{pattern}' (bare asterisk not allowed)")
                    continue
                self.wildcard_patterns.append((prefix, self._compile_underscore_regex(prefix)))
            else:
                self.exact_ports.add(pattern)
    
    def get_validation_errors(self) -> List[str]:
        """Get any validation errors from pattern initialization."""
        return self._validation_errors.copy()
    
    def get_wildcard_prefix(self, port: str) -> Optional[str]:
        """Return wildcard prefix if port matches a VALID wildcard pattern."""
        for prefix, regex in self.wildcard_patterns:
            if regex.fullmatch(port):  # Only valid expansions (with underscore)
                return prefix
        return None
    
    def exists(self, port: str) -> bool:
        """Check if port exists with valid syntax (exact match or valid wildcard expansion)."""
        return port in self.exact_ports or self.get_wildcard_prefix(port) is not None
    
    def uses_wildcard(self, port: str) -> bool:
        """Check if port uses a valid wildcard expansion."""
        return self.get_wildcard_prefix(port) is not None
    
    def matches_any_wildcard_prefix(self, port: str) -> bool:
        """Check if port starts with any wildcard prefix (for merging logic)."""
        for prefix, _ in self.wildcard_patterns:
            if port.startswith(prefix):
                return True
        return False
    
    @staticmethod
    def _compile_underscore_regex(prefix: str) -> re.Pattern:
        """Compile regex that enforces underscore syntax for wildcard ports."""
        escaped_prefix = re.escape(prefix)
        pattern = f"^{escaped_prefix}_(?:{PORT_NAME_RE})$"
        return re.compile(pattern)


class PortResolver:
    """Resolves wildcard ports and validates port connectivity."""
    
    def resolve_ports(self, dag: nx.DiGraph) -> Tuple[nx.DiGraph, List[str]]:
        """
        Resolve wildcard ports and validate connectivity.
        
        Args:
            dag: NetworkX DiGraph with potential wildcard ports
            
        Returns:
            (updated_dag, validation_errors)
        """
        errors = []
        
        try:
            # Step 1: Find concrete ports from actual connections
            port_mapping = self._find_concrete_ports(dag)
            
            # Step 2: Update DAG with concrete ports
            self._update_dag_ports(dag, port_mapping)
            
            # Step 3: Validate all port connections (single pass with cached matchers)
            errors.extend(self._validate_all_connections_optimized(dag))
            
            # Step 4: Add optimization metadata
            self._add_metadata(dag, port_mapping)
            
        except PortResolutionError as e:
            errors.append(str(e))
        
        return dag, errors
    
    # === CORE LOGIC ===
    
    def _find_concrete_ports(self, dag: nx.DiGraph) -> Dict[str, Dict[str, List[str]]]:
        """edge scan to find concrete ports."""
        inputs_by_node = defaultdict(set)
        outputs_by_node = defaultdict(set)
        
        # Single pass through edges - O(E) instead of O(N*E)
        for source, target, edge_data in dag.edges(data=True):
            if edge_data.get('edge_type') == 'data':
                source_port = edge_data.get('source_port')
                target_port = edge_data.get('target_port')
                
                # Collect missing metadata as validation errors instead of raising
                if source_port is None:
                    raise PortResolutionError(f"Data edge {source} -> {target} missing source_port")
                if target_port is None:
                    raise PortResolutionError(f"Data edge {source} -> {target} missing target_port")
                
                outputs_by_node[source].add(source_port)
                inputs_by_node[target].add(target_port)
        
        # Build mapping only for nodes with actual ports
        port_mapping = {}
        for node in dag.nodes():
            input_ports = sorted(inputs_by_node[node])
            output_ports = sorted(outputs_by_node[node])
            
            if input_ports or output_ports:
                port_mapping[node] = {
                    'input_ports': input_ports,
                    'output_ports': output_ports
                }
        
        return port_mapping
    
    def _update_dag_ports(self, dag: nx.DiGraph, port_mapping: Dict[str, Dict[str, List[str]]]) -> None:
        """Update DAG nodes with resolved concrete ports using cached matchers."""
        for component_name, ports in port_mapping.items():
            node_data = dag.nodes[component_name]
            
            # Update input ports with unified wildcard logic
            original_inputs = node_data.get('input_ports', [])
            node_data['input_ports'] = self._merge_ports_with_matcher(
                original_inputs, ports.get('input_ports', [])
            )
            
            # Update output ports with unified wildcard logic
            original_outputs = node_data.get('output_ports', [])
            node_data['output_ports'] = self._merge_ports_with_matcher(
                original_outputs, ports.get('output_ports', [])
            )
    
    def _merge_ports_with_matcher(self, original_ports: List[str], concrete_ports: List[str]) -> List[str]:
        """Merge ports using WildcardMatcher for consistent wildcard logic."""
        if not concrete_ports:
            return original_ports
        
        # Create matcher from original patterns
        matcher = WildcardMatcher(original_ports)
        result = []
        
        # Add exact ports that exist in original
        for port in original_ports:
            if not port.endswith('*'):
                result.append(port)
        
        # Add concrete ports that match wildcard patterns
        for concrete_port in concrete_ports:
            if matcher.matches_any_wildcard_prefix(concrete_port):
                result.append(concrete_port)
        
        return sorted(set(result))  # Remove duplicates and sort
    
    # === VALIDATION===
    
    def _validate_all_connections_optimized(self, dag: nx.DiGraph) -> List[str]:
        """Single-pass validation with cached WildcardMatchers per component."""
        errors = []
        connection_counts: Dict[Tuple[str, str], int] = Counter()
        
        # Cache matchers per component to avoid rebuilding
        component_matchers: Dict[Tuple[str, str], WildcardMatcher] = {}
        
        # Single pass through data edges
        for source, target, edge_data in dag.edges(data=True):
            if edge_data.get('edge_type') != 'data':
                continue
                
            source_port = edge_data.get('source_port')
            target_port = edge_data.get('target_port')
            
            # Track connections using tuple keys
            connection_counts[(target, target_port)] += 1
            
            # Validate both ports with cached matchers
            errors.extend(self._validate_port_with_cache(
                source, source_port, 'output', dag, component_matchers
            ))
            errors.extend(self._validate_port_with_cache(
                target, target_port, 'input', dag, component_matchers
            ))
        
        # Validate multi-input constraints using cached matchers
        errors.extend(self._validate_multi_input_with_cache(
            dag, connection_counts, component_matchers
        ))
        
        return errors
    
    def _validate_port_with_cache(self, component: str, port: str, port_type: str, 
                                 dag: nx.DiGraph, matcher_cache: Dict[Tuple[str, str], WildcardMatcher]) -> List[str]:
        """Validate port using cached WildcardMatcher."""
        errors = []
        
        # Get component metadata
        node_data = dag.nodes.get(component, {})
        valid_ports = node_data.get(f"{port_type}_ports", [])
        
        if not valid_ports:
            errors.append(f"Component '{component}' metadata not found")
            return errors
        
        # Validate port name format using pre-compiled regex
        if not _PORT_NAME_RE.fullmatch(port):
            errors.append(f"Invalid port name format: '{port}' (use alphanumeric + underscore)")
            return errors
        
        # Get or create cached matcher for this component and port type
        cache_key = (component, port_type)
        if cache_key not in matcher_cache:
            matcher = WildcardMatcher(valid_ports)
            # Add any pattern validation errors
            errors.extend([f"Component '{component}': {err}" for err in matcher.get_validation_errors()])
            matcher_cache[cache_key] = matcher
        else:
            matcher = matcher_cache[cache_key]
        
        # Check existence with strict validation (only valid expansions)
        if not matcher.exists(port):
            errors.append(
                f"Component '{component}' has no valid {port_type} port '{port}'. "
                f"Available: {valid_ports}"
            )
        
        return errors
    
    def _validate_multi_input_with_cache(self, dag: nx.DiGraph, 
                                        connection_counts: Dict[Tuple[str, str], int],
                                        matcher_cache: Dict[Tuple[str, str], WildcardMatcher]) -> List[str]:
        """Validate multi-input constraints using cached matchers."""
        errors = []
        
        for (component, port), count in connection_counts.items():
            if count <= 1:
                continue
                
            node_data = dag.nodes.get(component, {})
            
            # Get or create cached matcher
            cache_key = (component, 'input')
            if cache_key not in matcher_cache:
                input_ports = node_data.get('input_ports', [])
                matcher_cache[cache_key] = WildcardMatcher(input_ports)
            
            matcher = matcher_cache[cache_key]
            uses_wildcard = matcher.uses_wildcard(port)
            
            if not uses_wildcard:
                # Non-wildcard ports cannot have multiple connections
                errors.append(
                    f"Non-wildcard input port '{component}.{port}' "
                    f"cannot have multiple connections ({count} found)"
                )
            elif not node_data.get('allow_multi_in', False):
                # Wildcard ports need allow_multi_in=True
                errors.append(
                    f"Component '{component}' has allow_multi_in=False but "
                    f"wildcard port '{port}' has {count} connections"
                )
        
        return errors
    
    # === UTILITIES ===
    
    def _add_metadata(self, dag: nx.DiGraph, port_mapping: Dict[str, Dict[str, List[str]]]) -> None:
        """Add port mapping metadata for runtime optimization."""
        dag.graph['port_mapping'] = port_mapping
        dag.graph['merge_requirements'] = self._find_merge_requirements_optimized(dag)
    
    def _find_merge_requirements_optimized(self, dag: nx.DiGraph) -> Dict[str, Dict[str, str]]:
        """Find ports that need data merging using cached matchers."""
        merge_requirements = {}
        input_counts: Dict[Tuple[str, str], int] = Counter()
        matcher_cache: Dict[str, WildcardMatcher] = {}
        
        # Count connections to each input port using tuple keys
        for source, target, edge_data in dag.edges(data=True):
            if edge_data.get('edge_type') == 'data':
                target_port = edge_data.get('target_port')
                input_counts[(target, target_port)] += 1
        
        # Mark ports needing merge
        for (component, port), count in input_counts.items():
            if count <= 1:
                continue
                
            node_data = dag.nodes.get(component, {})
            
            # Check if component allows multi-input and port uses wildcard
            if node_data.get('allow_multi_in', False):
                # Get or create cached matcher
                if component not in matcher_cache:
                    input_ports = node_data.get('input_ports', [])
                    matcher_cache[component] = WildcardMatcher(input_ports)
                
                matcher = matcher_cache[component]
                if matcher.uses_wildcard(port):
                    if component not in merge_requirements:
                        merge_requirements[component] = {}
                    merge_requirements[component][port] = 'merge_required'
        
        return merge_requirements