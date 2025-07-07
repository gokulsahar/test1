import re
import json
import networkx as nx
from typing import Dict, List, Tuple, Any, Set, Optional
from dataclasses import dataclass
from collections import defaultdict, deque
from pype.core.registry.component_registry import ComponentRegistry
from pype.core.utils.constants import GLOBAL_VAR_PATTERN


@dataclass
class ValidationError:
    """Represents a validation error that prevents execution."""
    code: str
    message: str
    component: Optional[str] = None
    field_path: Optional[str] = None
    severity: str = "ERROR"


@dataclass 
class ValidationWarning:
    """Represents a validation warning that may impact performance."""
    code: str
    message: str
    component: Optional[str] = None
    severity: str = "WARNING"
    recommendation: Optional[str] = None


class StructureValidationError(Exception):
    """Base class for structure validation errors."""
    pass


class CycleDetectedError(StructureValidationError):
    """Raised when cycles are detected in DAG."""
    pass


class UnreachableComponentError(StructureValidationError):
    """Raised when components are unreachable."""
    pass


class InvalidGlobalReferenceError(StructureValidationError):
    """Raised when global variable references are invalid."""
    pass


class StructureValidator:
    """Validates DAG structural integrity and execution feasibility."""
    
    def __init__(self, registry: ComponentRegistry):
        """Initialize with component registry for metadata validation."""
        self.registry = registry
        self.HIGH_FAN_OUT_THRESHOLD = 10
        self.HIGH_COMPLEXITY_THRESHOLD = 50
        self.MAX_PATH_DEPTH_THRESHOLD = 20
    
    def validate_structure(self, dag: nx.DiGraph) -> Tuple[List[ValidationError], List[ValidationWarning]]:
        """
        Perform comprehensive structural validation of DAG.
        
        Args:
            dag: NetworkX DiGraph to validate
            
        Returns:
            (validation_errors, validation_warnings)
        """
        errors = []
        warnings = []
        
        try:
            # Critical validations (fail-fast)
            cycle_errors = self._detect_cycles(dag)
            if cycle_errors:
                raise CycleDetectedError(f"Cycles detected: {[e.message for e in cycle_errors]}")
            
            reachability_errors = self._analyze_reachability(dag)
            if reachability_errors:
                raise UnreachableComponentError(f"Unreachable components: {[e.component for e in reachability_errors]}")
            
            startable_errors = self._validate_startable_components(dag)
            if startable_errors:
                errors.extend(startable_errors)
                raise StructureValidationError(f"Startable component validation failed: {[e.message for e in startable_errors]}")
            
            # Mark iterator boundaries
            self._mark_iterator_boundaries(dag)
            
            # Non-critical validations
            errors.extend(self._validate_global_references(dag))
            errors.extend(self._validate_iterator_components(dag))
            errors.extend(self._validate_component_dependencies(dag))
            errors.extend(self._validate_edge_connectivity(dag))
            
            # Performance warnings
            warnings.extend(self._validate_performance_characteristics(dag))
            
        except StructureValidationError:
            # Re-raise critical errors
            raise
        except Exception as e:
            errors.append(ValidationError(
                code="VALIDATION_FAILED",
                message=f"Structure validation failed: {str(e)}"
            ))
        
        return errors, warnings
    
    def _detect_cycles(self, dag: nx.DiGraph) -> List[ValidationError]:
        """Detect cycles in DAG using NetworkX."""
        errors = []
        
        if not nx.is_directed_acyclic_graph(dag):
            # Find one cycle for detailed reporting
            try:
                cycle = nx.find_cycle(dag, orientation='original')
                cycle_path = self._format_cycle_path(cycle)
                errors.append(ValidationError(
                    code="CYCLE_DETECTED",
                    message=f"Circular dependency detected: {cycle_path}"
                ))
            except nx.NetworkXNoCycle:
                # Shouldn't happen, but handle gracefully
                errors.append(ValidationError(
                    code="CYCLE_DETECTED",
                    message="Circular dependency detected but unable to identify path"
                ))
        
        return errors
    
    def _format_cycle_path(self, cycle: List[Tuple]) -> str:
        """Format cycle path for human-readable output."""
        path_parts = []
        for source, target, edge_data in cycle:
            edge_type = edge_data.get('edge_type', 'unknown')
            if edge_type == 'control':
                trigger = edge_data.get('trigger', 'unknown')
                path_parts.append(f"{source} --(control:{trigger})--> {target}")
            else:
                path_parts.append(f"{source} --(data)--> {target}")
        return " -> ".join([part.split("-->")[0].strip() for part in path_parts]) + f" --> {cycle[0][0]}"
    
    def _analyze_reachability(self, dag: nx.DiGraph) -> List[ValidationError]:
        """Analyze component reachability from startable components."""
        errors = []
        
        startable_components = self._find_startable_components(dag)
        if not startable_components:
            return errors  # Will be caught by startable validation
        
        # Find all reachable components
        reachable = set()
        for start_comp in startable_components:
            reachable.add(start_comp)
            reachable.update(nx.descendants(dag, start_comp))
        
        # Find unreachable components
        all_components = set(dag.nodes())
        unreachable = all_components - reachable
        
        for comp in unreachable:
            errors.append(ValidationError(
                code="UNCONNECTED_NODE",
                message=f"Component '{comp}' is not reachable from any startable component",
                component=comp
            ))
        
        return errors
    
    def _find_startable_components(self, dag: nx.DiGraph) -> List[str]:
        """Find components that can start execution independently."""
        startable_components = []
        
        for node, node_data in dag.nodes(data=True):
            # Must have startable=True in registry metadata
            is_registry_startable = node_data.get('startable', False)
            
            # Must have no incoming control edges
            has_incoming_control = any(
                edge_data.get('edge_type') == 'control'
                for _, _, edge_data in dag.in_edges(node, data=True)
            )
            
            if is_registry_startable and not has_incoming_control:
                startable_components.append(node)
        
        return startable_components
    
    def _validate_startable_components(self, dag: nx.DiGraph) -> List[ValidationError]:
        """Validate startable component configuration."""
        errors = []
        
        startable_components = self._find_startable_components(dag)
        
        if not startable_components:
            errors.append(ValidationError(
                code="NO_STARTABLE_COMPONENTS",
                message="No components can start execution (must have startable=True and no incoming control edges)"
            ))
        
        # Validate each startable component
        for comp in startable_components:
            node_data = dag.nodes[comp]
            required_params = node_data.get('registry_metadata', {}).get('required_params', {})
            config = node_data.get('config', {})
            
            for param_name in required_params:
                if param_name not in config:
                    errors.append(ValidationError(
                        code="STARTABLE_MISSING_PARAM",
                        message=f"Startable component '{comp}' missing required parameter: {param_name}",
                        component=comp
                    ))
        
        return errors
    
    def _mark_iterator_boundaries(self, dag: nx.DiGraph) -> None:
        """Mark components with their iterator boundary using recursive algorithm."""
        # Initialize all components with empty iterator boundary
        for node in dag.nodes():
            dag.nodes[node]['iterator_boundary'] = ""
        
        # Find all iterator components
        iterator_components = [
            node for node, data in dag.nodes(data=True)
            if data.get('component_type') == 'iterator'
        ]
        
        # Process each iterator recursively
        for iterator_comp in iterator_components:
            self._mark_iterator_boundary_recursive(dag, iterator_comp, iterator_comp)
    
    def _mark_iterator_boundary_recursive(self, dag: nx.DiGraph, iterator_comp: str, boundary_name: str) -> None:
        """Recursively mark iterator boundary starting from iterator's data outputs."""
        # Gather all data outputs of this iterator
        data_outputs = [
            target
            for _, target, edge_data in dag.out_edges(iterator_comp, data=True)
            if edge_data.get('edge_type') == 'data'
        ]
        
        # Traverse from each data output under the given boundary
        for start in data_outputs:
            self._traverse_iterator_boundary(dag, start, boundary_name, set())


    def _traverse_iterator_boundary(
        self,
        dag: nx.DiGraph,
        current_comp: str,
        boundary_name: str,
        visited: Set[str]
    ) -> None:
        """Traverse downstream from component, marking iterator boundary."""
        # 1) Avoid cycles
        if current_comp in visited:
            return
        visited.add(current_comp)

        # 2) Check for conflicting boundary (validated elsewhere)
        existing = dag.nodes[current_comp].get('iterator_boundary', "")
        if existing and existing != boundary_name:
            pass  # will be caught in validation

        # 3) Nested iterator case
        if dag.nodes[current_comp].get('component_type') == 'iterator':
            # a) Tag this iterator under the *outer* boundary
            dag.nodes[current_comp]['iterator_boundary'] = boundary_name

            # b) Rescue edges off *this* iterator still belong to the outer boundary
            for _, rescue_target, rescue_data in dag.out_edges(current_comp, data=True):
                trig = rescue_data.get('trigger', "")
                if rescue_data.get('edge_type') == 'control' and \
                trig in ('ok', 'error', 'subjob_ok', 'subjob_error'):
                    self._traverse_iterator_boundary(
                        dag,
                        rescue_target,
                        boundary_name,
                        visited
                    )

            # c) Now start the *inner* boundary for this iterator
            self._mark_iterator_boundary_recursive(
                dag,
                current_comp,
                current_comp
            )
            return

        # 4) Normal component: tag under current boundary
        dag.nodes[current_comp]['iterator_boundary'] = boundary_name

        # 5) Recurse on outgoing edges, skipping only 
        #    - ok/error on iterators
        #    - subjob_ok/error on subjob-starts
        is_iterator     = dag.nodes[current_comp].get('component_type') == 'iterator'
        is_subjob_start = dag.nodes[current_comp].get('is_subjob_start', False)

        for _, target, edge_data in dag.out_edges(current_comp, data=True):
            etype   = edge_data.get('edge_type')
            trigger = edge_data.get('trigger', "")

            # skip ok/error only if we're on an iterator node
            if etype == 'control' and trigger in ('ok', 'error') and is_iterator:
                continue

            # skip subjob_ok/error only if we're on the subjob’s first component
            if etype == 'control' and trigger in ('subjob_ok', 'subjob_error') and is_subjob_start:
                continue

            # otherwise keep going under the same boundary
            self._traverse_iterator_boundary(
                dag,
                target,
                boundary_name,
                visited
            )
    
    def _validate_global_references(self, dag: nx.DiGraph) -> List[ValidationError]:
        """Validate global variable references in component configurations."""
        errors = []
        
        for node, node_data in dag.nodes(data=True):
            config = node_data.get('config', {})
            global_refs = self._extract_global_references_recursive(config)
            
            for comp_name, global_var in global_refs:
                if comp_name not in dag.nodes():
                    errors.append(ValidationError(
                        code="INVALID_GLOBAL_REFERENCE",
                        message=f"Component '{node}' references non-existent component '{comp_name}' in global variable '{comp_name}__{global_var}'",
                        component=node
                    ))
                else:
                    # Check if the referenced component declares this global variable
                    target_component_data = dag.nodes[comp_name]
                    output_globals = target_component_data.get('registry_metadata', {}).get('output_globals', [])
                    
                    if global_var not in output_globals:
                        errors.append(ValidationError(
                            code="INVALID_GLOBAL_REFERENCE",
                            message=f"Component '{node}' references global variable '{global_var}' from component '{comp_name}', but '{comp_name}' does not declare this global variable. Available globals: {output_globals}",
                            component=node
                        ))
        
        return errors
    
    def _extract_global_references_recursive(self, obj: Any, refs: List[Tuple[str, str]] = None) -> List[Tuple[str, str]]:
        """Recursively extract global variable references from nested structures."""
        if refs is None:
            refs = []
        
        if isinstance(obj, str):
            matches = re.findall(GLOBAL_VAR_PATTERN, obj)
            refs.extend(matches)
        elif isinstance(obj, dict):
            for value in obj.values():
                self._extract_global_references_recursive(value, refs)
        elif isinstance(obj, list):
            for item in obj:
                self._extract_global_references_recursive(item, refs)
        
        return refs
    
    def _validate_iterator_components(self, dag: nx.DiGraph) -> List[ValidationError]:
        """Validate iterator component structure and configuration."""
        errors = []
        
        iterator_components = [
            node for node, data in dag.nodes(data=True)
            if data.get('component_type') == 'iterator'
        ]
        
        for iterator_comp in iterator_components:
            # Validate iterator has data input
            has_data_input = any(
                edge_data.get('edge_type') == 'data'
                for _, _, edge_data in dag.in_edges(iterator_comp, data=True)
            )
            
            if not has_data_input:
                errors.append(ValidationError(
                    code="INVALID_ITERATOR_STRUCTURE",
                    message=f"Iterator component '{iterator_comp}' must have at least one data input",
                    component=iterator_comp
                ))
            
            # Validate iterator has data output
            has_data_output = any(
                edge_data.get('edge_type') == 'data'
                for _, _, edge_data in dag.out_edges(iterator_comp, data=True)
            )
            
            if not has_data_output:
                errors.append(ValidationError(
                    code="INVALID_ITERATOR_STRUCTURE",
                    message=f"Iterator component '{iterator_comp}' must have at least one data output",
                    component=iterator_comp
                ))
        
        return errors
    
    def _validate_component_dependencies(self, dag: nx.DiGraph) -> List[ValidationError]:
        """Validate component dependencies exist in DAG."""
        errors = []
        
        for node, node_data in dag.nodes(data=True):
            dependencies = node_data.get('dependencies', [])
            
            for dep in dependencies:
                if dep not in dag.nodes():
                    errors.append(ValidationError(
                        code="MISSING_DEPENDENCY",
                        message=f"Component '{node}' requires dependency '{dep}' which is not present in DAG",
                        component=node
                    ))
        
        return errors
    
    def _validate_edge_connectivity(self, dag: nx.DiGraph) -> List[ValidationError]:
        """Validate that there are no duplicate edges and no mixed data+control
        between the same pair of components."""
        errors = []
        
        # 1) Build signatures to catch exact duplicates
        edge_signatures = defaultdict(int)
        for source, target, edge_data in dag.edges(data=True):
            edge_type = edge_data.get('edge_type', 'unknown')
            if edge_type == 'control':
                trigger   = edge_data.get('trigger', 'unknown')
                signature = (source, target, edge_type, trigger)
            else:
                src_port  = edge_data.get('source_port', 'unknown')
                tgt_port  = edge_data.get('target_port', 'unknown')
                signature = (source, target, edge_type, src_port, tgt_port)
            edge_signatures[signature] += 1
        
        # 2) Report duplicates if the exact same edge appears more than once
        for signature, count in edge_signatures.items():
            if count > 1:
                src, tgt, etype = signature[:3]
                errors.append(ValidationError(
                    code="DUPLICATE_EDGE",
                    message=(
                        f"Duplicate {etype} edge between '{src}' and '{tgt}' "
                        f"({count} instances)"
                    )
                ))
        
        # 3) Gather types per component-pair
        component_pairs = defaultdict(set)
        for source, target, edge_data in dag.edges(data=True):
            component_pairs[(source, target)].add(
                edge_data.get('edge_type', 'unknown')
            )
        
        # 4) Flag any pair that has BOTH data and control
        for (src, tgt), types in component_pairs.items():
            if 'data' in types and 'control' in types:
                errors.append(ValidationError(
                    code="MIXED_EDGE_TYPES",
                    message=(
                        f"Components '{src}' and '{tgt}' cannot be connected "
                        "by both data and control edges."
                    )
                ))
        
        return errors
    
    def _validate_performance_characteristics(self, dag: nx.DiGraph) -> List[ValidationWarning]:
        """Validate DAG performance characteristics and generate warnings."""
        warnings = []
        
        # Check DAG complexity
        if len(dag.nodes()) > self.HIGH_COMPLEXITY_THRESHOLD:
            warnings.append(ValidationWarning(
                code="HIGH_COMPLEXITY",
                message=f"DAG has {len(dag.nodes())} components (>{self.HIGH_COMPLEXITY_THRESHOLD}), may impact performance",
                recommendation="Consider breaking into smaller jobs or using parallelization"
            ))
        
        # Check for high fan-out components
        for node in dag.nodes():
            out_degree = dag.out_degree(node)
            if out_degree > self.HIGH_FAN_OUT_THRESHOLD:
                warnings.append(ValidationWarning(
                    code="HIGH_FAN_OUT",
                    message=f"Component '{node}' has {out_degree} output connections (>{self.HIGH_FAN_OUT_THRESHOLD}), may impact performance",
                    component=node,
                    recommendation="Consider using iterator pattern or reducing fan-out"
                ))
        
        # Check path depth - only if DAG is acyclic
        try:
            if nx.is_directed_acyclic_graph(dag):
                longest_path = nx.dag_longest_path_length(dag)
                if longest_path > self.MAX_PATH_DEPTH_THRESHOLD:
                    warnings.append(ValidationWarning(
                        code="DEEP_NESTING",
                        message=f"DAG has maximum path depth of {longest_path} (>{self.MAX_PATH_DEPTH_THRESHOLD}), may impact readability",
                        recommendation="Consider restructuring to reduce sequential dependencies"
                    ))
        except nx.NetworkXError:
            # Skip path depth check if any NetworkX error occurs
            pass
        
        return warnings