import re
import json
import networkx as nx
from typing import Dict, List, Tuple, Any, Set, Optional
from dataclasses import dataclass
from collections import defaultdict, deque
from pype.core.registry.component_registry import ComponentRegistry
from pype.core.utils.constants import ( GLOBAL_VAR_PATTERN, GLOBAL_VAR_DELIMITER )


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
        Perform basic structural validation of DAG (cycles, isolation, reachability).
        
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
            
            # Check for isolated components
            isolated_errors = self._validate_no_isolated_components(dag)
            errors.extend(isolated_errors)
            
            # Non-critical validations
            errors.extend(self._validate_global_references(dag))
            
            # Basic performance warnings
            warnings.extend(self._validate_performance_characteristics(dag))
            
        except (CycleDetectedError, UnreachableComponentError) as e:
            # Convert critical structural errors to ValidationError
            errors.append(ValidationError(
                code="CRITICAL_STRUCTURE_ERROR",
                message=str(e)
            ))
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
        
        # Check if we have any startable components
        if not startable_components:
            errors.append(ValidationError(
                code="NO_STARTABLE_COMPONENTS",
                message="No components can start execution (must have startable=True and no incoming edges)"
            ))
            return errors  # Can't analyze reachability without startable components
        
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
            # Check registry metadata for startable flag
            is_registry_startable = node_data.get('startable', False)

            # Check if the node has no incoming edges at all
            has_any_incoming_edges = dag.in_degree(node) > 0

            # Add to result only if it's marked startable and has no dependencies
            if is_registry_startable and not has_any_incoming_edges:
                startable_components.append(node)

        return startable_components

    def _validate_no_isolated_components(self, dag: nx.DiGraph) -> List[ValidationError]:
        """Validate that no components are completely isolated."""
        errors = []
        isolated_components = []
        
        for node in dag.nodes():
            if dag.in_degree(node) == 0 and dag.out_degree(node) == 0:
                isolated_components.append(node)
        
        if isolated_components:
            errors.append(ValidationError(
                code="ISOLATED_COMPONENTS",
                message=f"Isolated components found (no incoming or outgoing connections): {isolated_components}"
            ))
        
        return errors
    
    
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
                        message=f"Component '{node}' references non-existent component '{comp_name}' in global variable '{comp_name}{GLOBAL_VAR_DELIMITER}{global_var}'",
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
    
    
    def _validate_performance_characteristics(self, dag: nx.DiGraph) -> List[ValidationWarning]:
        """Generate basic performance warnings for DAG characteristics."""
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
                    recommendation="Consider using forEach pattern or reducing fan-out"
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