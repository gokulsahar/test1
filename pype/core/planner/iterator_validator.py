import networkx as nx
from typing import Dict, List, Tuple, Any, Set, Optional
from collections import defaultdict, deque
from pype.core.utils.constants import FOREACH_COMPONENT_TYPE


class ForEachValidationError(Exception):
    """Base class for forEach validation errors."""
    pass


class ForEachValidator:
    """Validates forEach components and marks forEach boundaries"""
    
    def __init__(self):
        """Initialize forEach validator."""
        pass
    
    def analyze_forEach_boundaries(self, dag: nx.DiGraph) -> List[str]:
        """Analyze and mark forEach boundaries with hierarchy metadata."""
        errors = []
        
        # Step 1: Mark forEach boundaries and hierarchy
        boundary_errors = self._mark_forEach_boundaries(dag)
        errors.extend(boundary_errors)
        
        # Step 2: Validate forEach components
        validation_errors = self._validate_forEach_components(dag)
        errors.extend(validation_errors)
        
        return errors
    
    def _mark_forEach_boundaries(self, dag: nx.DiGraph) -> List[str]:
        """Mark components with their forEach boundary using recursive algorithm."""
        # Initialize all components with empty forEach boundary
        for node in dag.nodes():
            dag.nodes[node]['forEach_boundary'] = ""
            dag.nodes[node]['forEach_depth'] = 0
            dag.nodes[node]['outer_forEach'] = ""
        
        # Find all forEach components
        forEach_components = [
            node for node, data in dag.nodes(data=True)
            if data.get('component_type') == FOREACH_COMPONENT_TYPE
        ]
            
        errors: List[str] = []
        # Process each forEach recursively
        for forEach_comp in forEach_components:
            errors.extend(
                self._mark_forEach_boundary_recursive(
                    dag, forEach_comp, forEach_comp
                )
            )
        return errors
    
    def _mark_forEach_boundary_recursive(self, dag: nx.DiGraph, forEach_comp: str, boundary_name: str) -> List[str]:
        """Recursively mark forEach boundary starting from forEach's data outputs."""
        errors: List[str] = []
        # Gather all data outputs of this forEach
        data_outputs = [
            target
            for _, target, edge_data in dag.out_edges(forEach_comp, data=True)
            if edge_data.get('edge_type') == 'data'
        ]
        
        # Traverse from each data output under the given boundary
        for start in data_outputs:
            errors.extend(
                self._traverse_forEach_boundary(
                    dag, start, boundary_name, set()  # fresh visited per branch
                )
            )
        return errors

    def _traverse_forEach_boundary(
        self,
        dag: nx.DiGraph,
        current_comp: str,
        boundary_name: str,
        visited: Set[str]
    ) -> List[str]:
        """Traverse downstream from component, marking forEach boundary."""
        errors = []
        
        # Avoid cycles
        if current_comp in visited:
            return errors
        visited.add(current_comp)

        # Check for conflicting boundary
        existing = dag.nodes[current_comp].get('forEach_boundary', "")
        if existing and existing != boundary_name:
            errors.append(
                f"Component '{current_comp}' assigned to multiple forEach boundaries: '{existing}' and '{boundary_name}'"
            )
            return errors  # Don't continue traversal with conflicting boundary

        # Nested forEach case
        if dag.nodes[current_comp].get('component_type') == FOREACH_COMPONENT_TYPE:
            # Tag this forEach under the outer boundary
            dag.nodes[current_comp]['forEach_boundary'] = boundary_name
            
            # Set hierarchy metadata
            outer_depth = dag.nodes.get(boundary_name, {}).get('forEach_depth', 0)
            dag.nodes[current_comp]['forEach_depth'] = outer_depth + 1
            dag.nodes[current_comp]['outer_forEach'] = boundary_name

            # Rescue edges off this forEach still belong to the outer boundary
            for _, rescue_target, rescue_data in dag.out_edges(current_comp, data=True):
                trig = rescue_data.get('trigger', "")
                if rescue_data.get('edge_type') == 'control' and \
                trig in ('ok', 'error', 'subjob_ok', 'subjob_error'):
                    rescue_errors = self._traverse_forEach_boundary(
                        dag,
                        rescue_target,
                        boundary_name,
                        visited
                    )
                    errors.extend(rescue_errors)

            # Now start the inner boundary for this forEach
            inner_errors = self._mark_forEach_boundary_recursive(
                dag,
                current_comp,
                current_comp
            )
            errors.extend(inner_errors)
            return errors

        # Normal component: tag under current boundary
        dag.nodes[current_comp]['forEach_boundary'] = boundary_name
        
        # Set hierarchy metadata based on boundary forEach
        if boundary_name in dag.nodes:
            boundary_depth = dag.nodes[boundary_name].get('forEach_depth', 0)
            dag.nodes[current_comp]['forEach_depth'] = boundary_depth
            dag.nodes[current_comp]['outer_forEach'] = dag.nodes[boundary_name].get('outer_forEach', "")

        # Recurse on outgoing edges
        is_forEach = dag.nodes[current_comp].get('component_type') == FOREACH_COMPONENT_TYPE
        is_subjob_start = dag.nodes[current_comp].get('is_subjob_start', False)

        for _, target, edge_data in dag.out_edges(current_comp, data=True):
            etype = edge_data.get('edge_type')
            trigger = edge_data.get('trigger', "")

            # Skip ok/error only if we're on a forEach node
            if etype == 'control' and trigger in ('ok', 'error') and is_forEach:
                continue

            # Skip subjob_ok/error only if we're on the subjob's first component
            if etype == 'control' and trigger in ('subjob_ok', 'subjob_error') and is_subjob_start:
                continue

            # Otherwise keep going under the same boundary
            traverse_errors = self._traverse_forEach_boundary(
                dag,
                target,
                boundary_name,
                visited
            )
            errors.extend(traverse_errors)
        
        return errors
    
    def _validate_forEach_components(self, dag: nx.DiGraph) -> List[str]:
        """Validate forEach component structure and forbidden control edges."""
        errors = []
        
        forEach_components = [
            node for node, data in dag.nodes(data=True)
            if data.get('component_type') == FOREACH_COMPONENT_TYPE
        ]
        
        for forEach_comp in forEach_components:
            # Validate forEach has data input
            has_data_input = any(
                edge_data.get('edge_type') == 'data'
                for _, _, edge_data in dag.in_edges(forEach_comp, data=True)
            )
            
            if not has_data_input:
                errors.append(
                    f"ForEach component '{forEach_comp}' must have at least one data input"
                )
            
            # Validate forEach has data output
            has_data_output = any(
                edge_data.get('edge_type') == 'data'
                for _, _, edge_data in dag.out_edges(forEach_comp, data=True)
            )
            
            if not has_data_output:
                errors.append(
                    f"ForEach component '{forEach_comp}' must have at least one data output"
                )
        
        
        errors.extend(self._validate_forEach_forbidden_triggers(dag))
        
        return errors
    
    
    def _validate_forEach_forbidden_triggers(self, dag: nx.DiGraph) -> List[str]:
        """Validate that forEach scope components cannot emit subjob_ok/subjob_error."""
        errors = []
        
        for node, node_data in dag.nodes(data=True):
            forEach_boundary = node_data.get('forEach_boundary', '')
            
            # Skip components not in forEach scope
            if not forEach_boundary:
                continue
                
            # Skip the forEach component itself (it can emit ok)
            if node_data.get('component_type') == FOREACH_COMPONENT_TYPE:
                continue
            
            # Check outgoing control edges
            for _, target, edge_data in dag.out_edges(node, data=True):
                if edge_data.get('edge_type') == 'control':
                    trigger = edge_data.get('trigger', '')
                    if trigger in ['subjob_ok', 'subjob_error']:
                        errors.append(
                            f"Component '{node}' in forEach '{forEach_boundary}' scope "
                            f"cannot emit '{trigger}' edges. Use 'ok'/'error' instead."
                        )
        
        return errors