import networkx as nx
from typing import Dict, List, Tuple, Any, Set, Optional
from collections import defaultdict, deque
from pype.core.utils.constants import FOREACH_COMPONENT_TYPE


class SubjobAnalysisError(Exception):
    """Base class for subjob analysis errors."""
    pass


class InvalidSubjobStructureError(SubjobAnalysisError):
    """Raised when subjob structure is invalid."""
    pass


class OrphanedComponentError(SubjobAnalysisError):
    """Raised when components cannot be assigned to any subjob."""
    pass


class InvalidSubjobControlEdgeError(SubjobAnalysisError):
    """Raised when subjob_ok/subjob_error edges are on non-start components."""
    pass


class SubjobAnalyzer:
    """Analyzes DAG to detect subjob boundaries"""
    
    def __init__(self):
        """Initialize subjob analyzer."""
        self._component_to_subjob = {}  # {component_name: subjob_id}
        self._subjob_start_components = {}  # {subjob_id: [start_components]}
        
    def analyze_subjobs(self, dag: nx.DiGraph) -> Tuple[Dict[str, List[str]], Dict[str, Dict[str, Any]], List[str]]:
        """
        Analyze DAG to detect subjob boundaries using simplified model.
        
        Rule: Control edges create subjob boundaries.
        Components connected only by data edges belong to same subjob.
        
        Args:
            dag: NetworkX DiGraph with components and control edges
            
        Returns:
            (subjob_components, subjob_metadata, validation_errors)
        """
        errors = []
        
        try:
            # Step 1: Detect subjob boundaries using simplified rule
            self._component_to_subjob = self._detect_subjob_boundaries(dag)
            
            # Step 2: Build subjob component mapping
            subjob_components = self._group_components_by_subjob()
            
            # Step 3: Identify subjob start components and add flags
            self._subjob_start_components = self._identify_subjob_start_components(dag, subjob_components)
            self._add_subjob_start_flags(dag)
            
            # Step 4: Validate subjob control edges
            subjob_control_errors = self._validate_subjob_control_edges(dag)
            errors.extend(subjob_control_errors)
            
            # Step 5: Validate subjob structure
            structure_errors = self._validate_subjob_structure(dag, subjob_components)
            errors.extend(structure_errors)
            
            if errors:
                return {}, {}, errors
            
            # Step 6: Generate execution metadata with execution waves
            subjob_metadata = self._generate_subjob_metadata(dag, subjob_components)
            
        except Exception as e:
            errors.append(f"Subjob analysis failed: {str(e)}")
            return {}, {}, errors
        
        return subjob_components, subjob_metadata, errors
    
    def _detect_subjob_boundaries(self, dag: nx.DiGraph) -> Dict[str, str]:
        """
        Detect subjob boundaries using forEach dominance rule: 
        Control edges create boundaries, but forEach components force all downstream into same subjob.
        """
        component_to_subjob = {}
        subjob_counter = 0
        visited = set()
        
        # Find all components
        all_components = set(dag.nodes())
        
        # Find forEach components for dominance handling
        forEach_components = self._find_forEach_components(dag)
        
        # Process each unvisited component
        for component in sorted(all_components):
            if component in visited:
                continue
                
            # Find all components connected only by data edges (same subjob)
            subjob_components = self._find_data_connected_group_with_forEach_dominance(
                dag, component, visited, forEach_components
            )
            
            # Assign subjob ID
            subjob_id = f"subjob_{subjob_counter}"
            for comp in subjob_components:
                # Check for overlap (should never happen but validate)
                if comp in component_to_subjob:
                    raise SubjobAnalysisError(
                        f"Component '{comp}' assigned to multiple subjobs: "
                        f"'{component_to_subjob[comp]}' and '{subjob_id}'"
                    )
                component_to_subjob[comp] = subjob_id
                visited.add(comp)
            
            subjob_counter += 1
        
        return component_to_subjob
    
    
    def _find_forEach_components(self, dag: nx.DiGraph) -> Set[str]:
        """Find all forEach components in the DAG."""
        return {
            node for node, data in dag.nodes(data=True)
            if data.get('component_type') == FOREACH_COMPONENT_TYPE
        }
    
    def _find_data_connected_group_with_forEach_dominance(
    self, 
    dag: nx.DiGraph, 
    start_component: str, 
    global_visited: Set[str],
    forEach_components: Set[str]
) -> Set[str]:
        """
        Find all components connected to start_component considering forEach dominance.
        forEach components force all downstream components into same subjob.
        """
        group = set()
        queue = deque([start_component])
        local_visited = set()
        
        while queue:
            current = queue.popleft()
            
            if current in local_visited or current in global_visited:
                continue
                
            local_visited.add(current)
            group.add(current)
            
            # Follow data edges (both directions) - standard behavior
            for source, target, edge_data in dag.edges(current, data=True):
                if edge_data.get('edge_type') == 'data' and target not in local_visited:
                    queue.append(target)
                    
            for source, target, edge_data in dag.in_edges(current, data=True):
                if edge_data.get('edge_type') == 'data' and source not in local_visited:
                    queue.append(source)
            
            # NEW: forEach dominance - if current is forEach, include ALL downstream components
            if current in forEach_components:
                downstream_components = nx.descendants(dag, current)
                for downstream in downstream_components:
                    if downstream not in local_visited:
                        queue.append(downstream)
        
        return group
    
    def _group_components_by_subjob(self) -> Dict[str, List[str]]:
        """Group components by their subjob ID."""
        subjob_components = defaultdict(list)
        
        for component, subjob_id in sorted(self._component_to_subjob.items()):
            subjob_components[subjob_id].append(component)
        
        # Sort component lists for determinism
        for subjob_id in subjob_components:
            subjob_components[subjob_id].sort()
        
        return dict(subjob_components)
    
    def _identify_subjob_start_components(self, dag: nx.DiGraph, 
                                        subjob_components: Dict[str, List[str]]) -> Dict[str, List[str]]:
        """Identify the start components for each subjob"""
        subjob_start_components = {}
        
        for subjob_id, components in subjob_components.items():
            start_components = []
            
            # Find components that can start this subjob:
            # Has incoming control edges AND no incoming data edges
            for comp in components:
                has_incoming_control = any(
                    edge_data.get('edge_type') == 'control'
                    for _, _, edge_data in dag.in_edges(comp, data=True)
                )
                
                has_incoming_data = any(
                    edge_data.get('edge_type') == 'data'
                    for _, _, edge_data in dag.in_edges(comp, data=True)
                )
                
                if has_incoming_control and not has_incoming_data:
                    start_components.append(comp)
            
            # If no start components found using control edge logic,
            # look for naturally startable components (for initial subjobs)
            if not start_components:
                for comp in components:
                    is_startable = dag.nodes[comp].get('startable', False)
                    has_no_incoming_edges = dag.in_degree(comp) == 0
                    
                    if is_startable and has_no_incoming_edges:
                        start_components.append(comp)
            
            subjob_start_components[subjob_id] = sorted(start_components)
        
        return subjob_start_components
    
    def _add_subjob_start_flags(self, dag: nx.DiGraph) -> None:
        """Add is_subjob_start flag to appropriate components in the DAG."""
        for subjob_id, start_components in self._subjob_start_components.items():
            for comp in start_components:
                if comp in dag.nodes:
                    dag.nodes[comp]['is_subjob_start'] = True
                    dag.nodes[comp]['subjob_id'] = subjob_id
        
        # Ensure all other components have the flag set to False
        for node in dag.nodes():
            if 'is_subjob_start' not in dag.nodes[node]:
                dag.nodes[node]['is_subjob_start'] = False
            if 'subjob_id' not in dag.nodes[node]:
                dag.nodes[node]['subjob_id'] = self._component_to_subjob.get(node, 'unknown')
    
    def _validate_subjob_control_edges(self, dag: nx.DiGraph) -> List[str]:
        """Validate subjob_ok/subjob_error edges are only on subjob start components."""
        errors = []
        
        # Find all components that have subjob_ok or subjob_error outgoing edges
        components_with_subjob_edges = set()
        subjob_error_by_subjob = defaultdict(list)
        
        for source, target, edge_data in dag.edges(data=True):
            if (edge_data.get('edge_type') == 'control' and 
                edge_data.get('trigger') in ['subjob_ok', 'subjob_error']):
                components_with_subjob_edges.add(source)
                
                # Track subjob_error edges by subjob
                if edge_data.get('trigger') == 'subjob_error':
                    source_subjob = self._component_to_subjob.get(source, 'unknown')
                    subjob_error_by_subjob[source_subjob].append(source)
        
        # Validate each component with subjob edges
        for comp in components_with_subjob_edges:
            is_subjob_start = dag.nodes[comp].get('is_subjob_start', False)
            
            if not is_subjob_start:
                subjob_id = self._component_to_subjob.get(comp, 'unknown')
                start_components = self._subjob_start_components.get(subjob_id, [])
                
                errors.append(
                    f"Component '{comp}' has subjob_ok/subjob_error edges but is not a subjob start component. "
                    f"Only subjob start components can have these edges. "
                    f"Start components for subjob '{subjob_id}': {start_components}"
                )
        
        # Validate subjob-level constraint: only one subjob_error per subjob
        for subjob_id, error_components in subjob_error_by_subjob.items():
            if len(error_components) > 1:
                errors.append(
                    f"Subjob '{subjob_id}' has multiple subjob_error edges from components: {error_components}. "
                    f"Only one subjob_error edge allowed per subjob."
                )
        
        return errors
    
    def _validate_subjob_structure(self, dag: nx.DiGraph, 
                                  subjob_components: Dict[str, List[str]]) -> List[str]:
        """Validate subjob structure is correct and executable."""
        errors = []
        
        # Check all components assigned to exactly one subjob
        all_components = set(dag.nodes())
        assigned_components = set()
        for components in subjob_components.values():
            assigned_components.update(components)
        
        unassigned = all_components - assigned_components
        if unassigned:
            errors.append(f"Components not assigned to any subjob: {sorted(unassigned)}")
        
        # Validate that components with ONLY incoming control edges have startable=True
        for node in dag.nodes():
            has_incoming_control = any(
                edge_data.get('edge_type') == 'control'
                for _, _, edge_data in dag.in_edges(node, data=True)
            )
            
            has_incoming_data = any(
                edge_data.get('edge_type') == 'data'
                for _, _, edge_data in dag.in_edges(node, data=True)
            )
            
            is_startable = dag.nodes[node].get('startable', False)
            
            # Only validate components that have control edges BUT NO data edges
            if has_incoming_control and not has_incoming_data and not is_startable:
                errors.append(
                    f"Component '{node}' has only incoming control edges but startable=False. "
                    f"Components that start subjobs via control edges must have startable=True."
                )
        
        # Validate no data edges cross subjob boundaries
        for source, target, edge_data in sorted(dag.edges(data=True), key=lambda e: (e[0], e[1])):
            if edge_data.get('edge_type') == 'data':
                source_subjob = self._component_to_subjob.get(source)
                target_subjob = self._component_to_subjob.get(target)
                
                if source_subjob != target_subjob:
                    errors.append(
                        f"Data edge crosses subjob boundary: "
                        f"{source} ({source_subjob}) -> {target} ({target_subjob})"
                    )
        
        # Check for orphaned components (unreachable from startable)
        startable_components = [
            node for node, data in dag.nodes(data=True) 
            if data.get('startable', False)
        ]
        
        if not startable_components:
            errors.append("No startable components found in DAG")
        else:
            reachable = set()
            for start in sorted(startable_components):
                reachable.update(nx.descendants(dag, start))
                reachable.add(start)
            
            unreachable = all_components - reachable
            if unreachable:
                errors.append(
                    f"Components unreachable from any startable node: {sorted(unreachable)}"
                )
        
        return errors
    
    def _generate_subjob_metadata(self, dag: nx.DiGraph, 
                                 subjob_components: Dict[str, List[str]]) -> Dict[str, Dict[str, Any]]:
        """Generate metadata for each subjob with execution waves."""
        subjob_metadata = {}
        
        # Build subjob dependency graph for execution planning
        subjob_deps = self._build_subjob_dependency_graph(dag, subjob_components)
        
        # Calculate execution order using topological sort
        try:
            execution_order = list(nx.topological_sort(subjob_deps))
        except nx.NetworkXError:
            # Cycle detected - this should have been caught in validation
            execution_order = sorted(subjob_components.keys())
        
        # Identify execution waves for asyncio parallelism
        execution_waves = self._identify_execution_waves(execution_order, subjob_deps)
        
        # Generate metadata for each subjob
        for idx, subjob_id in enumerate(execution_order):
            components = subjob_components[subjob_id]
            
            # NEW: Topologically sort components within subjob
            subjob_subgraph = dag.subgraph(components)
            try:
                component_execution_order = list(nx.topological_sort(subjob_subgraph))
            except nx.NetworkXError:
                # If cycle exists (shouldn't happen), fallback to original order
                component_execution_order = components
            
            # NEW: Identify which components can run in parallel within subjob
            component_execution_waves = self._identify_component_execution_waves(
                component_execution_order, subjob_subgraph
            )
            
            # Find which execution wave this subjob belongs to
            execution_wave = 0
            for wave_idx, wave in enumerate(execution_waves):
                if subjob_id in wave:
                    execution_wave = wave_idx
                    break
            
            # Get subjob start components
            subjob_start_components = self._subjob_start_components.get(subjob_id, [])
            
            # Identify startable components within subjob
            startable_in_subjob = [
                comp for comp in components
                if dag.nodes[comp].get('startable', False)
            ]
            
            subjob_metadata[subjob_id] = {
                'components': components,
                'component_execution_order': component_execution_order,  # NEW
                'component_execution_waves': component_execution_waves,   # NEW
                'execution_order': idx,
                'execution_wave': execution_wave,
                'is_checkpoint_boundary': True,  # All subjob ends are checkpoints
                'resume_point': True,
                'dependencies': sorted(list(subjob_deps.predecessors(subjob_id))),
                'startable_components': sorted(startable_in_subjob),
                'subjob_start_components': sorted(subjob_start_components)
            }
        
        # Add execution waves to metadata
        for subjob_id in subjob_metadata:
            subjob_metadata[subjob_id]['execution_waves'] = execution_waves
        
        return subjob_metadata

    def _identify_component_execution_waves(self, component_order: List[str], 
                                        subjob_graph: nx.DiGraph) -> List[List[str]]:
        """
        Group components into execution waves for parallel execution within subjob.
        Components in the same wave have no dependencies on each other.
        """
        execution_waves = []
        remaining_components = set(component_order)
        processed_components = set()
        
        while remaining_components:
            # Find components that have no unprocessed dependencies
            current_wave = []
            for component in component_order:
                if component not in remaining_components:
                    continue
                    
                # Check if all predecessors are already processed
                predecessors = set(subjob_graph.predecessors(component))
                if predecessors.issubset(processed_components):
                    current_wave.append(component)
            
            if not current_wave:
                # This shouldn't happen with valid DAG, but handle gracefully
                # Just take the first remaining component
                current_wave = [next(iter(remaining_components))]
            
            execution_waves.append(current_wave)
            processed_components.update(current_wave)
            remaining_components -= set(current_wave)
        
        return execution_waves
    
    def _build_subjob_dependency_graph(self, dag: nx.DiGraph, 
                                      subjob_components: Dict[str, List[str]]) -> nx.DiGraph:
        """Build dependency graph between subjobs based on control edges."""
        subjob_graph = nx.DiGraph()
        
        # Add all subjobs as nodes
        subjob_graph.add_nodes_from(subjob_components.keys())
        
        # Add edges based on control flow between subjobs
        for source, target, edge_data in sorted(dag.edges(data=True), key=lambda e: (e[0], e[1])):
            if edge_data.get('edge_type') == 'control':
                source_subjob = self._component_to_subjob.get(source)
                target_subjob = self._component_to_subjob.get(target)
                
                if source_subjob and target_subjob and source_subjob != target_subjob:
                    # Add edge with trigger information for execution planning
                    trigger = edge_data.get('trigger', 'unknown')
                    if subjob_graph.has_edge(source_subjob, target_subjob):
                        # Multiple control edges between same subjobs - track all triggers
                        existing_triggers = subjob_graph[source_subjob][target_subjob].get('triggers', [])
                        existing_triggers.append(trigger)
                        subjob_graph[source_subjob][target_subjob]['triggers'] = existing_triggers
                    else:
                        subjob_graph.add_edge(source_subjob, target_subjob, triggers=[trigger])
        
        return subjob_graph
    
    def _identify_execution_waves(self, execution_order: List[str], 
                                 subjob_deps: nx.DiGraph) -> List[List[str]]:
        """Group subjobs into execution waves for asyncio parallelism."""
        execution_waves = []
        remaining_subjobs = set(execution_order)
        
        while remaining_subjobs:
            # Find subjobs that have no unprocessed dependencies
            current_wave = []
            for subjob in execution_order:
                if subjob not in remaining_subjobs:
                    continue
                    
                # Check if all dependencies are already processed
                dependencies = set(subjob_deps.predecessors(subjob))
                if dependencies.issubset(set(execution_order) - remaining_subjobs):
                    current_wave.append(subjob)
            
            if not current_wave:
                # This shouldn't happen with valid DAG, but handle gracefully
                current_wave = [list(remaining_subjobs)[0]]
            
            execution_waves.append(sorted(current_wave))
            remaining_subjobs -= set(current_wave)
        
        return execution_waves