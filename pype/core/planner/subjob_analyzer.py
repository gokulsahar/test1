import networkx as nx
from typing import Dict, List, Tuple, Any, Set, Optional
from collections import defaultdict, deque


class SubjobAnalysisError(Exception):
    """Base class for subjob analysis errors."""
    pass


class InvalidParallelStructureError(SubjobAnalysisError):
    """Raised when parallelise/synchronise structure is invalid."""
    pass


class SubjobCycleError(SubjobAnalysisError):
    """Raised when circular dependencies detected between subjobs."""
    pass


class OrphanedComponentError(SubjobAnalysisError):
    """Raised when components cannot be assigned to any subjob."""
    pass


class UnmatchedSynchroniseError(SubjobAnalysisError):
    """Raised when synchronise edge references invalid parallel branches."""
    pass


class SubjobAnalyzer:
    """Analyzes DAG to detect subjob boundaries for parallel execution and checkpointing."""
    
    def __init__(self):
        """Initialize subjob analyzer."""
        self._parallelise_map = {}  # {source_component: [target_components]}
        self._synchronise_map = {}  # {target_component: [source_components]}
        self._component_to_subjob = {}  # {component_name: subjob_id}
        
    def analyze_subjobs(self, dag: nx.DiGraph) -> Tuple[Dict[str, List[str]], Dict[str, Dict[str, Any]], List[str]]:
        """
        Analyze DAG to detect subjob boundaries and create execution plan.
        
        Args:
            dag: NetworkX DiGraph with components and control edges
            
        Returns:
            (subjob_components, subjob_metadata, validation_errors)
        """
        errors = []
        
        try:
            # Step 1: Extract parallelise and synchronise edges
            self._parallelise_map = self._find_parallelise_edges(dag)
            self._synchronise_map = self._find_synchronise_edges(dag)
            
            # Step 2: Validate parallel structure
            parallel_errors = self._validate_parallelise_synchronise_pairing(dag)
            errors.extend(parallel_errors)
            
            if errors:
                return {}, {}, errors
            
            # Step 3: Detect subjob boundaries
            self._component_to_subjob = self._detect_subjob_boundaries(dag)
            
            # Step 4: Build subjob component mapping
            subjob_components = self._group_components_by_subjob()
            
            # Step 5: Validate subjob structure
            structure_errors = self._validate_subjob_structure(dag, subjob_components)
            errors.extend(structure_errors)
            
            if errors:
                return {}, {}, errors
            
            # Step 6: Generate execution metadata
            subjob_metadata = self._generate_subjob_metadata(dag, subjob_components)
            
        except Exception as e:
            errors.append(f"Subjob analysis failed: {str(e)}")
            return {}, {}, errors
        
        return subjob_components, subjob_metadata, errors
    
    def _find_parallelise_edges(self, dag: nx.DiGraph) -> Dict[str, List[str]]:
        """Find all parallelise control edges in DAG."""
        parallelise_map = {}
        
        for source, target, edge_data in dag.edges(data=True):
            if (edge_data.get('edge_type') == 'control' and 
                edge_data.get('trigger') == 'parallelise'):
                
                if source not in parallelise_map:
                    parallelise_map[source] = []
                parallelise_map[source].append(target)
        
        return parallelise_map
    
    def _find_synchronise_edges(self, dag: nx.DiGraph) -> Dict[str, List[str]]:
        """Find all synchronise control edges in DAG."""
        synchronise_map = defaultdict(list)
        
        for source, target, edge_data in dag.edges(data=True):
            if (edge_data.get('edge_type') == 'control' and 
                edge_data.get('trigger') == 'synchronise'):
                synchronise_map[target].append(source)
        
        return dict(synchronise_map)
    
    def _validate_parallelise_synchronise_pairing(self, dag: nx.DiGraph) -> List[str]:
        """Validate parallelise and synchronise edges are properly paired."""
        errors = []
        
        # Check parallelise has at least 2 targets
        for source, targets in self._parallelise_map.items():
            if len(targets) < 2:
                errors.append(
                    f"Parallelise from '{source}' must have at least 2 targets, "
                    f"found {len(targets)}: {targets}"
                )
        
        # Track which parallel branches are synchronized
        synchronized_branches = set()
        for sync_target, sync_sources in self._synchronise_map.items():
            synchronized_branches.update(sync_sources)
        
        # Find all components that are targets of parallelise
        all_parallel_targets = set()
        for targets in self._parallelise_map.values():
            all_parallel_targets.update(targets)
        
        # Check for orphaned synchronise (sources not from parallelise)
        for sync_target, sync_sources in self._synchronise_map.items():
            for source in sync_sources:
                if source not in all_parallel_targets:
                    errors.append(
                        f"Synchronise edge from '{source}' to '{sync_target}' "
                        f"references non-parallel branch"
                    )
        
        # Validate each parallelise has exactly one corresponding synchronise
        for parallel_source, parallel_targets in self._parallelise_map.items():
            # Find synchronise nodes that reference these parallel branches
            matching_syncs = []
            for sync_target, sync_sources in self._synchronise_map.items():
                if set(sync_sources) == set(parallel_targets):
                    matching_syncs.append(sync_target)
            
            if len(matching_syncs) == 0:
                errors.append(
                    f"Parallelise from '{parallel_source}' with targets {parallel_targets} "
                    f"has no matching synchronise edge"
                )
            elif len(matching_syncs) > 1:
                errors.append(
                    f"Parallelise from '{parallel_source}' has multiple synchronise edges: "
                    f"{matching_syncs}"
                )
        
        return errors
    
    def _detect_subjob_boundaries(self, dag: nx.DiGraph) -> Dict[str, str]:
        """Detect subjob boundaries using parallelise/synchronise edge analysis."""
        component_to_subjob = {}
        
        # Start with all components in main subjob
        for node in dag.nodes():
            component_to_subjob[node] = "main"
        
        # Process each parallelise edge
        for parallel_source, parallel_targets in self._parallelise_map.items():
            # Each target starts a new subjob
            for idx, target in enumerate(sorted(parallel_targets)):  # Sort for determinism
                subjob_id = f"{parallel_source}_parallel_{idx}"
                
                # Find the synchronise node for this parallel group
                sync_node = self._find_sync_node_for_parallel(parallel_targets)
                
                # Trace all components in this parallel branch
                branch_components = self._trace_subjob_components(
                    dag, target, {sync_node} if sync_node else set()
                )
                
                # Assign subjob ID to all components in branch
                for comp in branch_components:
                    component_to_subjob[comp] = subjob_id
        
        # Handle nested parallelise (if any)
        component_to_subjob = self._handle_nested_subjobs(dag, component_to_subjob)
        
        return component_to_subjob
    
    def _find_sync_node_for_parallel(self, parallel_targets: List[str]) -> Optional[str]:
        """Find the synchronise node that waits for these parallel branches."""
        for sync_target, sync_sources in self._synchronise_map.items():
            if set(sync_sources) == set(parallel_targets):
                return sync_target
        return None
    
    def _trace_subjob_components(self, dag: nx.DiGraph, start_component: str, 
                                boundary_components: Set[str]) -> Set[str]:
        """Trace all components belonging to a subjob."""
        subjob_components = set()
        queue = deque([start_component])
        
        while queue:
            current = queue.popleft()
            
            if current in subjob_components or current in boundary_components:
                continue
                
            subjob_components.add(current)
            
            # Follow all outgoing edges (both data and control)
            for _, target, edge_data in dag.out_edges(current, data=True):
                if target not in subjob_components and target not in boundary_components:
                    # Skip if this is a parallelise edge (starts new subjob)
                    if not (edge_data.get('edge_type') == 'control' and 
                           edge_data.get('trigger') == 'parallelise'):
                        queue.append(target)
        
        return subjob_components
    
    def _handle_nested_subjobs(self, dag: nx.DiGraph, 
                              component_to_subjob: Dict[str, str]) -> Dict[str, str]:
        """Handle nested parallelise patterns within existing subjobs."""
        # For now, we don't have nested parallelise in the current structure
        # This is a placeholder for future enhancement
        return component_to_subjob
    
    def _group_components_by_subjob(self) -> Dict[str, List[str]]:
        """Group components by their subjob ID."""
        subjob_components = defaultdict(list)
        
        for component, subjob_id in self._component_to_subjob.items():
            subjob_components[subjob_id].append(component)
        
        # Sort component lists for determinism
        for subjob_id in subjob_components:
            subjob_components[subjob_id].sort()
        
        return dict(subjob_components)
    
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
        
        # Validate no data edges cross subjob boundaries
        for source, target, edge_data in dag.edges(data=True):
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
            for start in startable_components:
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
        """Generate comprehensive metadata for each subjob."""
        subjob_metadata = {}
        
        # Build subjob dependency graph
        subjob_deps = self._build_subjob_dependency_graph(dag, subjob_components)
        
        # Calculate execution order
        try:
            execution_order = list(nx.topological_sort(subjob_deps))
        except nx.NetworkXError:
            # Cycle detected - this should have been caught in validation
            execution_order = list(subjob_components.keys())
        
        # Identify parallel groups
        parallel_groups = self._identify_parallel_subjob_groups(execution_order, subjob_deps)
        
        # Generate metadata for each subjob
        for idx, subjob_id in enumerate(execution_order):
            components = subjob_components[subjob_id]
            
            # Find which parallel group this subjob belongs to
            parallel_group = 0
            for group_idx, group in enumerate(parallel_groups):
                if subjob_id in group:
                    parallel_group = group_idx
                    break
            
            # Identify parent and child subjobs
            parent_subjob = None
            child_subjobs = []
            
            if '_parallel_' in subjob_id:
                # Extract parent from subjob_id
                parent_subjob = 'main'
                
            # Find children by checking parallelise edges
            for comp in components:
                if comp in self._parallelise_map:
                    targets = self._parallelise_map[comp]
                    for idx, target in enumerate(sorted(targets)):
                        child_subjobs.append(f"{comp}_parallel_{idx}")
            
            # Identify startable components within subjob
            startable_in_subjob = [
                comp for comp in components
                if dag.nodes[comp].get('startable', False)
            ]
            
            subjob_metadata[subjob_id] = {
                'components': components,
                'execution_order': idx,
                'parallel_group': parallel_group,
                'parent_subjob': parent_subjob,
                'child_subjobs': child_subjobs,
                'checkpoint_boundary': True,  # All subjob ends are checkpoints
                'resume_point': True,
                'dependencies': list(subjob_deps.predecessors(subjob_id)),
                'startable_components': startable_in_subjob
            }
        
        return subjob_metadata
    
    def _build_subjob_dependency_graph(self, dag: nx.DiGraph, 
                                      subjob_components: Dict[str, List[str]]) -> nx.DiGraph:
        """Build dependency graph between subjobs."""
        subjob_graph = nx.DiGraph()
        
        # Add all subjobs as nodes
        subjob_graph.add_nodes_from(subjob_components.keys())
        
        # Add edges based on control flow between subjobs
        for source, target, edge_data in dag.edges(data=True):
            if edge_data.get('edge_type') == 'control':
                source_subjob = self._component_to_subjob.get(source)
                target_subjob = self._component_to_subjob.get(target)
                
                if source_subjob and target_subjob and source_subjob != target_subjob:
                    subjob_graph.add_edge(source_subjob, target_subjob)
        
        return subjob_graph
    
    def _identify_parallel_subjob_groups(self, execution_order: List[str], 
                                       subjob_deps: nx.DiGraph) -> List[List[str]]:
        """Group subjobs that can execute in parallel."""
        parallel_groups = []
        assigned = set()
        
        for subjob in execution_order:
            if subjob in assigned:
                continue
                
            # Find all subjobs at the same level (no dependencies between them)
            group = [subjob]
            assigned.add(subjob)
            
            for other in execution_order:
                if other not in assigned:
                    # Check if they can run in parallel (no path between them)
                    if (not nx.has_path(subjob_deps, subjob, other) and 
                        not nx.has_path(subjob_deps, other, subjob)):
                        group.append(other)
                        assigned.add(other)
            
            parallel_groups.append(group)
        
        return parallel_groups