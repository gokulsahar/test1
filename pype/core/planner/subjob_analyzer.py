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
            
            # Step 4: Handle nested parallelism
            self._component_to_subjob = self._handle_nested_subjobs(dag, self._component_to_subjob)
            
            # Step 5: Build subjob component mapping
            subjob_components = self._group_components_by_subjob()
            
            # Step 6: Validate subjob structure
            structure_errors = self._validate_subjob_structure(dag, subjob_components)
            errors.extend(structure_errors)
            
            if errors:
                return {}, {}, errors
            
            # Step 7: Generate execution metadata
            subjob_metadata = self._generate_subjob_metadata(dag, subjob_components)
            
        except Exception as e:
            errors.append(f"Subjob analysis failed: {str(e)}")
            return {}, {}, errors
        
        return subjob_components, subjob_metadata, errors
    
    def _find_parallelise_edges(self, dag: nx.DiGraph) -> Dict[str, List[str]]:
        """Find all parallelise control edges in DAG."""
        parallelise_map = {}
        
        for source, target, edge_data in sorted(dag.edges(data=True), key=lambda e: (e[0], e[1])):
            if (edge_data.get('edge_type') == 'control' and 
                edge_data.get('trigger') == 'parallelise'):
                
                if source not in parallelise_map:
                    parallelise_map[source] = []
                parallelise_map[source].append(target)
        
        return parallelise_map
    
    def _find_synchronise_edges(self, dag: nx.DiGraph) -> Dict[str, List[str]]:
        """Find all synchronise control edges in DAG."""
        synchronise_map = defaultdict(list)
        
        for source, target, edge_data in sorted(dag.edges(data=True), key=lambda e: (e[0], e[1])):
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
        """Detect subjob boundaries using flood-fill algorithm."""
        component_to_subjob = {}
        visited = set()
        
        # First, identify all subjob root components
        subjob_roots = []
        
        # Main subjob root: startable components not targeted by parallelise
        all_parallel_targets = set()
        for targets in self._parallelise_map.values():
            all_parallel_targets.update(targets)
        
        startable_components = [
            node for node, data in dag.nodes(data=True) 
            if data.get('startable', False)
        ]
        
        # Add main subjob roots (startable components not in parallel branches)
        for comp in sorted(startable_components):
            if comp not in all_parallel_targets:
                subjob_roots.append(('main', comp))
        
        # Add parallel subjob roots
        for parallel_source in sorted(self._parallelise_map.keys()):
            parallel_targets = self._parallelise_map[parallel_source]
            for idx, target in enumerate(sorted(parallel_targets)):
                subjob_id = f"{parallel_source}_parallel_{idx}"
                subjob_roots.append((subjob_id, target))
        
        # Now flood-fill from each subjob root
        for subjob_id, root_component in subjob_roots:
            if root_component not in visited:
                # Find boundary nodes for this subjob
                boundary_nodes = self._find_subjob_boundaries_for_root(
                    dag, root_component, subjob_id
                )
                
                # Flood-fill from root, stopping at boundaries
                flooded_components = self._flood_fill_subjob(
                    dag, root_component, boundary_nodes, visited
                )
                
                # Assign all flooded components to this subjob
                for comp in flooded_components:
                    component_to_subjob[comp] = subjob_id
                    visited.add(comp)
        
        # Handle any remaining unvisited components (assign to main)
        for node in sorted(dag.nodes()):
            if node not in component_to_subjob:
                component_to_subjob[node] = "main"
        
        return component_to_subjob
    
    def _find_subjob_boundaries_for_root(self, dag: nx.DiGraph, 
                                         root_component: str, 
                                         subjob_id: str) -> Set[str]:
        """Find boundary nodes for a subjob starting from root."""
        boundaries = set()
        
        # If this is a parallel subjob, find its synchronise node
        if '_parallel_' in subjob_id:
            # Extract the parallelise source from subjob_id
            parallel_source = subjob_id.split('_parallel_')[0]
            
            if parallel_source in self._parallelise_map:
                parallel_targets = self._parallelise_map[parallel_source]
                sync_node = self._find_sync_node_for_parallel(parallel_targets)
                if sync_node:
                    boundaries.add(sync_node)
        
        # Add any nodes that are targets of parallelise edges from within this subjob
        # (these would start new subjobs), but NOT the root itself
        for node in sorted(dag.nodes()):
            if node in self._parallelise_map:
                # This node starts new parallel branches
                for target in self._parallelise_map[node]:
                    if target != root_component:  # Don't add root as boundary
                        boundaries.add(target)
        
        return boundaries
    
    def _flood_fill_subjob(self, dag: nx.DiGraph, 
                           start_node: str, 
                           boundary_nodes: Set[str],
                           global_visited: Set[str]) -> Set[str]:
        """
        Flood-fill algorithm to find all components in a subjob.
        
        Args:
            dag: NetworkX DiGraph
            start_node: Starting component for flood-fill
            boundary_nodes: Nodes where flood-fill should stop
            global_visited: Set of all visited nodes across all subjobs
            
        Returns:
            Set of components belonging to this subjob
        """
        subjob_components = set()
        queue = deque([start_node])
        local_visited = set()
        
        while queue:
            current = queue.popleft()
            
            # Skip if already processed
            if current in local_visited or current in global_visited:
                continue
                
            # Skip if we hit a boundary (unless it's the start node)
            if current in boundary_nodes and current != start_node:
                continue
            
            # Add to subjob
            local_visited.add(current)
            subjob_components.add(current)
            
            # Follow all outgoing edges (both data and control)
            for _, target, edge_data in sorted(dag.out_edges(current, data=True), key=lambda e: (e[1])):
                # Skip if this edge creates a new subjob (parallelise)
                if (edge_data.get('edge_type') == 'control' and 
                    edge_data.get('trigger') == 'parallelise'):
                    continue
                    
                # Add target to queue if not visited and not boundary
                if (target not in local_visited and 
                    target not in global_visited and
                    target not in boundary_nodes):
                    queue.append(target)
            
            # Also follow incoming edges (for components connected only by data edges)
            for source, _, edge_data in sorted(dag.in_edges(current, data=True), key=lambda e: (e[0])):
                # Only follow data edges backwards to stay in same subjob
                if edge_data.get('edge_type') == 'data':
                    if (source not in local_visited and 
                        source not in global_visited and
                        source not in boundary_nodes):
                        queue.append(source)
        
        return subjob_components
    
    def _find_sync_node_for_parallel(self, parallel_targets: List[str]) -> Optional[str]:
        """Find the synchronise node that waits for these parallel branches."""
        for sync_target, sync_sources in self._synchronise_map.items():
            if set(sync_sources) == set(parallel_targets):
                return sync_target
        return None
    
    def _handle_nested_subjobs(self, dag: nx.DiGraph, 
                              component_to_subjob: Dict[str, str]) -> Dict[str, str]:
        """Handle nested parallelise patterns within existing subjobs."""
        # Keep processing until no new nested parallelism is found
        changed = True
        iteration = 0
        
        while changed and iteration < 10:  # Max 10 levels of nesting
            changed = False
            iteration += 1
            
            # Group components by current subjob
            current_subjobs = defaultdict(list)
            for comp, subjob_id in component_to_subjob.items():
                current_subjobs[subjob_id].append(comp)
            
            # Check each subjob for internal parallelise edges
            for subjob_id, components in current_subjobs.items():
                # Find parallelise sources within this subjob
                for comp in sorted(components):
                    if comp in self._parallelise_map:
                        targets = self._parallelise_map[comp]
                        
                        # Check if any target is in the same subjob (nested parallelism)
                        for target in targets:
                            if component_to_subjob.get(target) == subjob_id:
                                # We have nested parallelism!
                                changed = True
                                
                                # Create nested subjob IDs
                                for idx, nested_target in enumerate(sorted(targets)):
                                    nested_subjob_id = f"{subjob_id}.{comp}_parallel_{idx}"
                                    
                                    # Find sync node for these targets
                                    sync_node = self._find_sync_node_for_parallel(targets)
                                    boundaries = {sync_node} if sync_node else set()
                                    
                                    # Add other parallel targets as boundaries
                                    for t in targets:
                                        if t != nested_target:
                                            boundaries.add(t)
                                    
                                    # Flood fill for this nested subjob
                                    nested_components = self._flood_fill_subjob(
                                        dag, nested_target, boundaries, set()
                                    )
                                    
                                    # Update component assignments
                                    for nested_comp in nested_components:
                                        if nested_comp in components:  # Only reassign within current subjob
                                            component_to_subjob[nested_comp] = nested_subjob_id
                                
                                break  # Process one level at a time
        
        return component_to_subjob
    
    def _group_components_by_subjob(self) -> Dict[str, List[str]]:
        """Group components by their subjob ID."""
        subjob_components = defaultdict(list)
        
        for component, subjob_id in sorted(self._component_to_subjob.items()):
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
        """Generate comprehensive metadata for each subjob."""
        subjob_metadata = {}
        
        # Build subjob dependency graph
        subjob_deps = self._build_subjob_dependency_graph(dag, subjob_components)
        
        # Calculate execution order
        try:
            execution_order = list(nx.topological_sort(subjob_deps))
        except nx.NetworkXError:
            # Cycle detected - this should have been caught in validation
            execution_order = sorted(subjob_components.keys())
        
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
                if '.' in subjob_id:
                    # Nested subjob
                    parent_subjob = subjob_id.rsplit('.', 1)[0]
                else:
                    # Top-level parallel subjob
                    parent_subjob = 'main'
                
            # Find children by checking parallelise edges
            for comp in sorted(components):
                if comp in self._parallelise_map:
                    targets = self._parallelise_map[comp]
                    for idx, target in enumerate(sorted(targets)):
                        if '.' in subjob_id:
                            # Nested subjob children
                            child_subjobs.append(f"{subjob_id}.{comp}_parallel_{idx}")
                        else:
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
                'child_subjobs': sorted(child_subjobs),
                'checkpoint_boundary': True,  # All subjob ends are checkpoints
                'resume_point': True,
                'dependencies': sorted(list(subjob_deps.predecessors(subjob_id))),
                'startable_components': sorted(startable_in_subjob)
            }
        
        return subjob_metadata
    
    def _build_subjob_dependency_graph(self, dag: nx.DiGraph, 
                                      subjob_components: Dict[str, List[str]]) -> nx.DiGraph:
        """Build dependency graph between subjobs."""
        subjob_graph = nx.DiGraph()
        
        # Add all subjobs as nodes
        subjob_graph.add_nodes_from(subjob_components.keys())
        
        # Add edges based on control flow between subjobs
        for source, target, edge_data in sorted(dag.edges(data=True), key=lambda e: (e[0], e[1])):
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
            
            parallel_groups.append(sorted(group))  # Sort for determinism
        
        return parallel_groups