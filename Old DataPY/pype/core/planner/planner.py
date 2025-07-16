"""
DataPY Job Planner 

This module orchestrates the complete job planning process, transforming a validated JobModel
into an execution-ready PlanResult with metadata for runtime execution
"""

from datetime import datetime
from typing import Dict, List, Tuple, Any, Optional, Set
from dataclasses import dataclass
import networkx as nx

from pype.core.loader.loader import JobModel
from pype.core.registry.component_registry import ComponentRegistry
from pype.core.planner.graph_builder import GraphBuilder, GraphBuildError
from pype.core.planner.port_resolver import PortResolver, PortResolutionError
from pype.core.planner.subjob_analyzer import SubjobAnalyzer, SubjobAnalysisError
from pype.core.planner.iterator_validator import ForEachValidator
from pype.core.planner.structure_validator import (
    StructureValidator, 
    ValidationError, 
    ValidationWarning,
    StructureValidationError
)


from pype.core.utils.constants import (
    ENGINE_VERSION,
    DEFAULT_GC_THRESHOLD_MB,
    DEFAULT_RSS_SOFT_LIMIT_RATIO  
)

# Version constants
PLANNER_VERSION = "1.0.0"



@dataclass
class PlanResult:
    """
    Planning result with essential metadata for runtime execution including memory management.
    """
    # Core execution artifacts - Required for runtime
    dag: nx.DiGraph
    subjob_components: Dict[str, List[str]]
    subjob_execution_order: List[str]
    
    # Validation results
    validation_errors: List[ValidationError]
    validation_warnings: List[ValidationWarning]
    
    # Essential runtime metadata with memory management
    execution_metadata: Dict[str, Any]
    build_metadata: Dict[str, Any]


class PlanningError(Exception):
    """Base class for all planning errors."""
    pass


class CriticalPlanningError(PlanningError):
    """Raised when critical errors prevent plan creation."""
    def __init__(self, message: str, errors: List[ValidationError]):
        self.errors = errors
        super().__init__(message)


class PlanningPhaseError(PlanningError):
    """Raised when a specific planning phase fails."""
    def __init__(self, phase_name: str, message: str, errors: List[ValidationError]):
        self.phase_name = phase_name
        self.errors = errors
        super().__init__(f"Phase {phase_name} failed: {message}")


class JobPlanner:
    """
    Orchestrates complete job planning process with memory management integration.
    """
    
    def __init__(self, registry: ComponentRegistry):
        """Initialize planner with component registry."""
        self.registry = registry
        self.graph_builder = GraphBuilder(registry)
        self.port_resolver = PortResolver()
        self.subjob_analyzer = SubjobAnalyzer()
        self.structure_validator = StructureValidator(registry)
    
    def plan_job(self, job_model: JobModel) -> PlanResult:
        """
        Execute complete job planning process with memory management.
        
        Args:
            job_model: Validated JobModel from loader
            
        Returns:
            PlanResult with complete execution plan and memory management metadata
        """
        try:
            # Execute all planning phases in strict order
            dag, subjob_components, subjob_metadata, all_errors, all_warnings = self._execute_planning_phases(job_model)
            
            # Generate execution metadata with memory management
            execution_metadata = self._generate_execution_metadata(
                dag, subjob_components, subjob_metadata, job_model
            )
            
            # Create build metadata
            build_metadata = self._create_build_metadata(dag, job_model)
            
            # Build final result
            plan_result = self._build_plan_result(
                dag=dag,
                subjob_components=subjob_components,
                subjob_metadata=subjob_metadata,
                execution_metadata=execution_metadata,
                all_errors=all_errors,
                all_warnings=all_warnings,
                build_metadata=build_metadata
            )
            
            return plan_result
            
        except (CriticalPlanningError, PlanningPhaseError):
            raise
        except Exception as e:
            raise CriticalPlanningError(
                f"Unexpected error during planning: {str(e)}",
                [ValidationError(code="UNEXPECTED_ERROR", message=str(e))]
            )
    
    def _execute_planning_phases(self, job_model: JobModel) -> Tuple[nx.DiGraph, Dict[str, List[str]], Dict[str, Dict[str, Any]], List[ValidationError], List[ValidationWarning]]:
        """Execute all planning phases with strict dependency management."""
        all_errors = []
        all_warnings = []
        
        # Phase 1: Graph Building
        dag = self._phase_1_graph_building(job_model)
        
        # Phase 2: Structure Validation (cycles, basic structure)
        structure_errors, structure_warnings = self._phase_2_structure_validation(dag)
        all_errors.extend(structure_errors)
        all_warnings.extend(structure_warnings)
        
        # Phase 3: Port Resolution
        dag = self._phase_3_port_resolution(dag)
        
        # Phase 4: Iterator Validation
        dag = self._phase_4_iterator_validation(dag)
        
        # Phase 5: Subjob Analysis
        subjob_components, subjob_metadata = self._phase_5_subjob_analysis(dag)
        
        return dag, subjob_components, subjob_metadata, all_errors, all_warnings

    def _phase_1_graph_building(self, job_model: JobModel) -> nx.DiGraph:
        """Phase 1: Build core DAG structure."""
        try:
            dag = self.graph_builder.build_graph(job_model)
            return dag
        except GraphBuildError as e:
            raise PlanningPhaseError(
                "Graph Building", str(e),
                [ValidationError(code="GRAPH_BUILD_ERROR", message=str(e))]
            )
    
    def _phase_2_structure_validation(self, dag: nx.DiGraph) -> Tuple[List[ValidationError], List[ValidationWarning]]:
        """Phase 2: Comprehensive structure validation."""
        try:
            errors, warnings = self.structure_validator.validate_structure(dag)
            
            # Critical errors abort planning
            critical_errors = [e for e in errors if e.severity == "ERROR"]
            if critical_errors:
                raise CriticalPlanningError(
                    f"Structure validation failed with {len(critical_errors)} critical errors",
                    critical_errors
                )
            
            return errors, warnings
        except StructureValidationError as e:
            raise CriticalPlanningError(
                str(e), [ValidationError(code="STRUCTURE_VALIDATION_ERROR", message=str(e))]
            )
    
    def _phase_3_port_resolution(self, dag: nx.DiGraph) -> nx.DiGraph:
        """Phase 3: Resolve wildcard ports and validate connectivity."""
        try:
            updated_dag, errors = self.port_resolver.resolve_ports(dag)
            
            if errors:
                raise PlanningPhaseError(
                    "Port Resolution", f"Found {len(errors)} port resolution errors",
                    [ValidationError(code="PORT_RESOLUTION_ERROR", message=error) for error in errors]
                )
            
            return updated_dag
        except PortResolutionError as e:
            raise PlanningPhaseError(
                "Port Resolution", str(e),
                [ValidationError(code="PORT_RESOLUTION_ERROR", message=str(e))]
            )
    
    def _phase_4_iterator_validation(self, dag: nx.DiGraph) -> nx.DiGraph:
        """Phase 4: Iterator boundary marking and forEach validation."""
        try:
            iterator_validator = ForEachValidator()
            errors = iterator_validator.analyze_forEach_boundaries(dag)
            
            if errors:
                raise PlanningPhaseError(
                    "Iterator Validation", f"Found {len(errors)} forEach validation errors",
                    [ValidationError(code="FOREACH_VALIDATION_ERROR", message=error) for error in errors]
                )
            
            return dag
        except Exception as e:
            raise PlanningPhaseError(
                "Iterator Validation", str(e),
                [ValidationError(code="FOREACH_VALIDATION_ERROR", message=str(e))]
            )
    
    def _phase_5_subjob_analysis(self, dag: nx.DiGraph) -> Tuple[Dict[str, List[str]], Dict[str, Dict[str, Any]]]:
        """Phase 5: Analyze subjob structure for execution planning."""
        try:
            subjob_components, subjob_metadata, errors = self.subjob_analyzer.analyze_subjobs(dag)
            
            if errors:
                raise PlanningPhaseError(
                    "Subjob Analysis", f"Found {len(errors)} subjob analysis errors",
                    [ValidationError(code="SUBJOB_ANALYSIS_ERROR", message=error) for error in errors]
                )
            
            return subjob_components, subjob_metadata
        except SubjobAnalysisError as e:
            raise PlanningPhaseError(
                "Subjob Analysis", str(e),
                [ValidationError(code="SUBJOB_ANALYSIS_ERROR", message=str(e))]
            )
    
    def _find_job_starters(self, dag: nx.DiGraph) -> List[str]:
        """Find components that can start the job (startable=True + no incoming edges)."""
        job_starters = []
        for node, node_data in dag.nodes(data=True):
            is_registry_startable = node_data.get('startable', False)
            has_any_incoming_edges = dag.in_degree(node) > 0
            
            if is_registry_startable and not has_any_incoming_edges:
                job_starters.append(node)
        return job_starters
    
    def _calculate_edge_use_counts(self, dag: nx.DiGraph) -> Dict[str, int]:
        """
        Calculate use counts for data edges using "free-when-last-consumer-finishes" policy.
        
        Args:
            dag: NetworkX DiGraph with resolved ports
            
        Returns:
            Dict mapping edge_id ("source.port->target.port") to use count
        """
        use_counts = {}
        
        # Process all data edges to count consumers
        for source, target, edge_data in dag.edges(data=True):
            if edge_data.get('edge_type') != 'data':
                continue
                
            source_port = edge_data.get('source_port', 'main')
            target_port = edge_data.get('target_port', 'main')
            
            # Create edge ID: "source.port->target.port"
            edge_id = f"{source}.{source_port}->{target}.{target_port}"
            
            # Each data edge has exactly one consumer (the target component)
            # Use count represents how many components will consume this specific edge
            use_counts[edge_id] = 1
        
        return use_counts
    
    def _generate_memory_management_metadata(self, dag: nx.DiGraph, job_config: Dict[str, Any]) -> Dict[str, Any]:
        """
        Generate memory management metadata for runtime optimization.
        
        Args:
            dag: NetworkX DiGraph with resolved ports
            job_config: Job configuration from JobModel
            
        Returns:
            Memory management configuration and edge use counts
        """
        # Calculate edge use counts for reference counting
        edge_use_counts = self._calculate_edge_use_counts(dag)
        
        # Extract GC configuration from job_config with defaults
        gc_threshold_mb = job_config.get('gc_threshold_mb', DEFAULT_GC_THRESHOLD_MB)
        rss_soft_limit_ratio = job_config.get('rss_soft_limit_ratio', DEFAULT_RSS_SOFT_LIMIT_RATIO)
        
        return {
            'edge_use_counts': edge_use_counts,
            'gc_threshold_mb': gc_threshold_mb,
            'rss_soft_limit_ratio': rss_soft_limit_ratio,
            'memory_policy': 'free_when_last_consumer_finishes'
        }
    
    def _generate_execution_metadata(self, dag: nx.DiGraph, 
                               subjob_components: Dict[str, List[str]], 
                               subjob_metadata: Dict[str, Dict[str, Any]],
                               job_model: JobModel) -> Dict[str, Any]:
        """
        Generate execution metadata with memory management for runtime optimization.
        """
        # Find job starter components
        job_starters = self._find_job_starters(dag)
        
        if not job_starters:
            raise CriticalPlanningError(
                "No job starter components found",
                [ValidationError(code="NO_JOB_STARTERS", message="No components can start the job execution")]
            )
        
        # Create dependency and port caches
        component_dependencies, port_mapping = self._create_dependency_and_port_caches(dag)
        
        # Create component-to-subjob lookup
        component_to_subjob = self._create_component_subjob_lookup(subjob_components)
        
        # Generate dependency tokens for event-driven scheduler
        dependency_tokens = self._generate_dependency_tokens(dag, subjob_components, component_to_subjob)
        
        # Generate memory management metadata
        memory_management = self._generate_memory_management_metadata(dag, self._extract_job_config_metadata(job_model))
        
        # Extract all node metadata for engine
        node_metadata = {}
        for node, data in dag.nodes(data=True):
            node_metadata[node] = {
                'component_type': data.get('component_type'),
                'config': data.get('config', {}),
                'registry_metadata': data.get('registry_metadata', {}),
                'startable': data.get('startable', False),
                'allow_multi_in': data.get('allow_multi_in', False),
                'idempotent': data.get('idempotent', True),
                'input_ports': data.get('input_ports', []),
                'output_ports': data.get('output_ports', []),
                'dependencies': data.get('dependencies', []),
                'forEach_boundary': data.get('forEach_boundary', ''),
                'is_subjob_start': data.get('is_subjob_start', False),
                'subjob_id': component_to_subjob.get(node, 'main')
            }
        
        return {
            # Essential runtime data
            'job_starters': job_starters,
            'component_dependencies': component_dependencies,
            'port_mapping': port_mapping,
            'node_metadata': node_metadata,
            'subjob_boundaries': subjob_metadata,
            'dependency_tokens': dependency_tokens,
            
            # Memory management metadata
            'memory_management': memory_management,
            
            # Job configuration for runtime
            'job_config': self._extract_job_config_metadata(job_model)
        }
        
    def _generate_dependency_tokens(self, dag: nx.DiGraph, 
                                subjob_components: Dict[str, List[str]],
                                component_to_subjob: Dict[str, str]) -> Dict[str, Set[str]]:
        """Generate dependency tokens for event-driven scheduler with explicit trigger encoding."""
        # Initialize waiting tokens for each subjob
        subjob_waiting_tokens = {subjob_id: set() for subjob_id in subjob_components.keys()}
        
        # Process all control edges to generate tokens
        for source, target, edge_data in dag.edges(data=True):
            if edge_data.get('edge_type') != 'control':
                continue
                
            source_subjob = component_to_subjob.get(source)
            target_subjob = component_to_subjob.get(target)
            
            # Skip if within same subjob (handled by component execution order)
            if source_subjob == target_subjob:
                continue
                
            trigger = edge_data.get('trigger', 'unknown')
            
            # Generate appropriate token based on trigger type with explicit encoding
            if trigger == 'ok':
                token = f"{source}::ok"
            elif trigger == 'error':
                token = f"{source}::error"
            elif trigger.startswith('if'):
                token = f"{source}::{trigger}"  # e.g., "comp::if3"
            elif trigger == 'subjob_ok':
                token = f"SUBJOB_OK::{source_subjob}"
            elif trigger == 'subjob_error':
                token = f"SUBJOB_ERR::{source_subjob}"
            else:
                continue
            
            # Add token to target subjob's waiting list
            if target_subjob:
                subjob_waiting_tokens[target_subjob].add(token)
        
        # Convert sets to sorted lists for deterministic serialization
        return {
            subjob_id: sorted(list(tokens)) 
            for subjob_id, tokens in subjob_waiting_tokens.items()
        }
    
    def _create_dependency_and_port_caches(self, dag: nx.DiGraph) -> Tuple[Dict[str, Dict[str, List[str]]], Dict[str, Dict[str, List[Tuple[str, str]]]]]:
        """Create both dependency cache and port mapping in a single graph traversal."""
        dependencies = {}
        port_mapping = {}
        
        for node in dag.nodes():
            data_deps = []
            control_deps = []
            inputs = []
            outputs = []
            
            # Process incoming edges
            for source, target, edge_data in dag.in_edges(node, data=True):
                edge_type = edge_data.get('edge_type')
                if edge_type == 'data':
                    data_deps.append(source)
                    target_port = edge_data.get('target_port', 'main')
                    inputs.append((target_port, source))
                elif edge_type == 'control':
                    control_deps.append(source)
            
            # Process outgoing edges
            for source, target, edge_data in dag.out_edges(node, data=True):
                if edge_data.get('edge_type') == 'data':
                    source_port = edge_data.get('source_port', 'main')
                    outputs.append((source_port, target))
            
            dependencies[node] = {
                'data': sorted(data_deps),
                'control': sorted(control_deps)
            }
            
            port_mapping[node] = {
                'inputs': sorted(inputs),
                'outputs': sorted(outputs)
            }
        
        return dependencies, port_mapping
    
    def _create_component_subjob_lookup(self, subjob_components: Dict[str, List[str]]) -> Dict[str, str]:
        """Create O(1) lookup table for component-to-subjob mapping."""
        component_to_subjob = {}
        for subjob_id, components in subjob_components.items():
            for component in components:
                component_to_subjob[component] = subjob_id
        return component_to_subjob
    
    def _extract_job_config_metadata(self, job_model: JobModel) -> Dict[str, Any]:
        """Extract ALL job configuration metadata (core + arbitrary fields)."""
        if hasattr(job_model.job_config, 'model_dump'):
            return job_model.job_config.model_dump()
        elif hasattr(job_model.job_config, 'dict'):
            return job_model.job_config.dict()
        else:
            return job_model.job_config.__dict__
        
    def _create_build_metadata(self, dag: nx.DiGraph, job_model: JobModel) -> Dict[str, Any]:
        """Create build metadata for the plan."""
        return {
            'build_timestamp': datetime.now().isoformat(),
            'engine_version': ENGINE_VERSION,
            'planner_version': PLANNER_VERSION,
            'job_name': job_model.job.name,
            'job_version': job_model.job.version,
            'component_count': len(dag.nodes()),
            'edge_count': len(dag.edges()),
            'data_edge_count': sum(1 for _, _, d in dag.edges(data=True) if d.get('edge_type') == 'data'),
            'control_edge_count': sum(1 for _, _, d in dag.edges(data=True) if d.get('edge_type') == 'control')
        }
    
    def _build_plan_result(self, dag: nx.DiGraph, 
                        subjob_components: Dict[str, List[str]], 
                        subjob_metadata: Dict[str, Dict[str, Any]],
                        execution_metadata: Dict[str, Any],
                        all_errors: List[ValidationError], 
                        all_warnings: List[ValidationWarning],
                        build_metadata: Dict[str, Any]) -> PlanResult:
        """Construct final PlanResult."""
        # Extract execution order from subjob metadata
        subjob_execution_order = []
        for subjob_id in sorted(subjob_metadata.keys(), 
                            key=lambda x: subjob_metadata[x].get('execution_order', 0)):
            subjob_execution_order.append(subjob_id)
        
        return PlanResult(
            dag=dag,
            subjob_components=subjob_components,
            subjob_execution_order=subjob_execution_order,
            validation_errors=all_errors,
            validation_warnings=all_warnings,
            execution_metadata=execution_metadata,
            build_metadata=build_metadata
        )