"""
DataPY Job Planner - Simplified job planning with essential functionality.

This module orchestrates the complete job planning process, transforming a validated JobModel
into an execution-ready PlanResult with metadata for runtime execution.

The planner follows a strict 4-phase approach:
1. Graph Building - Convert JobModel to NetworkX DAG
2. Port Resolution - Resolve wildcard ports and validate connectivity
3. Subjob Analysis - Detect subjob boundaries using control edges
4. Structure Validation - Comprehensive DAG validation
"""

from datetime import datetime
from typing import Dict, List, Tuple, Any, Optional
from dataclasses import dataclass
import networkx as nx

from pype.core.loader.loader import JobModel
from pype.core.registry.component_registry import ComponentRegistry
from pype.core.planner.graph_builder import GraphBuilder, GraphBuildError
from pype.core.planner.port_resolver import PortResolver, PortResolutionError
from pype.core.planner.subjob_analyzer import SubjobAnalyzer, SubjobAnalysisError
from pype.core.planner.structure_validator import (
    StructureValidator, 
    ValidationError, 
    ValidationWarning,
    StructureValidationError
)
from pype.core.utils.constants import ENGINE_VERSION


# Version constants
PLANNER_VERSION = "1.0.0"


@dataclass
class PlanResult:
    """
    Simplified planning result with essential metadata for runtime execution.
    
    This dataclass contains all artifacts needed for runtime execution, focusing
    on essential DAG structure and subjob planning.
    
    Attributes:
        dag: NetworkX DiGraph with component metadata and resolved ports
        subjob_components: Mapping of subjob IDs to component lists
        subjob_execution_order: Topologically sorted subjob execution sequence
        validation_errors: Critical errors that prevent execution
        validation_warnings: Non-critical issues
        execution_metadata: Essential runtime data
        build_metadata: Build timestamp, versions, and artifact metadata
    """
    # Core execution artifacts - Required for runtime
    dag: nx.DiGraph
    subjob_components: Dict[str, List[str]]
    subjob_execution_order: List[str]
    
    # Validation results
    validation_errors: List[ValidationError]
    validation_warnings: List[ValidationWarning]
    
    # Essential runtime metadata
    execution_metadata: Dict[str, Any]
    build_metadata: Dict[str, Any]


class PlanningError(Exception):
    """Base class for all planning errors."""
    pass


class CriticalPlanningError(PlanningError):
    """
    Raised when critical errors prevent plan creation.
    
    These errors indicate fundamental issues that make the job unexecutable,
    such as circular dependencies, missing components, or invalid configurations.
    """
    def __init__(self, message: str, errors: List[ValidationError]):
        self.errors = errors
        super().__init__(message)


class PlanningPhaseError(PlanningError):
    """
    Raised when a specific planning phase fails.
    
    These errors indicate issues within a particular planning phase that prevent
    progression to subsequent phases.
    """
    def __init__(self, phase_name: str, message: str, errors: List[ValidationError]):
        self.phase_name = phase_name
        self.errors = errors
        super().__init__(f"Phase {phase_name} failed: {message}")


class JobPlanner:
    """
    Orchestrates complete job planning process with simplified approach.
    
    The JobPlanner transforms a validated JobModel into an execution-ready PlanResult
    through four distinct phases, generating essential metadata for runtime execution.
    
    Planning Phases:
    1. Graph Building: Convert JobModel to NetworkX DAG
    2. Port Resolution: Resolve wildcard ports and validate connectivity
    3. Subjob Analysis: Detect boundaries using control edges
    4. Structure Validation: Comprehensive DAG validation
    """
    
    def __init__(self, registry: ComponentRegistry):
        """
        Initialize planner with component registry.
        
        Args:
            registry: Component registry for metadata lookup during planning
        """
        self.registry = registry
        self.graph_builder = GraphBuilder(registry)
        self.port_resolver = PortResolver()
        self.subjob_analyzer = SubjobAnalyzer()
        self.structure_validator = StructureValidator(registry)
    
    def plan_job(self, job_model: JobModel) -> PlanResult:
        """
        Execute complete job planning process.
        
        This is the main entry point that orchestrates all planning phases and produces
        a complete PlanResult ready for runtime execution.
        
        Args:
            job_model: Validated JobModel from loader with resolved templates
            
        Returns:
            PlanResult with complete execution plan and essential metadata
            
        Raises:
            CriticalPlanningError: When critical errors prevent planning
            PlanningPhaseError: When a specific phase fails
        """
        try:
            # Execute all planning phases in strict order
            dag, subjob_components, subjob_metadata, all_errors, all_warnings = self._execute_planning_phases(job_model)
            
            # Generate essential execution metadata
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
            # Re-raise planning errors with full context
            raise
        except Exception as e:
            # Wrap unexpected errors in standard format
            raise CriticalPlanningError(
                f"Unexpected error during planning: {str(e)}",
                [ValidationError(
                    code="UNEXPECTED_ERROR",
                    message=str(e)
                )]
            )
    
    def _execute_planning_phases(self, job_model: JobModel) -> Tuple[nx.DiGraph, Dict[str, List[str]], Dict[str, Dict[str, Any]], List[ValidationError], List[ValidationWarning]]:
        """
        Execute all planning phases with strict dependency management.
        
        Returns:
            Tuple of (final_dag, subjob_components, subjob_metadata, all_errors, all_warnings)
        """
        all_errors = []
        all_warnings = []
        
        # Phase 1: Graph Building
        dag = self._phase_1_graph_building(job_model)
        
        # Phase 2: Port Resolution
        dag = self._phase_2_port_resolution(dag)
        
        # Phase 3: Subjob Analysis
        subjob_components, subjob_metadata = self._phase_3_subjob_analysis(dag)
        
        # Phase 4: Structure Validation
        errors, warnings = self._phase_4_structure_validation(dag, subjob_components)
        all_errors.extend(errors)
        all_warnings.extend(warnings)
        
        return dag, subjob_components, subjob_metadata, all_errors, all_warnings
    
    def _phase_1_graph_building(self, job_model: JobModel) -> nx.DiGraph:
        """
        Phase 1: Build core DAG structure.
        
        This phase converts the JobModel into a NetworkX DiGraph with metadata
        for each component and edge. It validates component existence in the registry
        and builds the foundation for subsequent planning phases.
        
        Args:
            job_model: Validated JobModel with all templates resolved
            
        Returns:
            NetworkX DiGraph with component nodes and connection edges
            
        Raises:
            PlanningPhaseError: On critical graph building errors
        """
        try:
            dag = self.graph_builder.build_graph(job_model)
            return dag
            
        except GraphBuildError as e:
            # Critical error - abort immediately
            raise PlanningPhaseError(
                "Graph Building",
                str(e),
                [ValidationError(
                    code="GRAPH_BUILD_ERROR",
                    message=str(e)
                )]
            )
    
    def _phase_2_port_resolution(self, dag: nx.DiGraph) -> nx.DiGraph:
        """
        Phase 2: Resolve wildcard ports and validate connectivity.
        
        This phase expands wildcard port patterns and validates port connectivity.
        
        Args:
            dag: DAG from Phase 1 with basic structure
            
        Returns:
            Updated DAG with resolved ports and connectivity metadata
            
        Raises:
            PlanningPhaseError: On critical port resolution errors
        """
        try:
            updated_dag, errors = self.port_resolver.resolve_ports(dag)
            
            if errors:
                # Port resolution errors are critical
                raise PlanningPhaseError(
                    "Port Resolution",
                    f"Found {len(errors)} port resolution errors",
                    [ValidationError(
                        code="PORT_RESOLUTION_ERROR",
                        message=error
                    ) for error in errors]
                )
            
            return updated_dag
            
        except PortResolutionError as e:
            raise PlanningPhaseError(
                "Port Resolution",
                str(e),
                [ValidationError(
                    code="PORT_RESOLUTION_ERROR",
                    message=str(e)
                )]
            )
    
    def _phase_3_subjob_analysis(self, dag: nx.DiGraph) -> Tuple[Dict[str, List[str]], Dict[str, Dict[str, Any]]]:
        """
        Phase 3: Analyze subjob structure for execution planning.
        
        This phase detects subjob boundaries using control edges and generates
        metadata for parallel execution and checkpointing.
        
        Args:
            dag: Resolved DAG from Phase 2 with concrete ports
            
        Returns:
            Tuple of (subjob_components, subjob_metadata)
            
        Raises:
            PlanningPhaseError: On critical subjob analysis errors
        """
        try:
            subjob_components, subjob_metadata, errors = self.subjob_analyzer.analyze_subjobs(dag)
            
            if errors:
                # Subjob analysis errors are critical
                raise PlanningPhaseError(
                    "Subjob Analysis",
                    f"Found {len(errors)} subjob analysis errors",
                    [ValidationError(
                        code="SUBJOB_ANALYSIS_ERROR",
                        message=error
                    ) for error in errors]
                )
            
            return subjob_components, subjob_metadata
            
        except SubjobAnalysisError as e:
            raise PlanningPhaseError(
                "Subjob Analysis",
                str(e),
                [ValidationError(
                    code="SUBJOB_ANALYSIS_ERROR",
                    message=str(e)
                )]
            )
    
    def _phase_4_structure_validation(self, dag: nx.DiGraph, subjob_components: Dict[str, List[str]]) -> Tuple[List[ValidationError], List[ValidationWarning]]:
        """
        Phase 4: Comprehensive structure validation.
        
        This phase performs final validation of the complete DAG structure,
        checking for cycles, unreachable components, and invalid references.
        
        Args:
            dag: Complete DAG from previous phases
            subjob_components: Subjob structure from Phase 3
            
        Returns:
            Tuple of (validation_errors, validation_warnings)
            
        Raises:
            CriticalPlanningError: On critical validation errors that prevent execution
        """
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
            # Structure validation critical errors
            raise CriticalPlanningError(
                str(e),
                [ValidationError(
                    code="STRUCTURE_VALIDATION_ERROR",
                    message=str(e)
                )]
            )
    
    def _generate_execution_metadata(self, dag: nx.DiGraph, 
                                   subjob_components: Dict[str, List[str]], 
                                   subjob_metadata: Dict[str, Dict[str, Any]],
                                   job_model: JobModel) -> Dict[str, Any]:
        """
        Generate essential execution metadata for runtime optimization.
        
        This method creates metadata needed for runtime execution, focusing on
        essential data structures that eliminate runtime registry/DAG queries.
        
        Returns:
            Essential metadata dictionary for runtime execution
        """
        # Create component dependency cache and port mapping in single pass
        component_dependencies, port_mapping = self._create_dependency_and_port_caches(dag)
        
        # Create component-to-subjob lookup for O(1) access
        component_to_subjob = self._create_component_subjob_lookup(subjob_components)
        
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
                'iterator_boundary': data.get('iterator_boundary', ''),
                'is_subjob_start': data.get('is_subjob_start', False),
                'subjob_id': component_to_subjob.get(node, 'main')
            }
        
        return {
            # Essential runtime data
            'component_dependencies': component_dependencies,
            'port_mapping': port_mapping,
            'node_metadata': node_metadata,
            'subjob_boundaries': subjob_metadata,
            
            # Job configuration for runtime
            'job_config': self._extract_job_config_metadata(job_model)
        }
    
    def _create_dependency_and_port_caches(self, dag: nx.DiGraph) -> Tuple[Dict[str, Dict[str, List[str]]], Dict[str, Dict[str, List[Tuple[str, str]]]]]:
        """
        Create both dependency cache and port mapping in a single graph traversal.
        
        Returns:
            Tuple of (component_dependencies, port_mapping)
        """
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
        """
        Create O(1) lookup table for component-to-subjob mapping.
        
        Returns:
            {component_name: subjob_id}
        """
        component_to_subjob = {}
        for subjob_id, components in subjob_components.items():
            for component in components:
                component_to_subjob[component] = subjob_id
        return component_to_subjob
    
    def _extract_job_config_metadata(self, job_model: JobModel) -> Dict[str, Any]:
        """Extract job configuration metadata for runtime decisions."""
        # Convert Pydantic ExecutionConfigModel to simple dict
        execution_config = getattr(job_model.job_config, 'execution', None)
        execution_dict = None
        if execution_config is not None:
            if hasattr(execution_config, 'model_dump'):
                execution_dict = execution_config.model_dump()
            elif hasattr(execution_config, 'dict'):
                execution_dict = execution_config.dict()
        
        return {
            'retries': job_model.job_config.retries,
            'timeout': job_model.job_config.timeout,
            'fail_strategy': job_model.job_config.fail_strategy,
            'execution_mode': job_model.job_config.execution_mode,
            'chunk_size': job_model.job_config.chunk_size,
            'execution_config': execution_dict  
        }
        
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
    
    def serialize_dag_for_pjob(self, dag: nx.DiGraph) -> Dict[str, Any]:
        """
        Convert NetworkX DAG to msgpack-compatible format for .pjob file.
        
        Args:
            dag: NetworkX DiGraph with full metadata
            
        Returns:
            Dictionary representation ready for msgpack serialization
        """
        # Convert to node-link format for serialization
        dag_data = nx.node_link_data(dag)
        return dag_data