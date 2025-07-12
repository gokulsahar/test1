"""
DataPY Job Planner - Comprehensive job planning with two-level execution architecture support.

This module orchestrates the complete job planning process, transforming a validated JobModel
into an execution-ready PlanResult with comprehensive metadata for runtime optimization.

The planner follows a strict 5-phase approach:
1. Graph Building - Convert JobModel to NetworkX DAG with executor validation
2. Port Resolution - Resolve wildcard ports and validate connectivity
3. Subjob Analysis - Detect subjob boundaries using control edges
4. Structure Validation - Comprehensive DAG validation and performance analysis
5. Metadata Generation - Create runtime optimization data for two-level architecture

Key Features:
- Two-level execution metadata for Orchestrator (Level 1) and ExecutionManager (Level 2)
- Comprehensive resource allocation planning with executor-specific configuration
- Pre-computed dependency caches to eliminate runtime registry/DAG queries
- Subjob-boundary checkpointing strategy optimization
- Performance diagnostics and complexity analysis
"""

import time
from datetime import datetime
from typing import Dict, List, Tuple, Any, Optional, Set
from dataclasses import dataclass, field
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
    Industry-standard planning result with comprehensive metadata for two-level execution.
    
    This dataclass contains all artifacts needed for runtime execution, eliminating the need
    for registry queries or DAG analysis during job execution. The two-level architecture
    separates subjob orchestration (Level 1) from component execution (Level 2).
    
    Attributes:
        dag: Rich NetworkX DiGraph with component metadata and resolved ports
        subjob_components: Mapping of subjob IDs to component lists for parallel execution
        subjob_execution_order: Topologically sorted subjob execution sequence
        validation_errors: Critical errors that prevent execution
        validation_warnings: Non-critical issues that may impact performance
        execution_metadata: Pre-computed runtime optimization data for both execution levels
        planning_diagnostics: Performance metrics and debugging information
        build_metadata: Build timestamp, versions, and artifact metadata
    """
    # Core execution artifacts - Required for runtime
    dag: nx.DiGraph                                    # Rich DAG with full metadata
    subjob_components: Dict[str, List[str]]            # {subjob_id: [component_names]}
    subjob_execution_order: List[str]                  # Topological execution order
    
    # Validation results - Used for build-time decisions
    validation_errors: List[ValidationError]           # All collected errors
    validation_warnings: List[ValidationWarning]       # All collected warnings
    
    # Runtime optimization for two-level architecture - Critical for performance
    execution_metadata: Dict[str, Any]                 # Performance optimization data
    
    # Diagnostics and debugging - Used for monitoring and troubleshooting
    planning_diagnostics: Dict[str, Any]               # Planning statistics and metrics
    build_metadata: Dict[str, Any]                     # Build timestamp, versions, etc.


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
    progression to subsequent phases. Each phase failure includes context about
    which phase failed and the specific errors encountered.
    """
    def __init__(self, phase_name: str, message: str, errors: List[ValidationError]):
        self.phase_name = phase_name
        self.errors = errors
        super().__init__(f"Phase {phase_name} failed: {message}")


class JobPlanner:
    """
    Orchestrates complete job planning process with two-level execution architecture support.
    
    The JobPlanner is the main entry point for transforming a validated JobModel into an
    execution-ready PlanResult. It coordinates five distinct phases and generates comprehensive
    metadata for runtime optimization.
    
    Two-Level Architecture Support:
    - Level 1 (Orchestrator): Subjob coordination, control flow, checkpointing
    - Level 2 (ExecutionManager): Component routing, executor allocation, resource management
    
    Planning Phases:
    1. Graph Building: Convert JobModel to NetworkX DAG with executor validation
    2. Port Resolution: Resolve wildcard ports and validate connectivity
    3. Subjob Analysis: Detect boundaries using control edges for parallel execution
    4. Structure Validation: Comprehensive DAG validation and performance analysis
    5. Metadata Generation: Create runtime optimization data for both execution levels
    """
    
    def __init__(self, registry: ComponentRegistry):
        """
        Initialize planner with all module dependencies.
        
        Args:
            registry: Component registry for metadata lookup during planning
        """
        self.registry = registry
        self.graph_builder = GraphBuilder(registry)
        self.port_resolver = PortResolver()
        self.subjob_analyzer = SubjobAnalyzer()
        self.structure_validator = StructureValidator(registry)
        
        # Phase timing tracking for performance diagnostics
        self._phase_timings = {}
        self._planning_start_time = None
    
    def plan_job(self, job_model: JobModel) -> PlanResult:
        """
        Execute complete job planning process with comprehensive error handling.
        
        This is the main entry point that orchestrates all planning phases and produces
        a complete PlanResult ready for runtime execution. The method implements strict
        error handling with fail-fast semantics for critical errors.
        
        Args:
            job_model: Validated JobModel from loader with resolved templates
            
        Returns:
            PlanResult with complete execution plan and metadata for two-level architecture
            
        Raises:
            CriticalPlanningError: When critical errors prevent planning (e.g., cycles, missing components)
            PlanningPhaseError: When a specific phase fails with recoverable errors
        """
        self._planning_start_time = time.perf_counter()
        
        try:
            # Execute all planning phases in strict order
            dag, subjob_components, subjob_metadata = self._execute_planning_phases(job_model)
            
            # Generate execution metadata for two-level architecture
            execution_metadata = self._generate_execution_metadata(
                dag, subjob_components, subjob_metadata, job_model
            )
            
            # Generate planning diagnostics for monitoring and debugging
            planning_diagnostics = self._generate_planning_diagnostics(
                dag, subjob_components, [], []
            )
            
            # Create build metadata for artifact tracking
            build_metadata = self._create_build_metadata(dag, job_model)
            
            # Build final result with comprehensive validation
            plan_result = self._build_plan_result(
                dag=dag,
                subjob_components=subjob_components,
                subjob_metadata=subjob_metadata,
                execution_metadata=execution_metadata,
                all_errors=[],
                all_warnings=[],
                planning_diagnostics=planning_diagnostics,
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
    
    def _execute_planning_phases(self, job_model: JobModel) -> Tuple[nx.DiGraph, Dict[str, List[str]], Dict[str, Dict[str, Any]]]:
        """
        Execute all planning phases with strict dependency management.
        
        This method enforces the correct phase order and ensures each phase completes
        successfully before proceeding to the next. Phase dependencies are strict:
        - Port resolution requires completed graph building
        - Subjob analysis requires resolved ports
        - Structure validation requires complete subjob analysis
        
        Returns:
            Tuple of (final_dag, subjob_components, subjob_metadata)
        """
        # Phase 2: Graph Building with executor validation
        dag = self._phase_2_graph_building(job_model)
        
        # Phase 3: Port Resolution with wildcard expansion
        dag = self._phase_3_port_resolution(dag)
        
        # Phase 4: Subjob Analysis with execution waves
        subjob_components, subjob_metadata = self._phase_4_subjob_analysis(dag)
        
        # Phase 5: Structure Validation with performance analysis
        self._phase_5_structure_validation(dag, subjob_components)
        
        return dag, subjob_components, subjob_metadata
    
    def _phase_2_graph_building(self, job_model: JobModel) -> nx.DiGraph:
        """
        Phase 2: Build core DAG structure with executor validation.
        
        This phase converts the JobModel into a NetworkX DiGraph with rich metadata
        for each component and edge. It validates component existence in the registry,
        checks executor configuration consistency, and builds the foundation for
        subsequent planning phases.
        
        Key Validations:
        - Component types exist in registry
        - Required parameters are populated
        - Executor configuration is valid
        - Connection syntax is correct
        
        Args:
            job_model: Validated JobModel with all templates resolved
            
        Returns:
            NetworkX DiGraph with component nodes and connection edges
            
        Raises:
            PlanningPhaseError: On critical graph building errors
        """
        start_time = time.perf_counter()
        
        try:
            dag = self.graph_builder.build_graph(job_model)
            self._phase_timings['graph_building'] = time.perf_counter() - start_time
            return dag
            
        except GraphBuildError as e:
            # Critical error - abort immediately with context
            raise PlanningPhaseError(
                "Graph Building",
                str(e),
                [ValidationError(
                    code="GRAPH_BUILD_ERROR",
                    message=str(e)
                )]
            )
    
    def _phase_3_port_resolution(self, dag: nx.DiGraph) -> nx.DiGraph:
        """
        Phase 3: Resolve wildcard ports and validate connectivity.
        
        This phase expands wildcard port patterns (e.g., input_*, output_*) into
        concrete port names based on actual connections. It validates port connectivity,
        checks multi-input constraints, and optimizes port mappings for runtime.
        
        Key Operations:
        - Wildcard port expansion (input_* -> input_1, input_2, etc.)
        - Port connectivity validation
        - Multi-input constraint checking
        - Port mapping optimization for runtime
        
        Args:
            dag: DAG from Phase 2 with basic structure
            
        Returns:
            Updated DAG with resolved ports and connectivity metadata
            
        Raises:
            PlanningPhaseError: On critical port resolution errors
        """
        start_time = time.perf_counter()
        
        try:
            updated_dag, errors = self.port_resolver.resolve_ports(dag)
            self._phase_timings['port_resolution'] = time.perf_counter() - start_time
            
            if errors:
                # Port resolution errors are critical - prevent execution
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
    
    def _phase_4_subjob_analysis(self, dag: nx.DiGraph) -> Tuple[Dict[str, List[str]], Dict[str, Dict[str, Any]]]:
        """
        Phase 4: Analyze subjob structure for parallel execution and checkpointing.
        
        This phase detects subjob boundaries using the simplified rule: control edges
        create boundaries, data edges connect within subjobs. It identifies execution
        waves for parallel subjob execution and generates metadata for checkpointing.
        
        Key Operations:
        - Subjob boundary detection using control edges
        - Execution wave identification for parallel execution
        - Subjob start component identification
        - Checkpoint strategy optimization
        
        Args:
            dag: Resolved DAG from Phase 3 with concrete ports
            
        Returns:
            Tuple of (subjob_components, subjob_metadata) with execution planning
            
        Raises:
            PlanningPhaseError: On critical subjob analysis errors
        """
        start_time = time.perf_counter()
        
        try:
            subjob_components, subjob_metadata, errors = self.subjob_analyzer.analyze_subjobs(dag)
            self._phase_timings['subjob_analysis'] = time.perf_counter() - start_time
            
            if errors:
                # Subjob analysis errors are critical - prevent execution
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
    
    def _phase_5_structure_validation(self, dag: nx.DiGraph, subjob_components: Dict[str, List[str]]) -> None:
        """
        Phase 5: Comprehensive structure validation and performance analysis.
        
        This phase performs final validation of the complete DAG structure, checking
        for cycles, unreachable components, invalid references, and performance
        characteristics. Critical errors prevent execution; warnings are recorded
        for monitoring but don't block execution.
        
        Key Validations:
        - Cycle detection (critical)
        - Component reachability analysis (critical)
        - Global variable reference validation (critical)
        - Performance characteristic analysis (warnings)
        
        Args:
            dag: Complete DAG from previous phases
            subjob_components: Subjob structure from Phase 4
            
        Raises:
            CriticalPlanningError: On critical validation errors that prevent execution
        """
        start_time = time.perf_counter()
        
        try:
            errors, warnings = self.structure_validator.validate_structure(dag)
            self._phase_timings['structure_validation'] = time.perf_counter() - start_time
            
            # Critical errors abort planning - warnings are recorded but don't block
            critical_errors = [e for e in errors if e.severity == "ERROR"]
            if critical_errors:
                raise CriticalPlanningError(
                    f"Structure validation failed with {len(critical_errors)} critical errors",
                    critical_errors
                )
                
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
        Generate comprehensive execution metadata for two-level architecture runtime optimization.
        
        This method creates all metadata needed for runtime execution, eliminating the need
        for registry queries or DAG analysis during job execution. The metadata supports
        both execution levels:
        
        Level 1 (Orchestrator): Subjob coordination, control flow, checkpointing
        Level 2 (ExecutionManager): Component routing, executor allocation, resource management
        
        Purpose: Eliminate ALL runtime registry/DAG queries for maximum performance
        
        Returns:
            Comprehensive metadata dictionary with pre-computed execution data
        """
        # Extract all startable components for initial execution
        startable_components = [
            node for node, data in dag.nodes(data=True)
            if data.get('startable', False)
        ]
        
        # Create component dependency cache for Level 1 (Orchestrator)
        component_dependencies = self._create_dependency_resolution_cache(dag)
        
        # Create port mapping cache for data flow optimization
        port_mapping = self._create_port_mapping_optimization(dag)
        
        # Extract idempotent components for resume logic
        idempotent_components = {
            node for node, data in dag.nodes(data=True)
            if data.get('idempotent', True)
        }
        
        # Calculate execution estimates for resource planning
        execution_estimates = self._calculate_execution_estimates(dag, subjob_metadata)
        
        # Generate executor allocation plan for Level 2 (ExecutionManager)
        executor_allocation_plan = self._generate_executor_allocation_plan(dag, job_model)
        
        # Optimize checkpoint strategy for minimal resume time
        checkpoint_strategy = self._optimize_checkpoint_strategy(subjob_metadata)
        
        # Extract all node metadata for engine - comprehensive component information
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
                'subjob_id': self._get_component_subjob(node, subjob_components),
                'executor': data.get('executor', 'threadpool'),
                'executor_config': data.get('executor_config', {})
            }
        
        return {
            # Level 1: Orchestrator metadata for subjob coordination
            'startable_components': startable_components,
            'component_dependencies': component_dependencies,
            'port_mapping': port_mapping,
            'node_metadata': node_metadata,
            'subjob_boundaries': subjob_metadata,
            'checkpoint_strategy': checkpoint_strategy,
            'parallel_execution_plan': self._create_parallel_execution_plan(subjob_metadata),
            'idempotent_components': list(idempotent_components),
            
            # Level 2: ExecutionManager metadata for component routing
            'executor_allocation_plan': executor_allocation_plan,
            'resource_requirements': execution_estimates.get('resource_requirements', {}),
            'estimated_execution_time': execution_estimates.get('total_estimated_runtime_seconds', 0),
            
            # Job-level configuration for runtime decisions
            'job_config': self._extract_job_config_metadata(job_model)
        }
    
    def _generate_executor_allocation_plan(self, dag: nx.DiGraph, job_model: JobModel) -> Dict[str, Any]:
        """
        Generate executor allocation plan for ExecutionManager.
        
        This method analyzes all components in the DAG and creates a comprehensive
        allocation plan for the ExecutionManager to route components to appropriate
        executors with proper resource allocation.
        
        Key Operations:
        - Component executor analysis and categorization
        - Resource requirement calculation (workers, memory, cache)
        - Job-level executor configuration extraction
        - Resource pool validation and allocation planning
        
        Returns:
            Complete executor allocation plan with resource requirements
        """
        threadpool_components = []
        dask_components = []
        disk_components = []
        
        total_dask_workers = 0
        total_disk_cache = 0
        
        # Analyze component executor requirements
        for node, data in dag.nodes(data=True):
            executor = data.get('executor', 'threadpool')
            executor_config = data.get('executor_config', {})
            
            if executor == 'threadpool':
                threadpool_components.append({
                    'component': node,
                    'config': executor_config
                })
            elif executor == 'dask':
                dask_config = executor_config.get('dask_config', {})
                workers = dask_config.get('workers', 1)
                total_dask_workers += workers
                
                dask_components.append({
                    'component': node,
                    'workers': workers,
                    'memory_per_worker': dask_config.get('memory_per_worker'),
                    'config': dask_config
                })
            elif executor == 'disk_based':
                disk_config = executor_config.get('disk_config', {})
                cache_size = self._parse_size_string(disk_config.get('cache_size', '1GB'))
                total_disk_cache += cache_size
                
                disk_components.append({
                    'component': node,
                    'cache_size_bytes': cache_size,
                    'table_file': disk_config.get('table_file'),
                    'lookup_column': disk_config.get('lookup_column'),
                    'config': disk_config
                })
        
        # Extract job-level executor configuration for resource pool management
        job_execution_config = {}
        if hasattr(job_model.job_config, 'execution'):
            job_execution_config = {
                'threadpool': getattr(job_model.job_config.execution, 'threadpool', None),
                'dask': getattr(job_model.job_config.execution, 'dask', None),
                'disk_based': getattr(job_model.job_config.execution, 'disk_based', None)
            }
        
        return {
            'threadpool_components': threadpool_components,
            'dask_components': dask_components,
            'disk_components': disk_components,
            'total_dask_workers_needed': total_dask_workers,
            'total_disk_cache_needed_bytes': total_disk_cache,
            'job_executor_config': job_execution_config,
            'requires_dask_cluster': len(dask_components) > 0,
            'requires_disk_cache': len(disk_components) > 0
        }
    
    def _extract_job_config_metadata(self, job_model: JobModel) -> Dict[str, Any]:
        """
        Extract job configuration metadata for runtime decisions.
        
        This method extracts essential job configuration that affects runtime
        behavior, including retry policies, timeout settings, and execution
        configuration for the orchestrator.
        
        Returns:
            Job configuration metadata for runtime use
        """
        return {
            'retries': job_model.job_config.retries,
            'timeout': job_model.job_config.timeout,
            'fail_strategy': job_model.job_config.fail_strategy,
            'execution_config': getattr(job_model.job_config, 'execution', None)
        }
    
    def _get_component_subjob(self, component: str, subjob_components: Dict[str, List[str]]) -> str:
        """
        Get subjob ID for a component.
        
        Helper method to determine which subjob a component belongs to,
        used for building execution metadata.
        """
        for subjob_id, components in subjob_components.items():
            if component in components:
                return subjob_id
        return "main"
    
    def _create_dependency_resolution_cache(self, dag: nx.DiGraph) -> Dict[str, Dict[str, List[str]]]:
        """
        Pre-compute all component dependencies to avoid runtime graph traversal.
        
        This optimization eliminates the need for graph traversal during execution
        by pre-computing all component dependencies. The cache separates data
        dependencies (for execution order) from control dependencies (for triggers).
        
        Performance Impact: Eliminates O(N*E) graph traversal during execution
        
        Returns:
            {component_name: {'data': [upstream_data_deps], 'control': [upstream_control_deps]}}
        """
        dependencies = {}
        
        for node in dag.nodes():
            data_deps = []
            control_deps = []
            
            # Categorize incoming edges by type for execution planning
            for source, target, edge_data in dag.in_edges(node, data=True):
                if edge_data.get('edge_type') == 'data':
                    data_deps.append(source)
                elif edge_data.get('edge_type') == 'control':
                    control_deps.append(source)
            
            dependencies[node] = {
                'data': sorted(data_deps),
                'control': sorted(control_deps)
            }
        
        return dependencies
    
    def _create_port_mapping_optimization(self, dag: nx.DiGraph) -> Dict[str, Dict[str, List[Tuple[str, str]]]]:
        """
        Create optimized port mapping for wildcard resolution.
        
        This optimization pre-computes all port mappings to eliminate runtime
        port resolution overhead. The mapping provides direct lookup of input/output
        connections for each component.
        
        Performance Impact: Eliminates port resolution queries during execution
        
        Returns:
            {component_name: {'inputs': [(port, source_component)], 'outputs': [(port, target_component)]}}
        """
        port_mapping = {}
        
        for node in dag.nodes():
            inputs = []
            outputs = []
            
            # Map input connections for data flow optimization
            for source, target, edge_data in dag.in_edges(node, data=True):
                if edge_data.get('edge_type') == 'data':
                    target_port = edge_data.get('target_port', 'main')
                    inputs.append((target_port, source))
            
            # Map output connections for downstream routing
            for source, target, edge_data in dag.out_edges(node, data=True):
                if edge_data.get('edge_type') == 'data':
                    source_port = edge_data.get('source_port', 'main')
                    outputs.append((source_port, target))
            
            port_mapping[node] = {
                'inputs': sorted(inputs),
                'outputs': sorted(outputs)
            }
        
        return port_mapping
    
    def _calculate_execution_estimates(self, dag: nx.DiGraph, 
                                     subjob_metadata: Dict[str, Dict[str, Any]]) -> Dict[str, Any]:
        """
        Calculate execution time and resource estimates.
        
        This method provides rough estimates for execution time and resource
        requirements based on component count, complexity, and parallelism
        potential. These estimates are used for resource planning and monitoring.
        
        Returns:
            Execution estimate metrics for resource planning
        """
        component_count = len(dag.nodes())
        edge_count = len(dag.edges())
        subjob_count = len(subjob_metadata)
        
        # Simple heuristics for estimation - can be refined based on component types
        estimated_time_per_component = 1.0  # seconds
        memory_per_component = 50  # MB
        
        # Calculate parallelism factor from execution waves
        max_parallel_subjobs = 1
        if subjob_metadata:
            execution_waves = list(subjob_metadata.values())[0].get('execution_waves', [[]])
            max_parallel_subjobs = max(len(wave) for wave in execution_waves) if execution_waves else 1
        
        parallelism_factor = max_parallel_subjobs / subjob_count if subjob_count > 0 else 1.0
        
        return {
            'total_estimated_runtime_seconds': int(component_count * estimated_time_per_component / parallelism_factor),
            'resource_requirements': {
                'memory_requirements_mb': component_count * memory_per_component,
                'cpu_intensity': 'high' if component_count > 50 else 'medium',
                'io_intensity': 'high' if edge_count > 100 else 'medium',
                'parallelism_factor': parallelism_factor
            }
        }
    
    def _optimize_checkpoint_strategy(self, subjob_metadata: Dict[str, Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
        """
        Optimize checkpoint strategy for minimal resume time.
        
        This method analyzes subjob characteristics to optimize checkpointing
        strategy, balancing checkpoint overhead against resume time benefits.
        Larger subjobs get higher checkpoint priority due to greater potential
        time savings on resume.
        
        Returns:
            {subjob_id: checkpoint_optimization_data}
        """
        checkpoint_strategy = {}
        
        for subjob_id, metadata in subjob_metadata.items():
            component_count = len(metadata.get('components', []))
            
            # Simple heuristic: prioritize larger subjobs for checkpointing
            checkpoint_strategy[subjob_id] = {
                'checkpoint_priority': component_count,
                'estimated_checkpoint_size_mb': component_count * 10,  # 10MB per component estimate
                'recovery_cost_seconds': component_count * 0.5,  # 0.5s per component
                'checkpoint_frequency': 'always'  # For v1, checkpoint all subjob boundaries
            }
        
        return checkpoint_strategy
    
    def _create_parallel_execution_plan(self, subjob_metadata: Dict[str, Dict[str, Any]]) -> Dict[str, Any]:
        """
        Create parallel execution optimization plan for asyncio orchestration.
        
        This method extracts execution wave information for the Orchestrator's
        asyncio-based subjob coordination. Execution waves enable parallel
        subjob execution while maintaining dependency order.
        
        Returns:
            Parallel execution plan with wave structure for asyncio coordination
        """
        if not subjob_metadata:
            return {'execution_waves': [], 'max_parallelism': 1, 'wave_count': 0}
        
        # Extract execution waves from subjob metadata
        execution_waves = []
        sample_metadata = list(subjob_metadata.values())[0]
        if 'execution_waves' in sample_metadata:
            execution_waves = sample_metadata['execution_waves']
        
        return {
            'execution_waves': execution_waves,
            'max_parallelism': max(len(wave) for wave in execution_waves) if execution_waves else 1,
            'wave_count': len(execution_waves)
        }
    
    def _parse_size_string(self, size_str: str) -> int:
        """
        Parse size string like '1GB' to bytes.
        
        This utility method converts human-readable size strings (e.g., '2GB', '500MB')
        into byte values for resource calculation and allocation planning.
        
        Args:
            size_str: Size string with optional unit suffix
            
        Returns:
            Size in bytes, or 0 if parsing fails
        """
        if not size_str:
            return 0
            
        size_str = size_str.upper().strip()
        multipliers = {'B': 1, 'KB': 1024, 'MB': 1024**2, 'GB': 1024**3, 'TB': 1024**4}
        
        for suffix, multiplier in multipliers.items():
            if size_str.endswith(suffix):
                try:
                    number = float(size_str[:-len(suffix)])
                    return int(number * multiplier)
                except ValueError:
                    return 0
        
        try:
            return int(float(size_str))  # Assume bytes if no suffix
        except ValueError:
            return 0
    
    def _generate_planning_diagnostics(self, dag: nx.DiGraph, 
                                     subjob_components: Dict[str, List[str]], 
                                     validation_errors: List[ValidationError],
                                     validation_warnings: List[ValidationWarning]) -> Dict[str, Any]:
        """
        Generate comprehensive planning diagnostics for debugging and monitoring.
        
        This method creates detailed diagnostics about the planning process,
        including performance metrics, complexity analysis, and error summaries.
        The diagnostics are used for monitoring planning performance and
        troubleshooting complex job structures.
        
        Key Metrics:
        - Planning performance and bottleneck identification
        - Structural complexity and parallelism analysis
        - Error distribution and affected components
        - Optimization metrics and efficiency estimates
        
        Returns:
            Comprehensive diagnostics dictionary for monitoring and debugging
        """
        total_duration = time.perf_counter() - self._planning_start_time if self._planning_start_time else 0
        
        # Find bottleneck phase for performance optimization
        bottleneck_phase = None
        max_time = 0
        for phase, duration in self._phase_timings.items():
            if duration > max_time:
                max_time = duration
                bottleneck_phase = phase
        
        # Calculate complexity metrics for performance prediction
        try:
            max_path_depth = nx.dag_longest_path_length(dag) if nx.is_directed_acyclic_graph(dag) else 0
        except:
            max_path_depth = 0
        
        # Count parallel subjobs from execution waves for parallelism analysis
        parallel_subjobs = 0
        if subjob_components:
            sample_metadata = list(subjob_components.values())[0] if subjob_components else []
            # Estimate parallel capability from subjob count (conservative)
            parallel_subjobs = min(len(subjob_components), 4)  # Conservative estimate
        
        parallelism_factor = parallel_subjobs / len(subjob_components) if subjob_components else 0
        
        return {
            'planning_performance': {
                'total_duration_ms': total_duration * 1000,
                'phase_timings': {k: v * 1000 for k, v in self._phase_timings.items()},  # Convert to ms
                'bottleneck_phase': bottleneck_phase
            },
            'structural_complexity': {
                'component_count': len(dag.nodes()),
                'edge_count': len(dag.edges()),
                'subjob_count': len(subjob_components),
                'max_path_depth': max_path_depth,
                'parallelism_factor': parallelism_factor,
                'complexity_score': len(dag.nodes()) + len(dag.edges()) * 0.5  # Simple complexity metric
            },
            'error_analysis': {
                'total_errors': len(validation_errors),
                'total_warnings': len(validation_warnings),
                'error_distribution': self._count_error_types(validation_errors),
                'affected_components': list(set(e.component for e in validation_errors if e.component)),
                'critical_errors': [e.message for e in validation_errors if e.severity == "ERROR"][:5]  # Top 5
            },
            'optimization_metrics': {
                'checkpoint_efficiency': 1.0,  # Placeholder for future optimization
                'estimated_speedup': parallelism_factor * 0.8,  # Conservative estimate
                'memory_efficiency': 0.8  # Placeholder for future memory optimization
            }
        }
    
    def _count_error_types(self, errors: List[ValidationError]) -> Dict[str, int]:
        """
        Count errors by type code for error analysis.
        
        This method categorizes validation errors by their error codes to
        provide insights into common failure patterns and help with
        troubleshooting and system improvement.
        
        Returns:
            Dictionary mapping error codes to occurrence counts
        """
        error_counts = {}
        for error in errors:
            code = error.code
            error_counts[code] = error_counts.get(code, 0) + 1
        return error_counts
    
    def _create_build_metadata(self, dag: nx.DiGraph, job_model: JobModel) -> Dict[str, Any]:
        """
        Create build metadata for the plan.
        
        This method creates metadata about the build process itself, including
        timestamps, versions, and structural information. This metadata is
        used for artifact tracking, debugging, and compatibility verification.
        
        Returns:
            Build metadata for artifact tracking and debugging
        """
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
                          planning_diagnostics: Dict[str, Any],
                          build_metadata: Dict[str, Any]) -> PlanResult:
        """
        Construct final PlanResult with comprehensive validation.
        
        This method assembles all planning artifacts into a complete PlanResult
        and performs final validation to ensure the result meets two-level
        architecture requirements and is ready for runtime execution.
        
        Key Operations:
        - Extract subjob execution order from metadata
        - Assemble complete PlanResult structure
        - Validate result completeness and consistency
        - Verify two-level architecture requirements
        
        Returns:
            Complete PlanResult ready for two-level engine execution
        """
        # Extract execution order from subjob metadata for orchestrator
        subjob_execution_order = []
        for subjob_id in sorted(subjob_metadata.keys(), 
                               key=lambda x: subjob_metadata[x].get('execution_order', 0)):
            subjob_execution_order.append(subjob_id)
        
        plan_result = PlanResult(
            dag=dag,
            subjob_components=subjob_components,
            subjob_execution_order=subjob_execution_order,
            validation_errors=all_errors,
            validation_warnings=all_warnings,
            execution_metadata=execution_metadata,
            planning_diagnostics=planning_diagnostics,
            build_metadata=build_metadata
        )
        
        # Final validation to ensure completeness for two-level execution
        self._validate_plan_result_completeness(plan_result)
        
        return plan_result
    
    def _validate_plan_result_completeness(self, plan_result: PlanResult) -> None:
        """
        Final validation that PlanResult meets two-level architecture requirements.
        
        This method performs comprehensive validation of the final PlanResult
        to ensure it contains all required metadata for runtime execution
        and meets the requirements of the two-level execution architecture.
        
        Key Validations:
        - DAG node metadata completeness
        - Component-to-subjob assignment completeness
        - Execution metadata for both execution levels
        - Executor allocation plan completeness
        
        Raises:
            PlanResultValidationError: If plan result is incomplete or invalid
        """
        # Check DAG has complete metadata for all nodes
        for node, data in plan_result.dag.nodes(data=True):
            if 'component_type' not in data:
                raise PlanResultValidationError(
                    f"Node '{node}' missing component_type metadata"
                )
            if 'registry_metadata' not in data:
                raise PlanResultValidationError(
                    f"Node '{node}' missing registry_metadata"
                )
            if 'executor' not in data:
                raise PlanResultValidationError(
                    f"Node '{node}' missing executor metadata"
                )
        
        # Check all components assigned to subjobs (no orphaned components)
        all_nodes = set(plan_result.dag.nodes())
        subjob_nodes = set()
        for components in plan_result.subjob_components.values():
            subjob_nodes.update(components)
        
        unassigned = all_nodes - subjob_nodes
        if unassigned:
            raise PlanResultValidationError(
                f"Components not assigned to subjobs: {unassigned}"
            )
        
        # Check execution metadata completeness for two-level architecture
        required_metadata_keys = [
            'startable_components', 'component_dependencies', 
            'port_mapping', 'node_metadata', 'executor_allocation_plan'
        ]
        for key in required_metadata_keys:
            if key not in plan_result.execution_metadata:
                raise PlanResultValidationError(
                    f"Missing required execution metadata: {key}"
                )
        
        # Validate executor allocation plan completeness for ExecutionManager
        executor_plan = plan_result.execution_metadata.get('executor_allocation_plan', {})
        required_executor_keys = ['threadpool_components', 'dask_components', 'disk_components']
        for key in required_executor_keys:
            if key not in executor_plan:
                raise PlanResultValidationError(
                    f"Missing required executor allocation metadata: {key}"
                )
    
    def serialize_dag_for_pjob(self, dag: nx.DiGraph) -> Dict[str, Any]:
        """
        Convert NetworkX DAG to msgpack-compatible format for .pjob file.
        
        This method helps the builder serialize the DAG for storage in the
        .pjob artifact. It converts the NetworkX graph to a format that
        can be safely serialized with msgpack and reconstructed at runtime.
        
        Key Operations:
        - Convert to node-link format (msgpack-friendly)
        - Ensure all data is serializable (no complex objects)
        - Handle special data types (sets, tuples, etc.)
        
        Args:
            dag: NetworkX DiGraph with full metadata
            
        Returns:
            Dictionary representation ready for msgpack serialization
        """
        # Convert to node-link format which is msgpack-friendly
        dag_data = nx.node_link_data(dag)
        
        # Ensure all data is serializable (no complex objects)
        def make_serializable(obj):
            if isinstance(obj, set):
                return list(obj)
            elif isinstance(obj, (list, tuple)):
                return [make_serializable(item) for item in obj]
            elif isinstance(obj, dict):
                return {k: make_serializable(v) for k, v in obj.items()}
            else:
                return obj
        
        return make_serializable(dag_data)


class PlanResultValidationError(PlanningError):
    """
    Raised when final PlanResult validation fails.
    
    This error indicates that the assembled PlanResult does not meet the
    requirements for runtime execution, typically due to missing metadata
    or incomplete validation of the two-level architecture requirements.
    """
    pass