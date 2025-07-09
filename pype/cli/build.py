import json
import time
import zipfile
import msgpack
from datetime import datetime
from pathlib import Path
from typing import Dict, Any, Optional

import click
import networkx as nx

from pype.core.loader.loader import load_job_yaml, LoaderError
from pype.core.registry.component_registry import ComponentRegistry
from pype.core.planner.planner import JobPlanner, CriticalPlanningError, PlanningPhaseError
from pype.core.utils.constants import (
    PJOB_EXTENSION, MANIFEST_FILE, ORIGINAL_YAML_FILE, 
    DAG_FILE, ASSETS_DIR, ENGINE_VERSION, FRAMEWORK_NAME
)


@click.command()
@click.argument("job_file", type=click.Path(exists=True, path_type=Path))
@click.option("--output", "-o", type=click.Path(path_type=Path), 
              help="Output directory (default: ./jobs/)")
@click.option("--context", type=click.Path(exists=True, path_type=Path),
              help="Context variables JSON file")
@click.option("--verbose", "-v", count=True,
              help="Verbosity level: -v (basic), -vv (detailed), -vvv (full diagnostics)")
@click.option("--force", "-f", is_flag=True,
              help="Overwrite existing .pjob file if it exists")
def build_command(job_file: Path, output: Optional[Path], context: Optional[Path], 
                 verbose: int, force: bool):
    """Build a .pjob file from YAML job definition."""
    
    try:
        # Load context if provided
        context_data = {}
        if context:
            with open(context, 'r') as f:
                context_data = json.load(f)
            if verbose >= 1:
                click.echo(f"Loaded context from {context}")
        
        # Determine output directory
        if output is None:
            output = Path.cwd() / "jobs"
        output.mkdir(parents=True, exist_ok=True)
        
        # Generate output filename
        pjob_file = output / f"{job_file.stem}{PJOB_EXTENSION}"
        
        # Check if file exists and force not specified
        if pjob_file.exists() and not force:
            click.echo(f"Error: {pjob_file} already exists. Use --force to overwrite.")
            raise click.Abort()
        
        if verbose >= 1:
            click.echo(f"Building {job_file} -> {pjob_file}")
        
        # Start build process
        build_start = time.perf_counter()
        
        # Step 1: Load and validate YAML
        if verbose >= 2:
            click.echo("Step 1: Loading and validating YAML...")
        
        try:
            job_model = load_job_yaml(job_file, context_data)
        except LoaderError as e:
            click.echo(f"YAML validation failed: {e}")
            if e.errors:
                for error in e.errors:
                    click.echo(f"  - {error}")
            raise click.Abort()
        
        if verbose >= 2:
            click.echo(f"   Loaded job '{job_model.job.name}' v{job_model.job.version}")
            click.echo(f"   Found {len(job_model.components)} components")
        
        # Step 2: Initialize registry and planner
        if verbose >= 2:
            click.echo("Step 2: Initializing planner...")
        
        registry = ComponentRegistry()
        planner = JobPlanner(registry)
        
        # Step 3: Plan job execution
        if verbose >= 2:
            click.echo("Step 3: Planning job execution...")
        
        try:
            plan_result = planner.plan_job(job_model)
        except (CriticalPlanningError, PlanningPhaseError) as e:
            click.echo(f"Planning failed: {e}")
            if hasattr(e, 'errors'):
                for error in e.errors:
                    click.echo(f"  - {error.message}")
            raise click.Abort()
        
        if verbose >= 2:
            click.echo(f"   Created DAG with {len(plan_result.dag.nodes())} nodes")
            click.echo(f"   Detected {len(plan_result.subjob_components)} subjobs")
            if plan_result.validation_warnings:
                click.echo(f"   {len(plan_result.validation_warnings)} warnings")
        
        # Step 4: Create .pjob file
        if verbose >= 2:
            click.echo("Step 4: Creating .pjob package...")
        
        _create_pjob_file(pjob_file, job_file, job_model, plan_result)
        
        # Build complete
        build_duration = time.perf_counter() - build_start
        
        if verbose >= 1:
            click.echo(f" Build completed in {build_duration:.2f}s")
            click.echo(f"  Output: {pjob_file}")
            
            # Show summary
            if verbose >= 2:
                _show_build_summary(plan_result, verbose)
        else:
            click.echo(f"Built: {pjob_file}")
            
    except click.Abort:
        raise
    except Exception as e:
        click.echo(f"Unexpected error: {e}")
        if verbose >= 3:
            import traceback
            traceback.print_exc()
        raise click.Abort()


def _create_pjob_file(pjob_file: Path, original_yaml: Path, job_model, plan_result):
    """Create the .pjob ZIP file with all required components."""
    
    with zipfile.ZipFile(pjob_file, 'w', zipfile.ZIP_DEFLATED) as zf:
        # 1. Add manifest.json
        manifest = _create_manifest(job_model, plan_result)
        zf.writestr(MANIFEST_FILE, json.dumps(manifest, indent=2))
        
        # 2. Add original YAML file
        zf.write(original_yaml, ORIGINAL_YAML_FILE)
        
        # 3. Add serialized DAG
        dag_data = _serialize_dag(plan_result.dag)
        dag_msgpack = msgpack.packb(dag_data)
        zf.writestr(DAG_FILE, dag_msgpack)
        
        # 4. Add execution metadata
        execution_data = msgpack.packb(plan_result.execution_metadata)
        zf.writestr("execution_metadata.msgpack", execution_data)
        
        # 5. Add subjob metadata
        subjob_data = {
            'components': plan_result.subjob_components,
            'execution_order': plan_result.subjob_execution_order
        }
        subjob_msgpack = msgpack.packb(subjob_data)
        zf.writestr("subjob_metadata.msgpack", subjob_msgpack)
        
        # 6. Create assets directory (empty for now)
        zf.writestr(f"{ASSETS_DIR}/", "")


def _create_manifest(job_model, plan_result) -> Dict[str, Any]:
    """Create manifest.json for the .pjob file."""
    return {
        "job_id": f"{job_model.job.name}_{job_model.job.version}",
        "job_name": job_model.job.name,
        "job_version": job_model.job.version,
        "build_timestamp": datetime.now().isoformat(),
        "engine_version": ENGINE_VERSION,
        "framework": FRAMEWORK_NAME,
        "component_count": len(plan_result.dag.nodes()),
        "edge_count": len(plan_result.dag.edges()),
        "subjob_count": len(plan_result.subjob_components),
        "data_edge_count": plan_result.build_metadata.get("data_edge_count", 0),
        "control_edge_count": plan_result.build_metadata.get("control_edge_count", 0),
        "startable_components": plan_result.execution_metadata.get("startable_components", []),
        "estimated_runtime_seconds": plan_result.execution_metadata.get("estimated_execution_time", 0),
        "checksums": {
            "dag": "sha256_placeholder",  # TODO: Implement in Phase 5
            "execution_metadata": "sha256_placeholder",
            "subjob_metadata": "sha256_placeholder"
        }
    }


def _serialize_dag(dag: nx.DiGraph) -> Dict[str, Any]:
    """Serialize NetworkX DAG to msgpack-compatible format."""
    # Convert to node-link format
    dag_data = nx.node_link_data(dag)
    
    # Ensure all data is serializable
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


def _show_build_summary(plan_result, verbose_level: int):
    """Show build summary based on verbosity level."""
    
    if verbose_level >= 2:
        click.echo("\n=== Build Summary ===")
        
        # Basic stats
        click.echo(f"Components: {len(plan_result.dag.nodes())}")
        click.echo(f"Connections: {len(plan_result.dag.edges())}")
        click.echo(f"Subjobs: {len(plan_result.subjob_components)}")
        
        # Subjob breakdown
        if plan_result.subjob_components:
            click.echo("\nSubjob Structure:")
            for subjob_id in plan_result.subjob_execution_order:
                components = plan_result.subjob_components[subjob_id]
                click.echo(f"  {subjob_id}: {len(components)} components")
        
        # Warnings
        if plan_result.validation_warnings:
            click.echo(f"\nWarnings ({len(plan_result.validation_warnings)}):")
            for warning in plan_result.validation_warnings[:5]:  # Show first 5
                click.echo(f"   {warning.message}")
            if len(plan_result.validation_warnings) > 5:
                click.echo(f"  ... and {len(plan_result.validation_warnings) - 5} more")
    
    if verbose_level >= 3:
        click.echo("\n=== Planning Diagnostics ===")
        diagnostics = plan_result.planning_diagnostics
        
        # Performance
        perf = diagnostics.get('planning_performance', {})
        click.echo(f"Planning time: {perf.get('total_duration_ms', 0):.1f}ms")
        if 'bottleneck_phase' in perf:
            click.echo(f"Bottleneck phase: {perf['bottleneck_phase']}")
        
        # Complexity
        complexity = diagnostics.get('structural_complexity', {})
        click.echo(f"Complexity score: {complexity.get('complexity_score', 0):.1f}")
        click.echo(f"Max path depth: {complexity.get('max_path_depth', 0)}")
        click.echo(f"Parallelism factor: {complexity.get('parallelism_factor', 0):.2f}")
        
        # Execution estimates
        exec_meta = plan_result.execution_metadata
        click.echo(f"Estimated runtime: {exec_meta.get('estimated_execution_time', 0)}s")
        resources = exec_meta.get('resource_requirements', {})
        click.echo(f"Memory estimate: {resources.get('memory_requirements_mb', 0)}MB")