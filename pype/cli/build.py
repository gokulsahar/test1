import json
import time
from datetime import datetime
from pathlib import Path
from typing import Dict, Any, Optional

import click
import networkx as nx

from pype.core.loader.loader import load_job_yaml, LoaderError
from pype.core.registry.component_registry import ComponentRegistry
from pype.core.planner.planner import JobPlanner, CriticalPlanningError, PlanningPhaseError
from pype.core.utils.constants import (
    DAG_FILE_SUFFIX, MANIFEST_FILE_SUFFIX, EXECUTION_METADATA_SUFFIX,
    SUBJOB_METADATA_SUFFIX, ASSETS_DIR, ENGINE_VERSION, FRAMEWORK_NAME, DEFAULT_ENCODING
)


@click.command()
@click.argument("job_file", type=click.Path(exists=True, path_type=Path))
@click.option("--output", "-o", type=click.Path(path_type=Path), 
              help="Base output directory (default: ./jobs/)")
@click.option("--context", type=click.Path(exists=True, path_type=Path),
              help="Context variables JSON file")
@click.option("--verbose", "-v", count=True,
              help="Verbosity level: -v (basic), -vv (detailed), -vvv (full diagnostics)")
@click.option("--force", "-f", is_flag=True,
              help="Overwrite existing job folder if it exists")
def build_command(job_file: Path, output: Optional[Path], context: Optional[Path], 
                 verbose: int, force: bool):
    """Build a job folder with JSON artifacts from YAML job definition."""
    
    try:
        # Load context if provided
        context_data = {}
        if context:
            with open(context, 'r', encoding=DEFAULT_ENCODING) as f:
                context_data = json.load(f)
            if verbose >= 1:
                click.echo(f"Loaded context from {context}")
        
        # Determine base output directory
        if output is None:
            output = Path.cwd() / "jobs"
        output.mkdir(parents=True, exist_ok=True)
        
        if verbose >= 1:
            click.echo(f"Building {job_file}...")
        
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
        
        # Generate job folder path using job name from YAML
        job_folder = output / job_model.job.name
        
        # Check if folder exists and force not specified
        if job_folder.exists() and not force:
            click.echo(f"Error: {job_folder} already exists. Use --force to overwrite.")
            raise click.Abort()
        
        if verbose >= 2:
            click.echo(f"   Loaded job '{job_model.job.name}' v{job_model.job.version}")
            click.echo(f"   Found {len(job_model.components)} components")
            click.echo(f"   Target: {job_folder}")
        
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
        
        # Step 4: Create job folder
        if verbose >= 2:
            click.echo("Step 4: Creating job folder...")
        
        _create_job_folder(job_folder, job_file, job_model, plan_result, force)
        
        # Build complete
        build_duration = time.perf_counter() - build_start
        
        if verbose >= 1:
            click.echo(f" Build completed in {build_duration:.2f}s")
            click.echo(f"  Output: {job_folder}")
            
            # Show summary
            if verbose >= 2:
                _show_build_summary(plan_result, verbose)
        else:
            click.echo(f"Built: {job_folder}")
            
    except click.Abort:
        raise
    except Exception as e:
        click.echo(f"Unexpected error: {e}")
        if verbose >= 3:
            import traceback
            traceback.print_exc()
        raise click.Abort()


def _create_job_folder(job_folder: Path, original_yaml: Path, job_model, plan_result, force: bool):
    """Create the job folder with JSON artifacts."""
    
    # Create/clear job folder
    if job_folder.exists() and force:
        import shutil
        shutil.rmtree(job_folder)
    job_folder.mkdir(parents=True, exist_ok=True)
    
    # Get job name for file prefixes
    job_name = job_model.job.name
    
    # 1. Copy original YAML file (preserve original filename)
    original_yaml_dest = job_folder / original_yaml.name
    import shutil
    shutil.copy2(original_yaml, original_yaml_dest)
    
    # 2. Create DAG JSON file
    dag_data = _serialize_dag_for_json(plan_result.dag)
    dag_file = job_folder / f"{job_name}{DAG_FILE_SUFFIX}"
    with open(dag_file, 'w', encoding=DEFAULT_ENCODING) as f:
        json.dump(dag_data, f, indent=2, default=str)
    
    # 3. Create execution metadata JSON
    execution_file = job_folder / f"{job_name}{EXECUTION_METADATA_SUFFIX}"
    with open(execution_file, 'w', encoding=DEFAULT_ENCODING) as f:
        json.dump(plan_result.execution_metadata, f, indent=2, default=str)
    
    # 4. Create subjob metadata JSON
    subjob_data = {
        'components': plan_result.subjob_components,
        'execution_order': plan_result.subjob_execution_order
    }
    subjob_file = job_folder / f"{job_name}{SUBJOB_METADATA_SUFFIX}"
    with open(subjob_file, 'w', encoding=DEFAULT_ENCODING) as f:
        json.dump(subjob_data, f, indent=2, default=str)
    
    # 5. Create assets directory
    assets_dir = job_folder / ASSETS_DIR
    assets_dir.mkdir(exist_ok=True)
    
    # 6. Create manifest JSON
    manifest_data = _create_manifest_metadata(job_model, plan_result, original_yaml.name)
    manifest_file = job_folder / f"{job_name}{MANIFEST_FILE_SUFFIX}"
    with open(manifest_file, 'w', encoding=DEFAULT_ENCODING) as f:
        json.dump(manifest_data, f, indent=2, default=str)


def _serialize_dag_for_json(dag: nx.DiGraph) -> Dict[str, Any]:
    """Convert NetworkX DAG to JSON-compatible format."""
    # Use NetworkX's node-link format which is JSON serializable
    return nx.node_link_data(dag)


def _create_manifest_metadata(job_model, plan_result, original_yaml_name: str) -> Dict[str, Any]:
    """Create manifest metadata for the job folder."""
    job_name = job_model.job.name
    
    return {
        "format": "datapy-job@2",  # New version for folder format
        "job_id": f"{job_model.job.name}_{job_model.job.version}",
        "job_name": job_model.job.name,
        "job_version": job_model.job.version,
        "build_timestamp": datetime.now().isoformat(),
        "engine_version": ENGINE_VERSION,
        "framework": FRAMEWORK_NAME,
        
        # File references with new naming convention
        "files": {
            "original_yaml": original_yaml_name,
            "dag": f"{job_name}{DAG_FILE_SUFFIX}",
            "execution_metadata": f"{job_name}{EXECUTION_METADATA_SUFFIX}",
            "subjob_metadata": f"{job_name}{SUBJOB_METADATA_SUFFIX}",
            "manifest": f"{job_name}{MANIFEST_FILE_SUFFIX}"
        },
        
        # Job statistics
        "statistics": {
            "component_count": len(plan_result.dag.nodes()),
            "edge_count": len(plan_result.dag.edges()),
            "subjob_count": len(plan_result.subjob_components),
            "data_edge_count": plan_result.build_metadata.get("data_edge_count", 0),
            "control_edge_count": plan_result.build_metadata.get("control_edge_count", 0),
            "job_starters": plan_result.execution_metadata.get("job_starters", []),
            "estimated_runtime_seconds": plan_result.execution_metadata.get("estimated_execution_time", 0)
        }
    }


def _show_build_summary(plan_result, verbose_level: int):
    """Show build summary based on verbosity level."""
    
    if verbose_level >= 2:
        click.echo("\n=== Build Summary ===")
        click.echo("=" * 40)
        
        # Basic stats
        click.echo(f"Components: {len(plan_result.dag.nodes())}")
        click.echo(f"Connections: {len(plan_result.dag.edges())}")
        click.echo(f"Subjobs: {len(plan_result.subjob_components)}")
        
        # Subjob breakdown with execution order
        if plan_result.subjob_components:
            click.echo("\nSubjob Structure:")
            for subjob_id in plan_result.subjob_execution_order:
                components = plan_result.subjob_components[subjob_id]
                click.echo(f"  {subjob_id}: {len(components)} components")
        
        # Dependency tokens summary
        dependency_tokens = plan_result.execution_metadata.get('dependency_tokens', {})
        if dependency_tokens:
            click.echo("\nDependency Tokens:")
            for subjob_id, tokens in dependency_tokens.items():
                if tokens:  # Only show subjobs with dependencies
                    click.echo(f"  {subjob_id}: waits for {len(tokens)} tokens")
        
        # Warnings
        if plan_result.validation_warnings:
            click.echo(f"\nWarnings ({len(plan_result.validation_warnings)}):")
            for warning in plan_result.validation_warnings[:5]:  # Show first 5
                click.echo(f"   {warning.message}")
            if len(plan_result.validation_warnings) > 5:
                remaining = len(plan_result.validation_warnings) - 5
                click.echo(f"  ... and {remaining} more")
    
    if verbose_level >= 3:
        click.echo("\n=== Job Execution Details ===")
        
        # Job starters
        job_starters = plan_result.execution_metadata.get("job_starters", [])
        click.echo(f"Job starters: {', '.join(job_starters)}")
        
        # Execution estimates (if available)
        exec_meta = plan_result.execution_metadata
        estimated_time = exec_meta.get("estimated_execution_time", 0)
        if estimated_time > 0:
            click.echo(f"Estimated runtime: {estimated_time}s")
        
        # Job configuration
        job_config = exec_meta.get("job_config", {})
        execution_mode = job_config.get("execution_mode", "pandas")
        click.echo(f"Execution mode: {execution_mode}")
        
        if execution_mode == "dask":
            chunk_size = job_config.get("chunk_size", "200MB")
            click.echo(f"Chunk size: {chunk_size}")