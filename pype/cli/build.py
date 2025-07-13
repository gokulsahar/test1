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
    PJOB_EXTENSION, ORIGINAL_YAML_FILE, 
    DAG_FILE, ASSETS_DIR, ENGINE_VERSION, FRAMEWORK_NAME,DEFAULT_ENCODING
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
            with open(context, 'r', encoding=DEFAULT_ENCODING) as f:
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
    """Create the .pjob ZIP file with envelope format per v1.1 specification."""
    
    # Preserve original YAML filename inside ZIP
    preserved_yaml_name = original_yaml.name
    
    with zipfile.ZipFile(pjob_file, 'w', zipfile.ZIP_DEFLATED) as zf:
        # 1. Add original YAML file with preserved filename
        zf.write(original_yaml, preserved_yaml_name)
        
        # 2. Add serialized DAG using centralized planner method
        from pype.core.registry.component_registry import ComponentRegistry
        from pype.core.planner.planner import JobPlanner
        
        # Use a planner instance for consistent serialization
        registry = ComponentRegistry()
        planner = JobPlanner(registry)
        dag_data = planner.serialize_dag_for_pjob(plan_result.dag)
        dag_msgpack = msgpack.packb(dag_data)
        zf.writestr(DAG_FILE, dag_msgpack)
        
        # 3. Add execution metadata
        execution_data = msgpack.packb(plan_result.execution_metadata)
        zf.writestr("execution_metadata.msgpack", execution_data)
        
        # 4. Add subjob metadata
        subjob_data = {
            'components': plan_result.subjob_components,
            'execution_order': plan_result.subjob_execution_order
        }
        subjob_msgpack = msgpack.packb(subjob_data)
        zf.writestr("subjob_metadata.msgpack", subjob_msgpack)
        
        # 5. Create assets directory (empty for now)
        zf.writestr(f"{ASSETS_DIR}/", "")
        
        # 6. Create manifest metadata
        manifest_data = _create_manifest_metadata(job_model, plan_result)
        
        # 7. Create manifest.json envelope (v1.1 specification format)
        envelope = {
            "format": "datapy-pjob@1",
            "yaml": preserved_yaml_name,
            "dag_msgpack": DAG_FILE,
            "manifest": manifest_data
        }
        zf.writestr("manifest.json", json.dumps(envelope, indent=2))


def _create_manifest_metadata(job_model, plan_result) -> Dict[str, Any]:
    """Create manifest metadata for the .pjob envelope."""
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
        "job_starters": plan_result.execution_metadata.get("job_starters", []),
        "estimated_runtime_seconds": plan_result.execution_metadata.get("estimated_execution_time", 0),
        "checksums": {
            "dag": "sha256_placeholder",  # TODO: Implement in Phase 5
            "execution_metadata": "sha256_placeholder",
            "subjob_metadata": "sha256_placeholder"
        }
    }


def _show_build_summary(plan_result, verbose_level: int, show_enhanced: bool = False):
    """Show build summary based on verbosity level."""
    
    if verbose_level >= 2:
        header = "Build Summary" if show_enhanced else "=== Build Summary ==="
        separator = "=" * 40
        
        click.echo(f"\n{header}")
        click.echo(separator)
        
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