import click
from pathlib import Path
from pype.cli.build import build_command
from pype.cli.registry import (
    register_component_command,
    list_components_command, 
    component_info_command,
    delete_component_command
)


@click.group()
@click.version_option(version="0.1.0", prog_name="pype")
@click.help_option("--help", "-h")
def cli():
    """DataPY ETL Framework - CLI-first, YAML-driven ETL pipeline engine."""
    pass


# Build commands
cli.add_command(build_command, name="build")

# Registry commands  
cli.add_command(register_component_command, name="register-component")
cli.add_command(register_component_command, name="register")  # Shorter alias
cli.add_command(list_components_command, name="list-components")
cli.add_command(list_components_command, name="list")  # Shorter alias
cli.add_command(component_info_command, name="component-info")
cli.add_command(component_info_command, name="info")  # Shorter alias
cli.add_command(delete_component_command, name="delete-component")


# Placeholder commands for future phases
@cli.command()
@click.argument("job_file", type=click.Path(exists=True))
@click.option("--context", type=click.Path(exists=True), help="Context JSON file")
@click.option("--resume", is_flag=True, help="Resume from last checkpoint")
def run(job_file, context, resume):
    """Run a .pjob file (Phase 2 - Not implemented yet)."""
    click.echo("Run command will be implemented in Phase 2")


@cli.command()
@click.argument("job_file", type=click.Path(exists=True))
@click.option("--format", type=click.Choice(["png", "svg", "pdf", "dot"]), default="png")
@click.option("--output", type=click.Path(), help="Output file path")
@click.option("--no-view", is_flag=True, help="Don't open the visualization file")
@click.option("--context", type=click.Path(exists=True), help="Context JSON file")
def visualize(job_file, format, output, no_view, context):
    """Create a visual graph of the job DAG using Graphviz."""
    try:
        from pype.core.loader.loader import load_job_yaml, LoaderError
        from pype.core.registry.component_registry import ComponentRegistry
        from pype.core.planner.planner import JobPlanner, CriticalPlanningError, PlanningPhaseError
        from pype.cli.graph_visualizer import visualize_dag, create_legend
        import json
        
        # Load context if provided
        context_data = {}
        if context:
            with open(context, 'r') as f:
                context_data = json.load(f)
        
        click.echo(f"Loading and planning job: {job_file}")
        
        # Load and plan the job
        job_model = load_job_yaml(Path(job_file), context_data)
        registry = ComponentRegistry()
        planner = JobPlanner(registry)
        plan_result = planner.plan_job(job_model)
        
        # Create visualization
        click.echo("Creating visualization...")
        viz_path = visualize_dag(
            plan_result.dag, 
            output_path=Path(output) if output else None,
            format=format,
            view=not no_view
        )
        
        click.echo(f"  Visualization created: {viz_path}")
        click.echo(f"  Components: {len(plan_result.dag.nodes())}")
        click.echo(f"  Connections: {len(plan_result.dag.edges())}")
        click.echo(f"  Subjobs: {len(plan_result.subjob_components)}")
        
        # Show legend
        if not no_view:
            click.echo(create_legend())
            
    except ImportError as e:
        click.echo(f"Error: {e}")
        click.echo("install graphviz")
        raise click.Abort()
    except (LoaderError, CriticalPlanningError, PlanningPhaseError) as e:
        click.echo(f"Job processing failed: {e}")
        raise click.Abort()
    except Exception as e:
        click.echo(f"Visualization failed: {e}")
        raise click.Abort()


@cli.command()
@click.argument("job_name")
@click.option("--template", type=click.Choice(["basic", "etl", "ml"]), default="basic")
@click.option("--output", type=click.Path(), help="Output directory")
def create(job_name, template, output):
    """Create new job from template (Phase 6 - Not implemented yet)."""
    click.echo("Create command will be implemented in Phase 6")


@cli.command()
@click.argument("input_dir", type=click.Path(exists=True))
@click.argument("output_file", type=click.Path())
def pack(input_dir, output_file):
    """Package job assets for distribution (Phase 6 - Not implemented yet)."""
    click.echo("Pack command will be implemented in Phase 6")


if __name__ == "__main__":
    cli()