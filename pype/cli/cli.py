import click
from pathlib import Path
from pype.cli.build import build_command
from pype.cli.registry import (
    register_component_command,
    list_components_command, 
    component_info_command,
    delete_component_command
)
from pype.cli.component_creator import (
    create_component,
    component_template,
    component_examples
)


@click.group()
@click.version_option(version="0.1.0", prog_name="pype")
@click.help_option("--help", "-h")
def cli():
    """DataPY ETL Framework - CLI-first, YAML-driven ETL pipeline engine."""
    pass


# Build commands
cli.add_command(build_command, name="build")

@cli.command()
@click.argument("pjob_file", type=click.Path(exists=True))
@click.option("--extract", type=click.Path(), help="Extract contents to directory")
@click.option("--show-yaml", is_flag=True, help="Show original YAML content")
@click.option("--show-dag", is_flag=True, help="Show detailed DAG information")
def inspect(pjob_file, extract, show_yaml, show_dag):
    """Inspect contents of a .pjob file."""
    try:
        from pype.cli.inspect_pjob import inspect_pjob_cli
        inspect_pjob_cli(pjob_file, extract, show_yaml, show_dag)
    except Exception as e:
        click.echo(f"Inspection failed: {e}")
        raise click.Abort()

# Registry commands  
cli.add_command(register_component_command, name="register-component")
cli.add_command(register_component_command, name="register")  # Shorter alias
cli.add_command(list_components_command, name="list-components")
cli.add_command(list_components_command, name="list")  # Shorter alias
cli.add_command(component_info_command, name="component-info")
cli.add_command(component_info_command, name="info")  # Shorter alias
cli.add_command(delete_component_command, name="delete-component")

# Component creation commands
cli.add_command(create_component, name="create-component")
cli.add_command(component_template, name="component-template")
cli.add_command(component_examples, name="component-examples")

# Placeholder commands for future phases
@cli.command()
@click.argument("job_file", type=click.Path(exists=True))
@click.option("--context", type=click.Path(exists=True), help="Context JSON file")
@click.option("--resume", is_flag=True, help="Resume from last checkpoint")
def run(job_file, context, resume):
    """Run a .pjob file (Phase 2 - Not implemented yet)."""
    click.echo("Run command will be implemented in Phase 2")


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