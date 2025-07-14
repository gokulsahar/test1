import click
from pathlib import Path
from pype.cli.build import build_command
from pype.cli.setup import setup_command
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


# Setup command
cli.add_command(setup_command, name="setup")

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

# Component creation commands
cli.add_command(create_component, name="create-component")
cli.add_command(component_template, name="component-template")
cli.add_command(component_examples, name="component-examples")

# Placeholder commands for future phases
@cli.command()
@click.argument("job_folder", type=click.Path(exists=True))
@click.option("--context", help="Context name (e.g., 'dev', 'prod') or defaults to job_name_context")
@click.option("--resume", is_flag=True, help="Resume from last checkpoint")
def run(job_folder, context, resume):
    """Run a job folder with runtime template resolution (Phase 2 - Not implemented yet)."""
    click.echo("Run command will be implemented in Phase 2")
    click.echo(f"Would execute job from: {job_folder}")
    if context:
        click.echo(f"Using context: {context}")
    else:
        job_name = Path(job_folder).name
        click.echo(f"Using default context: {job_name}_context.json")


@cli.command()
@click.argument("job_name")
@click.option("--template", type=click.Choice(["basic", "etl", "ml"]), default="basic")
@click.option("--output", type=click.Path(), help="Output directory")
def create(job_name, template, output):
    """Create new job from template (Phase 6 - Not implemented yet)."""
    click.echo("Create command will be implemented in Phase 6")


@cli.command()
@click.argument("job_folder", type=click.Path(exists=True))
@click.argument("output_file", type=click.Path())
def pack(job_folder, output_file):
    """Package job folder for distribution (Phase 6 - Not implemented yet)."""
    click.echo("Pack command will be implemented in Phase 6")


if __name__ == "__main__":
    cli()