import click
import os
import sys
from pathlib import Path
from typing import List, Tuple
from pype.core.registry.component_registry import ComponentRegistry
from pype.core.registry.joblet_registry import JobletRegistry


@click.command()
@click.option("--force", is_flag=True, 
              help="Force re-initialization even if already set up")
@click.option("--status", is_flag=True,
              help="Show current setup status without making changes")
@click.option("--verbose", "-v", is_flag=True,
              help="Show detailed setup progress")
def setup_command(force: bool, status: bool, verbose: bool):
    """Initialize DataPY workspace and check system readiness."""
    
    if status:
        _show_setup_status()
        return
    
    click.echo("DataPY Setup & Initialization")
    click.echo("=" * 50)
    
    # Check if already set up
    if not force and _is_already_setup():
        click.echo("DataPY appears to be already set up")
        click.echo("  Use --force to re-initialize or --status to check details")
        click.echo("  Run 'pype setup --status' for current status")
        return
    
    setup_steps = [
        ("Checking dependencies", _check_dependencies),
        ("Initializing database", _initialize_database),
        ("Creating default directories", _create_directories),
        ("Registering built-in components", _register_components),
        ("Validating setup", _validate_setup)
    ]
    
    success_count = 0
    
    for step_name, step_func in setup_steps:
        if verbose:
            click.echo(f"\n{step_name}...")
        else:
            click.echo(f"{step_name}...", nl=False)
        
        try:
            result = step_func(verbose)
            if result:
                if not verbose:
                    click.echo(" OK")
                success_count += 1
            else:
                if not verbose:
                    click.echo(" WARNING")
                click.echo(f"  Warning: {step_name} completed with issues")
        except Exception as e:
            if not verbose:
                click.echo(" ERROR")
            click.echo(f"  Error in {step_name}: {e}")
            if not click.confirm("Continue with remaining setup steps?"):
                raise click.Abort()
    
    # Show results
    click.echo()
    if success_count == len(setup_steps):
        click.echo("DataPY setup completed successfully!")
    else:
        click.echo(f"Setup completed with {len(setup_steps) - success_count} warnings")
    
    # Show available commands
    _show_available_commands()
    
    # Show next steps
    _show_next_steps()


def _is_already_setup() -> bool:
    """Check if DataPY is already set up."""
    try:
        # Check if database exists and has components
        registry = ComponentRegistry()
        components = registry.list_components()
        return len(components) > 0
    except:
        return False


def _show_setup_status():
    """Show current setup status."""
    click.echo("DataPY Setup Status")
    click.echo("=" * 30)
    
    # Check database
    try:
        registry = ComponentRegistry()
        components = registry.list_components()
        click.echo(f"[OK] Database: Ready ({len(components)} components registered)")
    except Exception as e:
        click.echo(f"[ERROR] Database: Not ready ({e})")
    
    # Check joblet registry
    try:
        joblet_registry = JobletRegistry()
        joblets = joblet_registry.list_joblets()
        click.echo(f"[OK] Joblet Registry: Ready ({len(joblets)} joblets)")
    except Exception as e:
        click.echo(f"[ERROR] Joblet Registry: Not ready ({e})")
    
    # Check dependencies
    missing_deps = _get_missing_dependencies()
    if not missing_deps:
        click.echo("[OK] Dependencies: All required packages present")
    else:
        click.echo(f"[WARNING] Dependencies: {len(missing_deps)} missing packages")
        for dep in missing_deps[:3]:  # Show first 3
            click.echo(f"    - {dep}")
        if len(missing_deps) > 3:
            click.echo(f"    ... and {len(missing_deps) - 3} more")
    
    # Check directories
    default_dirs = ["jobs", "logs", "checkpoints"]
    existing_dirs = [d for d in default_dirs if Path(d).exists()]
    click.echo(f"[OK] Directories: {len(existing_dirs)}/{len(default_dirs)} default directories exist")
    
    # Check CLI access
    try:
        import pype
        click.echo(f"[OK] CLI: pype command available (v{getattr(pype, '__version__', 'unknown')})")
    except:
        click.echo("[WARNING] CLI: pype command may not be properly installed")


def _check_dependencies(verbose: bool = False) -> bool:
    """Check for required dependencies."""
    missing = _get_missing_dependencies()
    
    if verbose:
        if not missing:
            click.echo("  All required packages are present")
        else:
            click.echo(f"  {len(missing)} packages missing:")
            for dep in missing:
                click.echo(f"    - {dep}")
            click.echo("  Install missing packages with:")
            click.echo("    pip install " + " ".join(missing))
    
    return len(missing) == 0


def _get_missing_dependencies() -> List[str]:
    """Get list of missing required dependencies."""
    required_packages = [
        "ruamel.yaml", "pydantic", "click", "networkx", "pandas", "numpy", "dask",
        "msgpack", "requests", "typing_extensions", "toolz", "cloudpickle",
        "partd", "fsspec", "python-dateutil", "tzdata", "pytz", "jsonschema"
    ]
    
    missing = []
    for pkg in required_packages:
        try:
            __import__(pkg.replace("-", "_"))
        except ImportError:
            missing.append(pkg)
    
    return missing


def _initialize_database(verbose: bool = False) -> bool:
    """Initialize the component and joblet registries."""
    try:
        # Initialize component registry (creates tables automatically)
        component_registry = ComponentRegistry()
        
        # Initialize joblet registry
        joblet_registry = JobletRegistry()
        
        if verbose:
            click.echo("  Database tables created successfully")
            click.echo(f"  Database location: {component_registry.db_path}")
        
        return True
    except Exception as e:
        if verbose:
            click.echo(f"  Database initialization failed: {e}")
        return False


def _create_directories(verbose: bool = False) -> bool:
    """Create default directories for DataPY operations."""
    directories = [
        ("jobs", "Built .pjob files"),
        ("logs", "Execution logs"),
        ("checkpoints", "Job checkpoints for resume"),
        ("temp", "Temporary files")
    ]
    
    created_count = 0
    
    for dir_name, description in directories:
        dir_path = Path(dir_name)
        try:
            dir_path.mkdir(exist_ok=True)
            if verbose:
                status = "created" if not dir_path.existed() else "exists"
                click.echo(f"  {dir_name}/ - {description} ({status})")
            created_count += 1
        except Exception as e:
            if verbose:
                click.echo(f"  {dir_name}/ - Failed to create: {e}")
    
    if verbose and created_count < len(directories):
        click.echo(f"  Created {created_count}/{len(directories)} directories")
    
    return created_count > 0


def _register_components(verbose: bool = False) -> bool:
    """Register all built-in components."""
    try:
        registry = ComponentRegistry()
        
        # Get current component count
        existing_components = len(registry.list_components())
        
        # Register components from package
        registered_count = registry.register_components_from_package()
        
        if verbose:
            if registered_count > 0:
                click.echo(f"  Registered {registered_count} components")
                
                # Show component breakdown by category
                components = registry.list_components()
                categories = {}
                for comp in components:
                    cat = comp['category']
                    categories[cat] = categories.get(cat, 0) + 1
                
                click.echo("  Component categories:")
                for category, count in sorted(categories.items()):
                    click.echo(f"    - {category}: {count}")
            else:
                click.echo("  No new components found to register")
                if existing_components > 0:
                    click.echo(f"    ({existing_components} components already registered)")
        
        return True
    except Exception as e:
        if verbose:
            click.echo(f"  Component registration failed: {e}")
        return False


def _validate_setup(verbose: bool = False) -> bool:
    """Validate that setup completed successfully."""
    validation_checks = [
        ("Database connectivity", _validate_database),
        ("Component registry", _validate_components),
        ("CLI commands", _validate_cli)
    ]
    
    passed_checks = 0
    
    for check_name, check_func in validation_checks:
        try:
            if check_func():
                if verbose:
                    click.echo(f"  {check_name} - OK")
                passed_checks += 1
            else:
                if verbose:
                    click.echo(f"  {check_name} - Issues detected")
        except Exception as e:
            if verbose:
                click.echo(f"  {check_name} - ERROR: {e}")
    
    if verbose:
        click.echo(f"  Validation: {passed_checks}/{len(validation_checks)} checks passed")
    
    return passed_checks == len(validation_checks)


def _validate_database() -> bool:
    """Validate database is working."""
    try:
        registry = ComponentRegistry()
        # Try to perform a simple operation
        components = registry.list_components()
        return True
    except:
        return False


def _validate_components() -> bool:
    """Validate components are registered."""
    try:
        registry = ComponentRegistry()
        components = registry.list_components()
        return len(components) > 0
    except:
        return False


def _validate_cli() -> bool:
    """Validate CLI commands are accessible."""
    try:
        # Try to import main CLI components
        from pype.cli.cli import cli
        return True
    except:
        return False


def _show_available_commands():
    """Show all available CLI commands with descriptions."""
    click.echo()
    click.echo("Available CLI Commands:")
    click.echo("=" * 30)
    
    commands = [
        # Core commands
        ("build <job.yaml>", "Build .pjob file from YAML definition"),
        ("run <job.pjob>", "Execute a .pjob file (Phase 2)"),
        ("inspect <job.pjob>", "Inspect contents of .pjob file"),
        
        # Component management
        ("register --all", "Register all built-in components"),
        ("register <name> --from-package", "Register specific component"),
        ("list", "List all registered components"),
        ("info <component>", "Show component details"),
        ("delete-component <name>", "Remove component from registry"),
        
        # Component creation
        ("create-component", "Create new component interactively"),
        ("component-template <name>", "Show component template"),
        ("component-examples", "Show component examples"),
        
        # Utilities
        ("setup", "Initialize DataPY workspace (this command)"),
        ("create <job_name>", "Create job from template (Phase 6)"),
        ("pack <dir> <output>", "Package job for distribution (Phase 6)"),
    ]
    
    # Calculate max width for alignment
    max_width = max(len(cmd) for cmd, _ in commands)
    
    for command, description in commands:
        click.echo(f"  pype {command:<{max_width}} - {description}")
    
    click.echo()
    click.echo("Use 'pype <command> --help' for detailed options")


def _show_next_steps():
    """Show recommended next steps after setup."""
    click.echo()
    click.echo("Next Steps:")
    click.echo("=" * 15)
    click.echo("1. View registered components:")
    click.echo("   pype list")
    click.echo()
    click.echo("2. Get component details:")
    click.echo("   pype info logging")
    click.echo()
    click.echo("3. Create your first job:")
    click.echo("   pype create-component my_transform --category transform")
    click.echo("   # Edit the generated component file")
    click.echo("   pype register my_transform --from-package")
    click.echo()
    click.echo("4. Build and test a job:")
    click.echo("   pype build test_jobs/sample_job.yaml")
    click.echo("   pype inspect jobs/sample_job.pjob")
    click.echo()
    click.echo("5. For more examples:")
    click.echo("   pype component-examples")
    click.echo()
    click.echo("Happy ETL building!")