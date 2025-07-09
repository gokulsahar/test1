import json
import click
from typing import Optional
from pype.core.registry.component_registry import ComponentRegistry


@click.command()
@click.option("--all", "register_all", is_flag=True, 
              help="Register all components from pype.components package")
@click.argument("component_name", required=False)
@click.option("--from-package", is_flag=True, 
              help="Auto-discover component from pype.components package")
def register_component_command(register_all: bool, component_name: str, from_package: bool):
    """Register component(s) in the registry."""
    
    registry = ComponentRegistry()
    
    try:
        if register_all:
            # Register all components from package
            click.echo("Scanning pype.components package...")
            registered_count = registry.register_components_from_package()
            
            if registered_count > 0:
                click.echo(f" Successfully registered {registered_count} components")
                
                # Show registered components
                components = registry.list_components()
                click.echo("\nRegistered components:")
                for comp in sorted(components, key=lambda x: (x['category'], x['name'])):
                    category = comp['category']
                    name = comp['name'] 
                    click.echo(f"  {name} ({category})")
            else:
                click.echo(" No components found to register")
                
        elif component_name:
            if from_package:
                # Auto-discover specific component from package
                success = registry.register_component_by_name(component_name)
                if success:
                    click.echo(f" Registered component '{component_name}' from package")
                else:
                    click.echo(f" Component '{component_name}' not found in package")
                    raise click.Abort()
            else:
                click.echo("Manual component registration not yet implemented")
                click.echo("Use --from-package to auto-discover from pype.components")
                raise click.Abort()
        else:
            click.echo("Error: Must specify either --all or provide component_name")
            click.echo("Examples:")
            click.echo("  pype register-component --all")
            click.echo("  pype register-component echo --from-package")
            raise click.Abort()
            
    except Exception as e:
        click.echo(f"Registration failed: {e}")
        raise click.Abort()


@click.command()
@click.option("--category", help="Filter by category")
@click.option("--format", "output_format", type=click.Choice(["table", "json", "names"]), 
              default="table", help="Output format")
@click.option("--verbose", "-v", is_flag=True, help="Show detailed information")
def list_components_command(category: Optional[str], output_format: str, verbose: bool):
    """List all registered components."""
    
    registry = ComponentRegistry()
    
    try:
        components = registry.list_components()
        
        if category:
            components = [c for c in components if c.get('category') == category]
        
        if not components:
            if category:
                click.echo(f"No components found in category '{category}'")
            else:
                click.echo("No components registered")
            return
        
        if output_format == "json":
            click.echo(json.dumps(components, indent=2))
        elif output_format == "names":
            for comp in components:
                click.echo(comp['name'])
        else:  # table format
            _show_components_table(components, verbose)
            
    except Exception as e:
        click.echo(f"Failed to list components: {e}")
        raise click.Abort()


@click.command()
@click.argument("component_name")
@click.option("--format", "output_format", type=click.Choice(["table", "json"]), 
              default="table", help="Output format")
def component_info_command(component_name: str, output_format: str):
    """Show detailed information about a component."""
    
    registry = ComponentRegistry()
    
    try:
        component = registry.get_component(component_name)
        
        if not component:
            click.echo(f"Component '{component_name}' not found")
            raise click.Abort()
        
        if output_format == "json":
            click.echo(json.dumps(component, indent=2))
        else:
            _show_component_details(component)
            
    except Exception as e:
        click.echo(f"Failed to get component info: {e}")
        raise click.Abort()


@click.command()
@click.argument("component_name")
@click.option("--confirm", is_flag=True, help="Skip confirmation prompt")
def delete_component_command(component_name: str, confirm: bool):
    """Delete a component from the registry."""
    
    registry = ComponentRegistry()
    
    try:
        # Check if component exists
        component = registry.get_component(component_name)
        if not component:
            click.echo(f"Component '{component_name}' not found")
            raise click.Abort()
        
        # Confirm deletion
        if not confirm:
            click.echo(f"Component: {component['name']} ({component['category']})")
            click.echo(f"Description: {component['description']}")
            if not click.confirm("Are you sure you want to delete this component?"):
                click.echo("Deletion cancelled")
                return
        
        # Delete component
        success = registry.delete_component(component_name)
        if success:
            click.echo(f" Deleted component '{component_name}'")
        else:
            click.echo(f" Failed to delete component '{component_name}'")
            raise click.Abort()
            
    except Exception as e:
        click.echo(f"Deletion failed: {e}")
        raise click.Abort()


def _show_components_table(components, verbose: bool):
    """Show components in table format."""
    
    # Header
    if verbose:
        click.echo(f"{'Name':<20} {'Category':<12} {'Startable':<9} {'Multi-In':<8} {'Ports':<12} {'Description'}")
        click.echo("-" * 85)
    else:
        click.echo(f"{'Name':<20} {'Category':<12} {'Description'}")
        click.echo("-" * 60)
    
    # Rows
    for comp in sorted(components, key=lambda x: (x['category'], x['name'])):
        name = comp['name'][:19]
        category = comp['category'][:11]
        description = comp['description'][:40] + "..." if len(comp['description']) > 40 else comp['description']
        
        if verbose:
            startable = "Yes" if comp['startable'] else "No"
            multi_in = "Yes" if comp['allow_multi_in'] else "No"
            
            input_count = len(comp['input_ports'])
            output_count = len(comp['output_ports'])
            ports = f"{input_count}->{output_count}"
            
            click.echo(f"{name:<20} {category:<12} {startable:<9} {multi_in:<8} {ports:<12} {description}")
        else:
            click.echo(f"{name:<20} {category:<12} {description}")
    
    # Summary
    click.echo(f"\nTotal: {len(components)} components")
    
    if verbose:
        # Category breakdown
        categories = {}
        for comp in components:
            cat = comp['category']
            categories[cat] = categories.get(cat, 0) + 1
        
        click.echo("Categories:", end="")
        for cat, count in sorted(categories.items()):
            click.echo(f" {cat}({count})", end="")
        click.echo()


def _show_component_details(component):
    """Show detailed component information."""
    
    click.echo(f"=== Component: {component['name']} ===")
    click.echo(f"Class: {component['class_name']}")
    click.echo(f"Module: {component['module_path']}")
    click.echo(f"Category: {component['category']}")
    click.echo(f"Description: {component['description']}")
    click.echo()
    
    # Capabilities
    click.echo("Capabilities:")
    click.echo(f"  Startable: {'Yes' if component['startable'] else 'No'}")
    click.echo(f"  Allow Multi-Input: {'Yes' if component['allow_multi_in'] else 'No'}")
    click.echo(f"  Idempotent: {'Yes' if component['idempotent'] else 'No'}")
    click.echo()
    
    # Ports
    click.echo("Ports:")
    input_ports = component['input_ports']
    output_ports = component['output_ports']
    
    if input_ports:
        click.echo(f"  Input: {', '.join(input_ports)}")
    else:
        click.echo("  Input: (none)")
        
    if output_ports:
        click.echo(f"  Output: {', '.join(output_ports)}")
    else:
        click.echo("  Output: (none)")
    click.echo()
    
    # Parameters
    required_params = component['required_params']
    optional_params = component['optional_params']
    
    if required_params or optional_params:
        click.echo("Parameters:")
        
        if required_params:
            click.echo("  Required:")
            for param, spec in required_params.items():
                param_type = spec.get('type', 'any')
                description = spec.get('description', 'No description')
                click.echo(f"    {param} ({param_type}): {description}")
        
        if optional_params:
            click.echo("  Optional:")
            for param, spec in optional_params.items():
                param_type = spec.get('type', 'any')
                default = spec.get('default', 'None')
                description = spec.get('description', 'No description')
                click.echo(f"    {param} ({param_type}, default={default}): {description}")
    else:
        click.echo("Parameters: (none)")
    click.echo()
    
    # Global variables
    output_globals = component['output_globals']
    if output_globals:
        click.echo(f"Global Variables: {', '.join(output_globals)}")
    else:
        click.echo("Global Variables: (none)")
    
    # Dependencies
    dependencies = component['dependencies']
    if dependencies:
        click.echo(f"Dependencies: {', '.join(dependencies)}")
    else:
        click.echo("Dependencies: (none)")
    
    # Metadata
    click.echo()
    click.echo("Metadata:")
    click.echo(f"  Created: {component['created_at']}")
    click.echo(f"  Updated: {component['updated_at']}")