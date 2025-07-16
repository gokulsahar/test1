import os
import click
from pathlib import Path
from typing import Optional
from pype.core.utils.constants import DEFAULT_ENCODING

@click.command()
@click.argument("component_name", required=False)
@click.option("--non-interactive", is_flag=True, 
              help="Skip interactive prompts and use defaults")
@click.option("--category", type=click.Choice(["source", "sink", "transform", "quality", "utility", "misc"]), 
              help="Component category (will prompt if not provided)")
@click.option("--output-dir", type=click.Path(), 
              help="Output directory (default: pype/components/{category}/)")
@click.option("--force", is_flag=True,
              help="Overwrite existing component file")
def create_component(component_name: Optional[str], non_interactive: bool, 
                    category: Optional[str], output_dir: Optional[str], force: bool):
    """Create a new component with interactive prompts."""
    
    click.echo("DataPY Component Creator")
    click.echo("=" * 40)
    
    # Interactive component name if not provided
    if not component_name:
        click.echo()
        _show_naming_examples()
        click.echo()
        component_name = click.prompt(
            "Component name (e.g., 'read_csv', 'validate_schema')",
            type=str
        )
    
    # Validate component name
    if not _is_valid_component_name(component_name):
        click.echo("Component name must start with letter and contain only letters, numbers, and underscores")
        raise click.Abort()
    
    # Check naming convention
    naming_suggestion = _check_naming_convention(component_name)
    if naming_suggestion and not non_interactive:
        click.echo(f"Naming suggestion: {naming_suggestion}")
        # Give user options: accept suggestion, enter new name, or keep current
        if "Consider" in naming_suggestion or "Use" in naming_suggestion:
            suggested_name = _extract_suggested_name(component_name, naming_suggestion)
            if suggested_name and suggested_name != component_name:
                choice = click.prompt(
                    f"Options: (1) Use '{suggested_name}', (2) Enter new name, (3) Keep '{component_name}'",
                    type=click.Choice(['1', '2', '3']),
                    default='1'
                )
                if choice == '1':
                    component_name = suggested_name
                elif choice == '2':
                    component_name = click.prompt("Enter new component name", type=str)
                    # Re-validate the new name
                    if not _is_valid_component_name(component_name):
                        click.echo("Invalid name format. Using original name.")
                        component_name = component_name  # Keep original if invalid
        else:
            # Just a tip, ask if they want to try a different name
            if click.confirm("Would you like to enter a different name?"):
                component_name = click.prompt("Enter new component name", type=str)
                if not _is_valid_component_name(component_name):
                    click.echo("Invalid name format. Using original name.")
    
    click.echo(f"Component name: {component_name}")
    
    # Interactive mode or use provided values
    if non_interactive:
        # Use defaults for non-interactive mode
        config = _get_default_config(component_name, category)
    else:
        # Interactive configuration
        config = _interactive_configuration(component_name, category, non_interactive)
    
    # Determine output directory
    if output_dir is None:
        output_dir = Path("pype") / "components" / config["category"]
    else:
        output_dir = Path(output_dir)
    
    # Create output directory if it doesn't exist
    output_dir.mkdir(parents=True, exist_ok=True)
    
    # Generate file path
    file_name = f"{component_name.lower()}.py"
    file_path = output_dir / file_name
    
    # Check if file exists
    if file_path.exists() and not force:
        if click.confirm(f"{file_path} already exists. Overwrite?"):
            force = True
        else:
            click.echo("Operation cancelled")
            raise click.Abort()
    
    # Generate component class name
    class_name = _to_class_name(component_name)
    
    # Show summary
    _show_component_summary(component_name, class_name, config, file_path)
    
    if not non_interactive:
        if not click.confirm("Create this component?"):
            click.echo("Operation cancelled")
            raise click.Abort()
    
    # Generate component code
    component_code = _generate_component_code(
        component_name=component_name,
        class_name=class_name,
        **config
    )
    
    # Write file
    try:
        with open(file_path, 'w', encoding=DEFAULT_ENCODING) as f:
            f.write(component_code)
        
        click.echo()
        click.echo("Component created successfully!")
        click.echo(f"File: {file_path}")
        click.echo()
        _show_next_steps(component_name)
        
    except Exception as e:
        click.echo(f"Error creating component file: {e}")
        raise click.Abort()


def _interactive_configuration(component_name: str, category: Optional[str], non_interactive: bool = False) -> dict:
    """Interactive configuration with smart prompts."""
    config = {}
    
    # Category selection
    if category:
        config["category"] = category
        click.echo(f"Category: {category}")
    else:
        click.echo()
        click.echo("Available categories:")
        click.echo("  source:    Read data from external sources")
        click.echo("  sink:      Write data to external destinations") 
        click.echo("  transform: Transform and manipulate data")
        click.echo("  quality:   Validate and ensure data quality")
        click.echo("  utility:   Provide utility functionality")
        click.echo("  misc:      Custom/general purpose (default)")
        click.echo()
        config["category"] = click.prompt(
            "Component category",
            type=click.Choice(["source", "sink", "transform", "quality", "utility", "misc"]),
            default="misc",
            show_default=True
        )
    
    # Show naming suggestions for selected category
    if not non_interactive:
        suggestions = _get_name_suggestions(config["category"])
        if suggestions:
            click.echo(f"\nNaming examples for {config['category']} category:")
            click.echo(f"  {', '.join(suggestions[:6])}")
            click.echo()
    
    # Version
    config["version"] = click.prompt(
        "Component version",
        default="1.0.0",
        show_default=True
    )
    
    # Validate version
    if not _is_valid_semver(config["version"]):
        click.echo("Version must follow semantic versioning (e.g., '1.0.0')")
        raise click.Abort()
    
    # Author
    config["author"] = click.prompt(
        "Author name & ID",
        default=os.environ.get("USER", "DataPY Developer"),
        show_default=True
    )
    
    # Description
    default_desc = _get_default_description(component_name, config["category"])
    config["description"] = click.prompt(
        "Component description",
        default=default_desc,
        show_default=True
    )
    
    click.echo()
    click.echo("Component Behavior:")
    
    # Startable (with smart defaults based on category)
    default_startable = config["category"] == "source"
    config["startable"] = click.confirm(
        "Can this component start a pipeline?",
        default=default_startable
    )
    
    # Multi-input
    config["multi_input"] = click.confirm(
        "Allow multiple input connections?",
        default=False
    )
    
    # Advanced options
    click.echo()
    if click.confirm("Configure advanced options?", default=False):
        config.update(_advanced_configuration(config))
    else:
        # Use smart defaults
        config["idempotent"] = True
        config["output_globals"] = []
        config["dependencies"] = []
    
    return config


def _advanced_configuration(base_config: dict) -> dict:
    """Advanced configuration options."""
    advanced = {}
    
    # Idempotent
    advanced["idempotent"] = click.confirm(
        "Is component idempotent? (safe to retry without side effects)",
        default=True
    )
    
    # Output globals
    if click.confirm("Does component create global variables?", default=False):
        globals_input = click.prompt(
            "Global variable names (comma-separated, or press Enter to skip)",
            default="",
            show_default=False
        )
        if globals_input.strip():
            advanced["output_globals"] = [g.strip() for g in globals_input.split(",")]
        else:
            advanced["output_globals"] = []
    else:
        advanced["output_globals"] = []
    
    # Dependencies
    if click.confirm("Does component require external libraries?", default=False):
        deps_input = click.prompt(
            "Dependencies (comma-separated, or press Enter to skip)",
            default="",
            show_default=False
        )
        if deps_input.strip():
            advanced["dependencies"] = [d.strip() for d in deps_input.split(",")]
        else:
            advanced["dependencies"] = []
    else:
        advanced["dependencies"] = []
    
    return advanced


def _get_default_config(component_name: str, category: Optional[str]) -> dict:
    """Get default configuration for non-interactive mode."""
    cat = category or "misc"
    return {
        "category": cat,
        "version": "1.0.0",
        "author": os.environ.get("USER", "DataPY Developer"),
        "description": _get_default_description(component_name, cat),
        "startable": cat == "source",
        "multi_input": False,
        "idempotent": True,
        "output_globals": [],
        "dependencies": []
    }


def _get_default_description(component_name: str, category: str) -> str:
    """Generate smart default description."""
    name_lower = component_name.lower().replace("_", " ")
    
    if category == "source":
        return f"Reads data from {name_lower.replace('read', '').replace('load', '').strip() or 'external source'}"
    elif category == "sink":
        return f"Writes data to {name_lower.replace('write', '').replace('save', '').strip() or 'external destination'}"
    elif category == "transform":
        return f"Transforms data using {name_lower} logic"
    elif category == "quality":
        return f"Validates and ensures quality of data using {name_lower} rules"
    elif category == "utility":
        return f"Provides {name_lower} utility functionality"
    else:  # misc
        return f"Custom component that handles {name_lower} operations"


def _show_component_summary(component_name: str, class_name: str, config: dict, file_path: Path):
    """Show component creation summary."""
    click.echo()
    click.echo("Component Summary:")
    click.echo("=" * 30)
    click.echo(f"Name: {component_name}")
    click.echo(f"Class: {class_name}")
    click.echo(f"Category: {config['category']}")
    click.echo(f"Version: {config['version']}")
    click.echo(f"Author: {config['author']}")
    click.echo(f"Description: {config['description']}")
    click.echo(f"Startable: {'Yes' if config['startable'] else 'No'}")
    click.echo(f"Multi-input: {'Yes' if config['multi_input'] else 'No'}")
    click.echo(f"File: {file_path}")
    
    if config.get("output_globals"):
        click.echo(f"Global vars: {', '.join(config['output_globals'])}")
    if config.get("dependencies"):
        click.echo(f"Dependencies: {', '.join(config['dependencies'])}")


def _show_next_steps(component_name: str):
    """Show next steps after component creation."""
    click.echo("Next Steps:")
    click.echo("1. Edit the component file to implement your logic")
    click.echo("2. Update CONFIG_SCHEMA with actual parameters")
    click.echo("3. Test component registration:")
    click.echo(f"   pype register {component_name} --from-package")
    click.echo("4. Create a test job YAML to verify functionality")
    click.echo()
    click.echo("Tips:")
    click.echo("- Check the TODO comments in the generated file")
    click.echo("- Use 'pype component-examples' for usage examples")
    click.echo("- Run 'pype list' to see your registered component")


def _generate_component_code(component_name: str, class_name: str, category: str,
                           startable: bool, multi_input: bool, version: str,
                           author: str, description: str, idempotent: bool = True,
                           output_globals: list = None, dependencies: list = None) -> str:
    """Generate the complete component code."""
    
    output_globals = output_globals or []
    dependencies = dependencies or []
    
    # Determine input/output ports and config based on category
    if category == "source":
        input_ports = '[]'  # Source components typically have no inputs
        output_ports = '["main"]'
        config_example = '''        "required": {
            "connection_string": {"type": "str", "description": "Data source connection details"}
        },
        "optional": {
            "batch_size": {"type": "int", "default": 1000, "description": "Number of records per batch"},
            "timeout": {"type": "int", "default": 30, "description": "Connection timeout in seconds"}
        }'''
        execute_body = '''        # TODO: Implement data source reading logic
        # Example: Connect to database/API/file and read data
        
        # Create DataFrame based on execution mode
        if self.execution_mode == "pandas":
            import pandas as pd
            sample_data = pd.DataFrame({
                "id": [1, 2, 3],
                "value": ["a", "b", "c"]
            })
        else:  # dask mode
            import pandas as pd
            import dask.dataframe as dd
            sample_data = pd.DataFrame({
                "id": [1, 2, 3],
                "value": ["a", "b", "c"]
            })
            sample_data = dd.from_pandas(sample_data, npartitions=2)
        
        return {"main": sample_data}'''
        
    elif category == "sink":
        input_ports = '["main"]'
        output_ports = '["main"]'  # Sink can pass through for chaining
        config_example = '''        "required": {
            "destination": {"type": "str", "description": "Output destination path or connection"}
        },
        "optional": {
            "format": {"type": "str", "default": "csv", "description": "Output format"},
            "overwrite": {"type": "bool", "default": False, "description": "Overwrite existing data"}
        }'''
        execute_body = '''        # TODO: Implement data sink writing logic
        # Get input data
        input_data = inputs["main"]
        
        # Convert to pandas for writing (if needed)
        if self.execution_mode == "dask":
            df = input_data.compute()  # Convert dask to pandas for writing
        else:
            df = input_data
        
        # Example: Write data to database/file/API
        # your_write_logic_here(df)
        
        # Pass through the data for potential chaining
        return {"main": input_data}'''
        
    elif category == "transform":
        input_ports = '["main"]'
        output_ports = '["main"]'
        config_example = '''        "required": {},
        "optional": {
            "operation": {"type": "str", "default": "passthrough", "description": "Transformation operation to perform"}
        }'''
        execute_body = '''        # TODO: Implement transformation logic
        # Get input data
        input_data = inputs["main"]
        
        # Apply transformation based on execution mode
        if self.execution_mode == "pandas":
            # Pandas transformation
            transformed_df = input_data.copy()
            # Add your pandas transformations here
            # transformed_df = transformed_df[transformed_df['column'] > 0]
            
        else:  # dask mode
            # Dask transformation
            transformed_df = input_data.copy()
            # Add your dask transformations here
            # transformed_df = transformed_df[transformed_df['column'] > 0]
        
        return {"main": transformed_df}'''
        
    elif category == "quality":
        input_ports = '["main"]'
        output_ports = '["main", "rejected"]'  # Quality components often have reject output
        config_example = '''        "required": {},
        "optional": {
            "validation_rules": {"type": "str", "default": "", "description": "Validation rules to apply"},
            "reject_invalid": {"type": "bool", "default": True, "description": "Output rejected records separately"}
        }'''
        execute_body = '''        # TODO: Implement data quality logic
        # Get input data
        input_data = inputs["main"]
        
        # Apply quality validation based on execution mode
        if self.execution_mode == "pandas":
            # Example quality validation
            # valid_mask = input_data['column'].notnull()
            # valid_df = input_data[valid_mask]
            # rejected_df = input_data[~valid_mask]
            
            # For now, assume all data is valid
            valid_df = input_data.copy()
            import pandas as pd
            rejected_df = pd.DataFrame()  # Empty rejected data
            
        else:  # dask mode
            # Example quality validation for dask
            # valid_mask = input_data['column'].notnull()
            # valid_df = input_data[valid_mask]
            # rejected_df = input_data[~valid_mask]
            
            # For now, assume all data is valid
            valid_df = input_data.copy()
            import pandas as pd
            import dask.dataframe as dd
            rejected_df = dd.from_pandas(pd.DataFrame(), npartitions=1)
        
        result = {"main": valid_df}
        
        # Only include rejected port if there's rejected data
        if self.execution_mode == "pandas":
            if not rejected_df.empty:
                result["rejected"] = rejected_df
        else:  # dask
            if len(rejected_df) > 0:
                result["rejected"] = rejected_df
        
        return result'''
        
    elif category == "utility":
        input_ports = '["main"]'
        output_ports = '["main"]'
        config_example = '''        "required": {},
        "optional": {
            "utility_param": {"type": "str", "default": "", "description": "Utility-specific parameter"}
        }'''
        execute_body = '''        # TODO: Implement utility logic
        # Get input data
        input_data = inputs["main"]
        
        # Apply utility operation based on execution mode
        if self.execution_mode == "pandas":
            # Your utility logic here for pandas
            processed_df = input_data.copy()
            
        else:  # dask mode
            # Your utility logic here for dask
            processed_df = input_data.copy()
        
        return {"main": processed_df}'''
        
    else:  # misc
        input_ports = '["main"]'
        output_ports = '["main"]'
        config_example = '''        "required": {},
        "optional": {}'''
        execute_body = '''        # TODO: Implement your custom logic
        # Get input data (if any)
        input_data = inputs.get("main")
        
        if input_data is not None:
            # Process input data based on execution mode
            if self.execution_mode == "pandas":
                processed_df = input_data.copy()
                # Your custom pandas logic here
            else:  # dask mode
                processed_df = input_data.copy()
                # Your custom dask logic here
            
            return {"main": processed_df}
        else:
            # No input data - create your own based on execution mode
            if self.execution_mode == "pandas":
                import pandas as pd
                sample_data = pd.DataFrame({"result": ["completed"]})
            else:  # dask mode
                import pandas as pd
                import dask.dataframe as dd
                sample_data = pd.DataFrame({"result": ["completed"]})
                sample_data = dd.from_pandas(sample_data, npartitions=1)
            
            return {"main": sample_data}'''
    
    # Generate lifecycle hooks for source and sink components
    lifecycle_hooks = ""
    if category in ["source", "sink"]:
        lifecycle_hooks = '''
    def setup(self, context: Dict[str, Any]) -> None:
        """Initialize connections/resources."""
        # TODO: Initialize database connections, file handles, etc.
        pass
    
    def cleanup(self, context: Dict[str, Any]) -> None:
        """Clean up connections/resources."""
        # TODO: Close connections, cleanup temporary files, etc.
        pass'''
    
    # Format output globals and dependencies
    output_globals_str = str(output_globals) if output_globals else "[]"
    dependencies_str = str(dependencies) if dependencies else "[]"
    
    template = f'''"""
{description}

Author: {author}
Version: {version}
Category: {category}
Created: Auto-generated by DataPY CLI
"""

from typing import Dict, Any, Union
import pandas as pd
import dask.dataframe as dd
from pype.components.base import BaseComponent


class {class_name}(BaseComponent):
    """
    {description}
    
    This component {_get_category_description(category)}.
    """
    
    COMPONENT_NAME = "{component_name}"
    VERSION = "{version}"
    CATEGORY = "{category}"
    INPUT_PORTS = {input_ports}
    OUTPUT_PORTS = {output_ports}
    OUTPUT_GLOBALS = {output_globals_str}
    DEPENDENCIES = {dependencies_str}
    STARTABLE = {str(startable).title()}
    ALLOW_MULTI_IN = {str(multi_input).title()}
    IDEMPOTENT = {str(idempotent).title()}
    
    CONFIG_SCHEMA = {{
{config_example}
    }}
    
    def execute(self, context: Dict[str, Any], inputs: Dict[str, Union[pd.DataFrame, dd.DataFrame]]) -> Dict[str, Union[pd.DataFrame, dd.DataFrame]]:
        """
        Execute the {component_name} component.
        
        Args:
            context: Execution context with run metadata and global variables
            inputs: Input DataFrames from upstream components
            
        Returns:
            Dictionary of output DataFrames for downstream components
        """
{execute_body}{lifecycle_hooks}


# TODO: Implementation Checklist:
# [ ] 1. Implement the execute() method logic
# [ ] 2. Update CONFIG_SCHEMA with actual parameters needed
# [ ] 3. Set OUTPUT_GLOBALS if component creates global variables
# [ ] 4. Add DEPENDENCIES if component requires external libraries
# [ ] 5. Implement setup()/cleanup() if component needs resource management
# [ ] 6. Set IDEMPOTENT = False if component has side effects
# [ ] 7. Test the component: pype register --from-package
# [ ] 8. Create a test job YAML to verify functionality
'''
    
    return template


def _get_category_description(category: str) -> str:
    """Get description text for category."""
    descriptions = {
        "source": "reads data from external sources like files, databases, or APIs",
        "sink": "writes data to external destinations like files, databases, or APIs", 
        "transform": "transforms data by filtering, mapping, aggregating, or modifying records",
        "quality": "validates data quality and ensures data meets specified criteria",
        "utility": "provides utility functionality like caching, conversion, or helper operations",
        "misc": "performs custom data processing operations"
    }
    return descriptions.get(category, "performs data processing operations")


def _show_naming_examples():
    """Show naming convention examples."""
    click.echo("Naming Convention: {action}_{system/format}_{optional_detail}")
    click.echo("Examples by category:")
    click.echo("  source:    read_csv, extract_oracle, fetch_api")
    click.echo("  sink:      write_postgres, save_excel, send_email")
    click.echo("  transform: filter_rows, map_columns, join_tables")
    click.echo("  quality:   validate_schema, check_nulls, clean_duplicates")
    click.echo("  utility:   cache_lookup, convert_timezone, split_data")
    click.echo("  misc:      process_orders, calculate_metrics")


def _extract_suggested_name(original_name: str, suggestion: str) -> str:
    """Extract a concrete suggested name from the suggestion text."""
    original_lower = original_name.lower()
    
    # Handle specific patterns and provide concrete suggestions
    if "csv_reader" in original_lower:
        return "read_csv"
    elif "json_reader" in original_lower:
        return "read_json"
    elif "data_filter" in original_lower:
        return "filter_rows"
    elif "data_validator" in original_lower:
        return "validate_schema"
    elif "processor" in original_lower:
        # Try to suggest based on context
        if "order" in original_lower:
            return "process_orders"
        elif "data" in original_lower:
            return "transform_data"
        else:
            return original_name  # No concrete suggestion
    elif "handler" in original_lower:
        if "error" in original_lower:
            return "handle_errors"
        elif "event" in original_lower:
            return "handle_events"
        else:
            return original_name
    elif original_name and "_" not in original_name:
        # Suggest adding action prefix based on common patterns
        if "csv" in original_lower:
            return f"read_{original_name}"
        elif "filter" in original_lower:
            return f"{original_name}_rows"
        elif "validate" in original_lower:
            return f"{original_name}_schema"
    
    return original_name  # Return original if no concrete suggestion


def _check_naming_convention(name: str) -> str:
    """Check naming convention and suggest improvements."""
    name_lower = name.lower()
    
    # Common anti-patterns
    anti_patterns = {
        "processor": "Use specific action like 'process_orders' or 'transform_data'",
        "handler": "Use specific action like 'handle_errors' or 'process_events'", 
        "manager": "Use specific action like 'manage_cache' or 'coordinate_tasks'",
        "component": "Remove 'component' suffix, it's implied",
        "data": "Be more specific about what data operation"
    }
    
    for pattern, suggestion in anti_patterns.items():
        if pattern in name_lower:
            return f"Avoid '{pattern}': {suggestion}"
    
    # Category-specific suggestions
    category_verbs = {
        "reader": "Consider 'read_' prefix (e.g., 'read_csv' instead of 'csv_reader')",
        "writer": "Consider 'write_' prefix (e.g., 'write_json' instead of 'json_writer')",
        "loader": "Consider 'load_' prefix for sources or 'extract_' for systems",
        "saver": "Consider 'save_' or 'write_' prefix",
        "filter": "Good! Consider 'filter_rows' or 'filter_nulls' for specificity",
        "validator": "Consider 'validate_' prefix (e.g., 'validate_schema')",
        "cleaner": "Consider 'clean_' prefix (e.g., 'clean_duplicates')"
    }
    
    for pattern, suggestion in category_verbs.items():
        if pattern in name_lower:
            return f"Naming tip: {suggestion}"
    
    # Check if name follows action_subject pattern
    if "_" not in name_lower:
        return "Consider action_subject pattern: 'read_csv', 'filter_rows', 'validate_schema'"
    
    # If name looks good, return empty string
    return ""


def _get_name_suggestions(category: str) -> list:
    """Get name suggestions based on category."""
    suggestions = {
        "source": [
            "read_csv", "read_json", "read_parquet", "extract_oracle", 
            "extract_postgres", "extract_mysql", "fetch_api", "fetch_s3"
        ],
        "sink": [
            "write_csv", "write_json", "write_parquet", "load_postgres",
            "load_snowflake", "save_excel", "send_email", "send_webhook"
        ],
        "transform": [
            "filter_rows", "filter_nulls", "map_columns", "map_values",
            "join_tables", "join_lookup", "aggregate_sum", "aggregate_group"
        ],
        "quality": [
            "validate_schema", "validate_format", "check_nulls", "check_duplicates",
            "clean_data", "clean_outliers", "profile_data", "audit_quality"
        ],
        "utility": [
            "cache_lookup", "cache_dataframe", "convert_timezone", "convert_currency",
            "split_data", "merge_data", "hash_columns", "encrypt_data"
        ],
        "misc": [
            "process_orders", "calculate_metrics", "generate_report", "analyze_trends",
            "reconcile_accounts", "summarize_data", "enrich_data", "score_model"
        ]
    }
    return suggestions.get(category, [])


def _is_valid_component_name(name: str) -> bool:
    """Validate component name format."""
    import re
    pattern = r'^[a-zA-Z][a-zA-Z0-9_]*$'
    return bool(re.match(pattern, name))


def _is_valid_semver(version: str) -> bool:
    """Validate semantic versioning format."""
    import re
    pattern = r'^(\d+)\.(\d+)\.(\d+)$'
    return bool(re.match(pattern, version))


def _to_class_name(component_name: str) -> str:
    """Convert component_name to ClassName."""
    # Split by underscores and capitalize each part
    parts = component_name.split('_')
    class_name = ''.join(word.capitalize() for word in parts)
    
    # Ensure it ends with Component
    if not class_name.endswith('Component'):
        class_name += 'Component'
    
    return class_name


# Additional utility commands
@click.command()
@click.argument("component_name")
@click.option("--category", help="Component category")
def component_template(component_name: str, category: Optional[str]):
    """Show component template without creating file."""
    
    # Generate template with sample values
    template = _generate_component_code(
        component_name=component_name,
        class_name=_to_class_name(component_name),
        category=category or "misc",
        startable=category == "source",
        multi_input=False,
        version="1.0.0",
        author="Your Name",
        description=_get_default_description(component_name, category or "misc")
    )
    
    click.echo("Component Template:")
    click.echo("=" * 50)
    click.echo(template)


@click.command()
def component_examples():
    """Show examples of different component types with naming conventions."""
    
    click.echo("Component Creation Examples with Naming Conventions:")
    click.echo("=" * 60)
    click.echo()
    click.echo("Naming Pattern: {action}_{system/format}_{optional_detail}")
    click.echo()
    
    examples = {
        "Source Components (Data Readers)": [
            ("read_csv --category source", "Read CSV files"),
            ("extract_oracle --category source", "Extract from Oracle database"),
            ("fetch_api --category source", "Fetch from REST API")
        ],
        "Sink Components (Data Writers)": [
            ("write_postgres --category sink", "Write to PostgreSQL"),
            ("save_excel --category sink", "Save to Excel file"), 
            ("send_email --category sink", "Send via email")
        ],
        "Transform Components": [
            ("filter_rows --category transform", "Filter data rows"),
            ("map_columns --category transform", "Transform columns"),
            ("join_tables --category transform", "Join datasets")
        ],
        "Quality Components": [
            ("validate_schema --category quality", "Validate data schema"),
            ("check_nulls --category quality", "Check for null values"),
            ("clean_duplicates --category quality", "Remove duplicates")
        ],
        "Utility Components": [
            ("cache_lookup --category utility", "Cache lookup data"),
            ("convert_timezone --category utility", "Convert timezones"),
            ("split_data --category utility", "Split datasets")
        ],
        "Misc Components (Custom)": [
            ("process_orders --category misc", "Process business orders"),
            ("calculate_metrics --category misc", "Calculate KPIs"),
            ("generate_report --category misc", "Generate reports")
        ]
    }
    
    for category_desc, category_examples in examples.items():
        click.echo(f"{category_desc}:")
        for cmd, desc in category_examples:
            click.echo(f"  pype create-component {cmd}")
            click.echo(f"    {desc}")
        click.echo()
    
    click.echo("Naming Guidelines:")
    click.echo("- Use snake_case (lowercase with underscores)")
    click.echo("- Start with action verb (read, write, filter, validate)")
    click.echo("- Include system/format when relevant")
    click.echo("- Be specific but concise")
    click.echo("- Avoid generic terms (processor, handler, manager)")
    click.echo()
    click.echo("Use --help for all options")