#!/usr/bin/env python3
"""
Script to create all necessary __init__.py files for DataPY package structure.
"""

import os
from pathlib import Path


def create_init_file(directory: Path, content: str = ""):
    """Create __init__.py file in directory with optional content."""
    init_file = directory / "__init__.py"
    
    # Create directory if it doesn't exist
    directory.mkdir(parents=True, exist_ok=True)
    
    # Create __init__.py file
    if not init_file.exists():
        with open(init_file, 'w') as f:
            f.write(content)
        print(f" Created {init_file}")
    else:
        print(f"- {init_file} already exists")


def main():
    """Create all necessary __init__.py files."""
    
    print("Creating DataPY package structure...")
    print("=" * 40)
    
    base_dir = Path(".")
    
    # Core package structure
    packages = [
        # Main package
        "pype",
        
        # CLI module
        "pype/cli",
        
        # Core modules
        "pype/core",
        "pype/core/engine", 
        "pype/core/loader",
        "pype/core/planner",
        "pype/core/registry",
        "pype/core/observability",
        "pype/core/checkpointing",
        "pype/core/utils",
        
        # Components
        "pype/components",
        "pype/components/io",
        "pype/components/control", 
        "pype/components/transform",
        
        # Schemas (not a Python package, but good to have organized)
        "pype/schemas",
    ]
    
    # Create __init__.py for each package
    for package_path in packages:
        if package_path == "pype/schemas":
            # Skip schemas, it's not a Python package
            continue
            
        directory = base_dir / package_path
        
        # Special content for main pype package
        if package_path == "pype":
            content = '''"""
DataPY - CLI-first, YAML-driven ETL framework.

A Talend-style ETL engine built in Python.
"""

__version__ = "0.1.0"
__author__ = "DataPY Team"
'''
        # Special content for components package
        elif package_path == "pype/components":
            content = '''"""
DataPY Components Package.

Contains all built-in components for ETL operations:
- io: Input/output components (file readers, writers, etc.)
- transform: Data transformation components (map, filter, aggregate, etc.) 
- control: Control flow components (iterator, joblet, logging, etc.)
"""
'''
        else:
            content = ""
        
        create_init_file(directory, content)
    
    print("\n" + "=" * 40)
    print("Package structure created successfully!")
    print("\nNext steps:")
    print("1. Save the logging component to: pype/components/control/logging.py")
    print("2. Run: pype register --all")
    print("3. Run: pype list")


if __name__ == "__main__":
    main()