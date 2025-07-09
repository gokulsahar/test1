#!/usr/bin/env python3
"""
Clean .pjob file inspector without emojis
Usage: python inspect_pjob.py jobs/data_processing_pipeline.pjob
"""
import zipfile
import json
import sys
from pathlib import Path


def inspect_pjob(pjob_path, extract_to=None):
    """Inspect contents of a .pjob file."""
    pjob_path = Path(pjob_path)
    
    if not pjob_path.exists():
        print(f"Error: {pjob_path} not found")
        return
    
    print(f"Inspecting: {pjob_path}")
    print("=" * 60)
    
    with zipfile.ZipFile(pjob_path, 'r') as zf:
        # List all files
        print("Contents:")
        total_size = 0
        for info in zf.infolist():
            size_kb = info.file_size / 1024
            total_size += info.file_size
            print(f"  {info.filename:<25} {size_kb:>8.1f} KB")
        print(f"  Total size: {total_size/1024:.1f} KB")
        print()
        
        # Show manifest
        if 'manifest.json' in zf.namelist():
            print("Manifest:")
            try:
                manifest = json.loads(zf.read('manifest.json'))
                for key, value in manifest.items():
                    if key == 'checksums':
                        print(f"  {key}: {len(value)} files")
                    else:
                        print(f"  {key}: {value}")
            except Exception as e:
                print(f"  Error reading manifest: {e}")
            print()
        
        # Show original YAML snippet
        if 'job_original.yaml' in zf.namelist():
            print("Original YAML (first 10 lines):")
            try:
                yaml_content = zf.read('job_original.yaml').decode('utf-8')
                lines = yaml_content.split('\n')[:10]
                for line in lines:
                    print(f"  {line}")
                if len(yaml_content.split('\n')) > 10:
                    print(f"  ... ({len(yaml_content.split('\n'))} total lines)")
            except Exception as e:
                print(f"  Error reading YAML: {e}")
            print()
        
        # Show DAG summary
        if 'dag.msgpack' in zf.namelist():
            print("DAG Summary:")
            try:
                import msgpack
                dag_data = msgpack.unpackb(zf.read('dag.msgpack'), raw=False)
                nodes = dag_data.get('nodes', [])
                links = dag_data.get('links', [])
                print(f"  Nodes (Components): {len(nodes)}")
                print(f"  Links (Connections): {len(links)}")
                
                # Show component types
                if nodes:
                    print("  Component Types:")
                    types = {}
                    for node in nodes:
                        comp_type = node.get('component_type', 'unknown')
                        types[comp_type] = types.get(comp_type, 0) + 1
                    for comp_type, count in types.items():
                        print(f"    - {comp_type}: {count}")
                
                # Show first few nodes
                print("  First 3 Components:")
                for node in nodes[:3]:
                    node_id = node.get('id', 'unknown')
                    comp_type = node.get('component_type', 'unknown')
                    startable = "[START]" if node.get('startable') else ""
                    print(f"    - {node_id} ({comp_type}) {startable}")
                        
            except ImportError:
                print("  Install msgpack to see DAG details: pip install msgpack")
            except Exception as e:
                print(f"  Error reading DAG: {e}")
            print()
            
        # Show execution metadata
        if 'execution_metadata.msgpack' in zf.namelist():
            print("Execution Metadata:")
            try:
                import msgpack
                exec_data = msgpack.unpackb(zf.read('execution_metadata.msgpack'), raw=False)
                
                startable = exec_data.get('startable_components', [])
                resources = exec_data.get('resource_requirements', {})
                runtime = exec_data.get('estimated_execution_time', 0)
                
                print(f"  Startable Components: {', '.join(startable)}")
                print(f"  Estimated Runtime: {runtime} seconds")
                print(f"  Memory Requirements: {resources.get('memory_requirements_mb', 0)} MB")
                print(f"  CPU Intensity: {resources.get('cpu_intensity', 'unknown')}")
                        
            except ImportError:
                print("  Install msgpack to see execution details: pip install msgpack")
            except Exception as e:
                print(f"  Error reading execution metadata: {e}")
            print()
        
        # Show subjob info
        if 'subjob_metadata.msgpack' in zf.namelist():
            print("Subjob Info:")
            try:
                import msgpack
                subjob_data = msgpack.unpackb(zf.read('subjob_metadata.msgpack'), raw=False)
                components = subjob_data.get('components', {})
                execution_order = subjob_data.get('execution_order', [])
                
                print(f"  Total Subjobs: {len(components)}")
                print(f"  Execution Order: {' -> '.join(execution_order)}")
                print("  Subjob Breakdown:")
                for subjob_id in execution_order:
                    comps = components.get(subjob_id, [])
                    print(f"    - {subjob_id}: {len(comps)} components")
                    for comp in comps[:3]:  # Show first 3 components
                        print(f"      * {comp}")
                    if len(comps) > 3:
                        print(f"      * ... and {len(comps) - 3} more")
                        
            except ImportError:
                print("  Install msgpack to see subjob details: pip install msgpack")
            except Exception as e:
                print(f"  Error reading subjobs: {e}")
            print()
        
        # Extract if requested
        if extract_to:
            extract_path = Path(extract_to)
            extract_path.mkdir(parents=True, exist_ok=True)
            zf.extractall(extract_path)
            print(f"Extracted to: {extract_path}")


def read_msgpack_file(file_path, summary_only=False):
    """Read and display a specific MessagePack file."""
    try:
        import msgpack
        with open(file_path, 'rb') as f:
            data = msgpack.unpackb(f.read(), raw=False)
        
        print(f"File: {file_path}")
        print("=" * 50)
        
        if summary_only:
            print_data_summary(data)
        else:
            print(json.dumps(data, indent=2, default=str))
            
    except ImportError:
        print("Error: msgpack package required. Install with: pip install msgpack")
    except Exception as e:
        print(f"Error reading {file_path}: {e}")


def print_data_summary(data):
    """Print a summary of the data structure."""
    if isinstance(data, dict):
        print("Dictionary with keys:")
        for key, value in data.items():
            if isinstance(value, list):
                print(f"  {key}: List with {len(value)} items")
            elif isinstance(value, dict):
                print(f"  {key}: Dict with {len(value)} keys")
            else:
                print(f"  {key}: {type(value).__name__} = {str(value)[:50]}...")
                
        # Special handling for DAG structure
        if 'nodes' in data and 'links' in data:
            print(f"\nGraph Structure:")
            print(f"  Nodes: {len(data['nodes'])}")
            print(f"  Links: {len(data['links'])}")
            
        # Special handling for subjob structure
        if 'components' in data:
            print(f"\nSubjob Structure:")
            for subjob_id, components in data['components'].items():
                print(f"  {subjob_id}: {len(components)} components")
                
    elif isinstance(data, list):
        print(f"List with {len(data)} items")
        if data:
            print(f"  First item type: {type(data[0]).__name__}")
    else:
        print(f"{type(data).__name__}: {str(data)[:100]}")


def main():
    if len(sys.argv) < 2:
        print("Usage:")
        print("  python inspect_pjob.py <pjob_file>                    # Inspect .pjob file")
        print("  python inspect_pjob.py <pjob_file> extract <dir>      # Extract contents")
        print("  python inspect_pjob.py <msgpack_file> msgpack         # Read .msgpack file")
        print("  python inspect_pjob.py <msgpack_file> msgpack summary # Read .msgpack summary")
        print("\nExamples:")
        print("  python inspect_pjob.py jobs\\my_job.pjob")
        print("  python inspect_pjob.py jobs\\my_job.pjob extract extracted")
        print("  python inspect_pjob.py extracted\\dag.msgpack msgpack")
        sys.exit(1)
    
    file_path = sys.argv[1]
    
    if not Path(file_path).exists():
        print(f"Error: File not found: {file_path}")
        sys.exit(1)
    
    # Handle different modes
    if len(sys.argv) >= 3 and sys.argv[2] == 'msgpack':
        # Read msgpack file directly
        summary_only = len(sys.argv) > 3 and sys.argv[3] == 'summary'
        read_msgpack_file(file_path, summary_only)
    elif len(sys.argv) >= 4 and sys.argv[2] == 'extract':
        # Extract pjob file
        extract_dir = sys.argv[3]
        inspect_pjob(file_path, extract_dir)
    else:
        # Inspect pjob file
        inspect_pjob(file_path)


if __name__ == "__main__":
    main()