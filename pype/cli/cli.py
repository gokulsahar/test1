import zipfile
import json
import click
from pathlib import Path


def inspect_pjob_cli(pjob_file, extract_to=None, show_yaml=False, show_dag=False):
    """CLI wrapper for .pjob file inspection with Click integration."""
    pjob_path = Path(pjob_file)
    
    if not pjob_path.exists():
        click.echo(f"Error: {pjob_path} not found")
        raise click.Abort()
    
    click.echo(f"Inspecting: {pjob_path}")
    click.echo("=" * 60)
    
    with zipfile.ZipFile(pjob_path, 'r') as zf:
        # List all files
        click.echo("Contents:")
        total_size = 0
        for info in zf.infolist():
            size_kb = info.file_size / 1024
            total_size += info.file_size
            click.echo(f"  {info.filename:<25} {size_kb:>8.1f} KB")
        click.echo(f"  Total size: {total_size/1024:.1f} KB")
        click.echo()
        
        # Show manifest
        if 'manifest.json' in zf.namelist():
            click.echo("Manifest:")
            try:
                manifest = json.loads(zf.read('manifest.json'))
                for key, value in manifest.items():
                    if key == 'checksums':
                        click.echo(f"  {key}: {len(value)} files")
                    else:
                        click.echo(f"  {key}: {value}")
            except Exception as e:
                click.echo(f"  Error reading manifest: {e}")
            click.echo()
        
        # Show original YAML
        if 'job_original.yaml' in zf.namelist():
            if show_yaml:
                click.echo("Original YAML (complete):")
                try:
                    yaml_content = zf.read('job_original.yaml').decode('utf-8')
                    for line in yaml_content.split('\n'):
                        click.echo(f"  {line}")
                except Exception as e:
                    click.echo(f"  Error reading YAML: {e}")
            else:
                click.echo("Original YAML (first 10 lines):")
                try:
                    yaml_content = zf.read('job_original.yaml').decode('utf-8')
                    lines = yaml_content.split('\n')[:10]
                    for line in lines:
                        click.echo(f"  {line}")
                    if len(yaml_content.split('\n')) > 10:
                        click.echo(f"  ... ({len(yaml_content.split('\n'))} total lines)")
                        click.echo("  Use --show-yaml to see complete content")
                except Exception as e:
                    click.echo(f"  Error reading YAML: {e}")
            click.echo()
        
        # Show DAG summary or detailed info
        if 'dag.msgpack' in zf.namelist():
            if show_dag:
                _show_detailed_dag_info(zf)
            else:
                _show_dag_summary(zf)
            click.echo()
            
        # Show execution metadata
        if 'execution_metadata.msgpack' in zf.namelist():
            _show_execution_metadata(zf)
            click.echo()
        
        # Show subjob info
        if 'subjob_metadata.msgpack' in zf.namelist():
            _show_subjob_info(zf)
            click.echo()
        
        # Extract if requested
        if extract_to:
            extract_path = Path(extract_to)
            extract_path.mkdir(parents=True, exist_ok=True)
            zf.extractall(extract_path)
            click.echo(f"Extracted to: {extract_path}")


def _show_dag_summary(zf):
    """Show basic DAG summary."""
    click.echo("DAG Summary:")
    try:
        import msgpack
        dag_data = msgpack.unpackb(zf.read('dag.msgpack'), raw=False)
        nodes = dag_data.get('nodes', [])
        links = dag_data.get('links', [])
        click.echo(f"  Nodes (Components): {len(nodes)}")
        click.echo(f"  Links (Connections): {len(links)}")
        
        # Show component types
        if nodes:
            click.echo("  Component Types:")
            types = {}
            for node in nodes:
                comp_type = node.get('component_type', 'unknown')
                types[comp_type] = types.get(comp_type, 0) + 1
            for comp_type, count in sorted(types.items()):
                click.echo(f"    - {comp_type}: {count}")
        
        # Show first few nodes
        click.echo("  First 3 Components:")
        for node in nodes[:3]:
            node_id = node.get('id', 'unknown')
            comp_type = node.get('component_type', 'unknown')
            startable = "[START]" if node.get('startable') else ""
            click.echo(f"    - {node_id} ({comp_type}) {startable}")
        
        if len(nodes) > 3:
            click.echo("    Use --show-dag for complete details")
                
    except ImportError:
        click.echo("  Install msgpack to see DAG details: pip install msgpack")
    except Exception as e:
        click.echo(f"  Error reading DAG: {e}")


def _show_detailed_dag_info(zf):
    """Show detailed DAG information with visualization."""
    try:
        import msgpack
        dag_data = msgpack.unpackb(zf.read('dag.msgpack'), raw=False)
        
        # Try to get subjob data if available
        subjob_data = None
        if 'subjob_metadata.msgpack' in zf.namelist():
            subjob_data = msgpack.unpackb(zf.read('subjob_metadata.msgpack'), raw=False)
        
        # Import and use the visualizer
        from pype.cli.dag_visualizer import visualize_dag_interactive
        visualize_dag_interactive(dag_data, subjob_data)
                
    except ImportError as e:
        if 'msgpack' in str(e):
            click.echo("  Install msgpack to see DAG details: pip install msgpack")
        else:
            click.echo(f"  Error importing visualizer: {e}")
            # Fallback to simple display
            _show_simple_dag_fallback(zf)
    except Exception as e:
        click.echo(f"  Error reading DAG: {e}")
        _show_simple_dag_fallback(zf)


def _show_simple_dag_fallback(zf):
    """Fallback simple DAG display if visualizer fails."""
    try:
        import msgpack
        dag_data = msgpack.unpackb(zf.read('dag.msgpack'), raw=False)
        nodes = dag_data.get('nodes', [])
        links = dag_data.get('links', [])
        
        click.echo("DAG Details (Simple View):")
        click.echo(f"  Total Nodes: {len(nodes)}")
        click.echo(f"  Total Links: {len(links)}")
        click.echo()
        
        # Show all nodes
        click.echo("  All Components:")
        for node in nodes:
            node_id = node.get('id', 'unknown')
            comp_type = node.get('component_type', 'unknown')
            startable = "[START]" if node.get('startable') else ""
            allow_multi = "[MULTI]" if node.get('allow_multi_in') else ""
            idempotent = "[IDEM]" if node.get('idempotent') else ""
            flags = f" {startable}{allow_multi}{idempotent}".strip()
            click.echo(f"    - {node_id} ({comp_type}){flags}")
        click.echo()
        
        # Show all links
        click.echo("  All Connections:")
        data_links = []
        control_links = []
        
        for link in links:
            source = link.get('source', 'unknown')
            target = link.get('target', 'unknown')
            link_type = link.get('edge_type', 'unknown')
            
            if link_type == 'data':
                src_port = link.get('source_port', 'main')
                tgt_port = link.get('target_port', 'main')
                data_links.append(f"    {source}.{src_port} --> {target}.{tgt_port}")
            elif link_type == 'control':
                trigger = link.get('trigger', 'unknown')
                control_links.append(f"    {source} --({trigger})--> {target}")
        
        if data_links:
            click.echo("    Data Flow:")
            for link in sorted(data_links):
                click.echo(link)
        
        if control_links:
            click.echo("    Control Flow:")
            for link in sorted(control_links):
                click.echo(link)
                
    except Exception as e:
        click.echo(f"  Fallback display also failed: {e}")


def _show_execution_metadata(zf):
    """Show execution metadata."""
    click.echo("Execution Metadata:")
    try:
        import msgpack
        exec_data = msgpack.unpackb(zf.read('execution_metadata.msgpack'), raw=False)
        
        startable = exec_data.get('startable_components', [])
        resources = exec_data.get('resource_requirements', {})
        runtime = exec_data.get('estimated_execution_time', 0)
        
        click.echo(f"  Startable Components: {', '.join(startable)}")
        click.echo(f"  Estimated Runtime: {runtime} seconds")
        click.echo(f"  Memory Requirements: {resources.get('memory_requirements_mb', 0)} MB")
        click.echo(f"  CPU Intensity: {resources.get('cpu_intensity', 'unknown')}")
        click.echo(f"  IO Intensity: {resources.get('io_intensity', 'unknown')}")
        
        # Show parallel execution plan if available
        parallel_plan = exec_data.get('parallel_execution_plan', {})
        if parallel_plan:
            max_parallel = parallel_plan.get('max_parallelism', 1)
            exec_waves = parallel_plan.get('execution_waves', 1)
            click.echo(f"  Max Parallelism: {max_parallel}")
            click.echo(f"  Execution Waves: {exec_waves}")
                
    except ImportError:
        click.echo("  Install msgpack to see execution details: pip install msgpack")
    except Exception as e:
        click.echo(f"  Error reading execution metadata: {e}")


def _show_subjob_info(zf):
    """Show subjob information."""
    click.echo("Subjob Info:")
    try:
        import msgpack
        subjob_data = msgpack.unpackb(zf.read('subjob_metadata.msgpack'), raw=False)
        components = subjob_data.get('components', {})
        execution_order = subjob_data.get('execution_order', [])
        
        click.echo(f"  Total Subjobs: {len(components)}")
        click.echo(f"  Execution Order: {' -> '.join(execution_order)}")
        click.echo("  Subjob Breakdown:")
        
        for subjob_id in execution_order:
            comps = components.get(subjob_id, [])
            click.echo(f"    - {subjob_id}: {len(comps)} components")
            for comp in comps[:3]:  # Show first 3 components
                click.echo(f"      * {comp}")
            if len(comps) > 3:
                click.echo(f"      * ... and {len(comps) - 3} more")
                
    except ImportError:
        click.echo("  Install msgpack to see subjob details: pip install msgpack")
    except Exception as e:
        click.echo(f"  Error reading subjobs: {e}")


def read_msgpack_file_cli(file_path, summary_only=False):
    """CLI wrapper for reading MessagePack files directly."""
    try:
        import msgpack
        with open(file_path, 'rb') as f:
            data = msgpack.unpackb(f.read(), raw=False)
        
        click.echo(f"File: {file_path}")
        click.echo("=" * 50)
        
        if summary_only:
            _print_data_summary(data)
        else:
            click.echo(json.dumps(data, indent=2, default=str))
            
    except ImportError:
        click.echo("Error: msgpack package required. Install with: pip install msgpack")
        raise click.Abort()
    except Exception as e:
        click.echo(f"Error reading {file_path}: {e}")
        raise click.Abort()


def _print_data_summary(data):
    """Print a summary of the data structure."""
    if isinstance(data, dict):
        click.echo("Dictionary with keys:")
        for key, value in data.items():
            if isinstance(value, list):
                click.echo(f"  {key}: List with {len(value)} items")
            elif isinstance(value, dict):
                click.echo(f"  {key}: Dict with {len(value)} keys")
            else:
                value_str = str(value)[:50]
                if len(str(value)) > 50:
                    value_str += "..."
                click.echo(f"  {key}: {type(value).__name__} = {value_str}")
                
        # Special handling for DAG structure
        if 'nodes' in data and 'links' in data:
            click.echo(f"\nGraph Structure:")
            click.echo(f"  Nodes: {len(data['nodes'])}")
            click.echo(f"  Links: {len(data['links'])}")
            
        # Special handling for subjob structure
        if 'components' in data:
            click.echo(f"\nSubjob Structure:")
            for subjob_id, components in data['components'].items():
                click.echo(f"  {subjob_id}: {len(components)} components")
                
    elif isinstance(data, list):
        click.echo(f"List with {len(data)} items")
        if data:
            click.echo(f"  First item type: {type(data[0]).__name__}")
    else:
        data_str = str(data)[:100]
        if len(str(data)) > 100:
            data_str += "..."
        click.echo(f"{type(data).__name__}: {data_str}")