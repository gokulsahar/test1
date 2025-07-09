import networkx as nx
from pathlib import Path
from typing import Optional
import tempfile
import subprocess
import sys


def visualize_dag(dag: nx.DiGraph, output_path: Optional[Path] = None, 
                  format: str = "png", view: bool = True) -> Path:
    """
    Create a visual representation of the DAG using Graphviz.
    
    Args:
        dag: NetworkX DiGraph to visualize
        output_path: Output file path (if None, creates temp file)
        format: Output format (png, svg, pdf, dot)
        view: Whether to open the file after creation
        
    Returns:
        Path to the created visualization file
    """
    try:
        import graphviz
    except ImportError:
        raise ImportError("graphviz package required for visualization. Install with: pip install graphviz")
    
    # Create graphviz digraph
    dot = graphviz.Digraph(comment='DataPY Job DAG')
    dot.attr(rankdir='TB', size='12,8')
    dot.attr('node', shape='box', style='rounded,filled')
    
    # Add nodes with styling based on component type
    for node, data in dag.nodes(data=True):
        component_type = data.get('component_type', 'unknown')
        category = data.get('registry_metadata', {}).get('category', 'unknown')
        startable = data.get('startable', False)
        
        # Color coding
        if startable:
            color = 'lightgreen'
        elif category == 'io':
            color = 'lightblue'
        elif category == 'control':
            color = 'lightyellow'
        elif category == 'transform':
            color = 'lightcoral'
        else:
            color = 'lightgray'
        
        # Node label with type info
        label = f"{node}\\n({component_type})"
        if startable:
            label += "\\n[START]"
            
        dot.node(node, label=label, fillcolor=color)
    
    # Add edges with styling based on edge type
    for source, target, data in dag.edges(data=True):
        edge_type = data.get('edge_type', 'unknown')
        
        if edge_type == 'data':
            # Data edges: solid blue lines
            source_port = data.get('source_port', 'main')
            target_port = data.get('target_port', 'main')
            label = f"{source_port} → {target_port}"
            dot.edge(source, target, label=label, color='blue', style='solid')
            
        elif edge_type == 'control':
            # Control edges: dashed red lines with trigger info
            trigger = data.get('trigger', 'unknown')
            condition = data.get('condition', '')
            
            if trigger == 'if':
                label = f"if: {condition}"
                color = 'orange'
            elif trigger in ['parallelise', 'synchronise']:
                label = trigger
                color = 'purple'
            elif trigger in ['subjob_ok', 'subjob_error']:
                label = trigger
                color = 'darkgreen'
            else:
                label = trigger
                color = 'red'
                
            dot.edge(source, target, label=label, color=color, style='dashed')
    
    # Generate output file
    if output_path is None:
        temp_dir = tempfile.mkdtemp()
        output_path = Path(temp_dir) / f"dag_visualization.{format}"
    else:
        output_path = Path(output_path)
    
    # Render the graph
    dot.render(str(output_path.with_suffix('')), format=format, cleanup=True)
    final_path = output_path.with_suffix(f'.{format}')
    
    # Open file if requested
    if view:
        try:
            if sys.platform.startswith('darwin'):  # macOS
                subprocess.call(['open', str(final_path)])
            elif sys.platform.startswith('win'):   # Windows
                subprocess.call(['start', str(final_path)], shell=True)
            else:  # Linux
                subprocess.call(['xdg-open', str(final_path)])
        except Exception as e:
            print(f"Could not open file automatically: {e}")
            print(f"Visualization saved to: {final_path}")
    
    return final_path


def create_legend() -> str:
    """Create a legend explaining the visualization symbols."""
    return """
DAG Visualization Legend:
========================

Node Colors:
  • Light Green: Startable components (entry points)
  • Light Blue: IO components (input/output)
  • Light Yellow: Control components (logging, iteration)
  • Light Coral: Transform components (data processing)
  • Light Gray: Other/Unknown components

Edge Types:
  • Blue Solid Lines: Data flow (with port names)
  • Red Dashed Lines: Control flow (ok, error)
  • Orange Dashed Lines: Conditional flow (if conditions)
  • Purple Dashed Lines: Parallel flow (parallelise, synchronise)
  • Dark Green Dashed Lines: Subjob flow (subjob_ok, subjob_error)

Node Labels:
  • Component Name
  • (component_type)
  • [START] for startable components
"""