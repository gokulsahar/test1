import click
from typing import Dict, List, Tuple, Any, Set
from collections import defaultdict, deque
import textwrap


class DAGVisualizer:
    """ASCII-based DAG visualization with multiple layout options."""
    
    def __init__(self, dag_data: Dict[str, Any]):
        """Initialize with DAG data from msgpack."""
        self.nodes = {node['id']: node for node in dag_data.get('nodes', [])}
        self.links = dag_data.get('links', [])
        self.subjobs = {}
        self.width = 80
        
    def set_subjob_data(self, subjob_data: Dict[str, Any]):
        """Add subjob information for grouping."""
        components = subjob_data.get('components', {})
        execution_order = subjob_data.get('execution_order', [])
        
        # Map components to subjobs
        self.subjobs = {}
        for subjob_id, comp_list in components.items():
            for comp in comp_list:
                self.subjobs[comp] = subjob_id
    
    def visualize(self, layout: str = "tree", show_metadata: bool = True, 
                  show_ports: bool = True, compact: bool = False):
        """Main visualization method with layout options."""
        click.echo("\n" + "=" * self.width)
        click.echo("DAG VISUALIZATION")
        click.echo("=" * self.width)
        
        if layout == "tree":
            self._show_tree_layout(show_metadata, show_ports, compact)
        elif layout == "flow":
            self._show_flow_layout(show_metadata, show_ports, compact)
        elif layout == "network":
            self._show_network_layout(show_metadata, show_ports, compact)
        else:
            click.echo(f"Unknown layout: {layout}")
            return
            
        self._show_legend()
    
    def _show_tree_layout(self, show_metadata: bool, show_ports: bool, compact: bool):
        """Hierarchical tree-like visualization."""
        click.echo("TREE LAYOUT (Hierarchical Flow)")
        click.echo("-" * 40)
        
        # Find root nodes (startable or no incoming edges)
        roots = self._find_root_nodes()
        
        if not roots:
            click.echo("No root nodes found!")
            return
        
        visited = set()
        for root in sorted(roots):
            if root not in visited:
                self._draw_tree_node(root, 0, visited, show_metadata, show_ports, compact)
                click.echo()
    
    def _show_flow_layout(self, show_metadata: bool, show_ports: bool, compact: bool):
        """Flow diagram with boxes and connections."""
        click.echo("FLOW LAYOUT (Box Diagram)")
        click.echo("-" * 40)
        
        # Group by subjobs if available
        if self.subjobs:
            self._show_subjob_grouped_flow(show_metadata, show_ports, compact)
        else:
            self._show_simple_flow(show_metadata, show_ports, compact)
    
    def _show_network_layout(self, show_metadata: bool, show_ports: bool, compact: bool):
        """Network-style showing all relationships."""
        click.echo("NETWORK LAYOUT (All Connections)")
        click.echo("-" * 40)
        
        # Show components first
        click.echo("COMPONENTS:")
        for node_id in sorted(self.nodes.keys()):
            self._draw_component_box(node_id, show_metadata, show_ports, compact)
            click.echo()
        
        # Show all connections
        click.echo("CONNECTIONS:")
        self._show_all_connections()
    
    def _find_root_nodes(self) -> List[str]:
        """Find nodes that can start execution."""
        # Nodes with no incoming data edges or marked as startable
        incoming_data = set()
        for link in self.links:
            if link.get('edge_type') == 'data':
                incoming_data.add(link['target'])
        
        roots = []
        for node_id, node_data in self.nodes.items():
            is_startable = node_data.get('startable', False)
            has_no_data_input = node_id not in incoming_data
            
            if is_startable or has_no_data_input:
                roots.append(node_id)
        
        return roots
    
    def _draw_tree_node(self, node_id: str, depth: int, visited: Set[str], 
                       show_metadata: bool, show_ports: bool, compact: bool, 
                       prefix: str = ""):
        """Recursively draw tree structure."""
        if node_id in visited:
            return
        visited.add(node_id)
        
        indent = "  " * depth
        node = self.nodes.get(node_id, {})
        
        # Draw the node
        if compact:
            self._draw_compact_node(node_id, indent + prefix)
        else:
            self._draw_detailed_node(node_id, indent + prefix, show_metadata, show_ports)
        
        # Find data output connections
        children = []
        for link in self.links:
            if link['source'] == node_id and link.get('edge_type') == 'data':
                children.append((link['target'], link))
        
        # Draw children
        for i, (child_id, link) in enumerate(sorted(children)):
            is_last = i == len(children) - 1
            child_prefix = "└── " if is_last else "├── "
            
            # Show connection info
            src_port = link.get('source_port', 'main')
            tgt_port = link.get('target_port', 'main')
            conn_info = f"[{src_port}→{tgt_port}] "
            
            self._draw_tree_node(child_id, depth + 1, visited, show_metadata, 
                               show_ports, compact, child_prefix + conn_info)
    
    def _draw_compact_node(self, node_id: str, prefix: str):
        """Draw compact node representation."""
        node = self.nodes.get(node_id, {})
        comp_type = node.get('component_type', 'unknown')
        
        # Collect flags
        flags = []
        if node.get('startable'): flags.append("START")
        if node.get('allow_multi_in'): flags.append("MULTI")
        if node.get('idempotent'): flags.append("IDEM")
        
        flag_str = f" [{','.join(flags)}]" if flags else ""
        subjob = self.subjobs.get(node_id, "")
        subjob_str = f" @{subjob}" if subjob else ""
        
        click.echo(f"{prefix}{node_id} ({comp_type}){flag_str}{subjob_str}")
    
    def _draw_detailed_node(self, node_id: str, prefix: str, show_metadata: bool, show_ports: bool):
        """Draw detailed node with full information."""
        node = self.nodes.get(node_id, {})
        
        # Node header
        comp_type = node.get('component_type', 'unknown')
        click.echo(f"{prefix}┌─ {node_id} ({comp_type})")
        
        # Flags and metadata
        if show_metadata:
            flags = []
            if node.get('startable'): flags.append("STARTABLE")
            if node.get('allow_multi_in'): flags.append("MULTI_IN")
            if node.get('idempotent'): flags.append("IDEMPOTENT")
            
            if flags:
                click.echo(f"{prefix}│  Flags: {', '.join(flags)}")
            
            subjob = self.subjobs.get(node_id)
            if subjob:
                click.echo(f"{prefix}│  Subjob: {subjob}")
        
        # Ports
        if show_ports:
            input_ports = node.get('input_ports', [])
            output_ports = node.get('output_ports', [])
            
            if input_ports:
                click.echo(f"{prefix}│  In:  {', '.join(input_ports)}")
            if output_ports:
                click.echo(f"{prefix}│  Out: {', '.join(output_ports)}")
        
        click.echo(f"{prefix}└─")
    
    def _show_subjob_grouped_flow(self, show_metadata: bool, show_ports: bool, compact: bool):
        """Show flow layout grouped by subjobs."""
        # Group components by subjob
        subjob_groups = defaultdict(list)
        for comp_id, subjob_id in self.subjobs.items():
            subjob_groups[subjob_id].append(comp_id)
        
        for subjob_id in sorted(subjob_groups.keys()):
            click.echo(f"\n╔══ SUBJOB: {subjob_id} ══")
            components = sorted(subjob_groups[subjob_id])
            
            for comp_id in components:
                self._draw_component_box(comp_id, show_metadata, show_ports, compact, "║  ")
            
            click.echo("╚" + "═" * (len(subjob_id) + 12))
        
        # Show inter-subjob connections
        click.echo("\nINTER-SUBJOB CONNECTIONS:")
        for link in self.links:
            src_subjob = self.subjobs.get(link['source'])
            tgt_subjob = self.subjobs.get(link['target'])
            
            if src_subjob and tgt_subjob and src_subjob != tgt_subjob:
                edge_type = link.get('edge_type', 'unknown')
                if edge_type == 'data':
                    src_port = link.get('source_port', 'main')
                    tgt_port = link.get('target_port', 'main')
                    click.echo(f"  {src_subjob}:{link['source']}.{src_port} ═══► {tgt_subjob}:{link['target']}.{tgt_port}")
                else:
                    trigger = link.get('trigger', 'unknown')
                    click.echo(f"  {src_subjob}:{link['source']} ──[{trigger}]──► {tgt_subjob}:{link['target']}")
    
    def _show_simple_flow(self, show_metadata: bool, show_ports: bool, compact: bool):
        """Show simple flow without subjob grouping."""
        for node_id in sorted(self.nodes.keys()):
            self._draw_component_box(node_id, show_metadata, show_ports, compact)
            click.echo()
    
    def _draw_component_box(self, node_id: str, show_metadata: bool, show_ports: bool, 
                          compact: bool, prefix: str = ""):
        """Draw a component as a box."""
        node = self.nodes.get(node_id, {})
        comp_type = node.get('component_type', 'unknown')
        
        if compact:
            # Compact box
            flags = []
            if node.get('startable'): flags.append("S")
            if node.get('allow_multi_in'): flags.append("M")
            if node.get('idempotent'): flags.append("I")
            flag_str = f"[{','.join(flags)}]" if flags else ""
            
            click.echo(f"{prefix}┌─ {node_id} ({comp_type}) {flag_str}")
            click.echo(f"{prefix}└─")
        else:
            # Detailed box
            max_width = max(len(node_id), len(comp_type)) + 4
            border = "─" * max_width
            
            click.echo(f"{prefix}┌{border}┐")
            click.echo(f"{prefix}│ {node_id:<{max_width-2}} │")
            click.echo(f"{prefix}│ ({comp_type})<{max_width-len(comp_type)-4} │")
            
            if show_metadata:
                flags = []
                if node.get('startable'): flags.append("START")
                if node.get('allow_multi_in'): flags.append("MULTI")
                if node.get('idempotent'): flags.append("IDEM")
                
                if flags:
                    flag_text = ', '.join(flags)
                    click.echo(f"{prefix}│ {flag_text:<{max_width-2}} │")
            
            if show_ports:
                input_ports = node.get('input_ports', [])
                output_ports = node.get('output_ports', [])
                
                if input_ports:
                    ports_text = f"In: {', '.join(input_ports)}"
                    click.echo(f"{prefix}│ {ports_text:<{max_width-2}} │")
                
                if output_ports:
                    ports_text = f"Out: {', '.join(output_ports)}"
                    click.echo(f"{prefix}│ {ports_text:<{max_width-2}} │")
            
            click.echo(f"{prefix}└{border}┘")
    
    def _show_all_connections(self):
        """Show all connections in network layout."""
        # Group by connection type
        data_connections = []
        control_connections = []
        
        for link in self.links:
            edge_type = link.get('edge_type', 'unknown')
            source = link['source']
            target = link['target']
            
            if edge_type == 'data':
                src_port = link.get('source_port', 'main')
                tgt_port = link.get('target_port', 'main')
                data_connections.append(f"  {source}.{src_port} ══════► {target}.{tgt_port}")
            elif edge_type == 'control':
                trigger = link.get('trigger', 'unknown')
                condition = link.get('condition', '')
                if condition:
                    control_connections.append(f"  {source} ──[{trigger}: {condition}]──► {target}")
                else:
                    control_connections.append(f"  {source} ──[{trigger}]──► {target}")
        
        if data_connections:
            click.echo("\n  DATA FLOW:")
            for conn in sorted(data_connections):
                click.echo(conn)
        
        if control_connections:
            click.echo("\n  CONTROL FLOW:")
            for conn in sorted(control_connections):
                click.echo(conn)
    
    def _show_legend(self):
        """Show legend explaining symbols."""
        click.echo("\n" + "=" * self.width)
        click.echo("LEGEND")
        click.echo("=" * self.width)
        click.echo("Symbols:")
        click.echo("  ══════►  Data flow connection")
        click.echo("  ──[X]──► Control flow (X = trigger type)")
        click.echo("  ┌─┐ └─┘  Component boxes")
        click.echo("  ├── └──  Tree structure")
        click.echo("  ╔══ ╚══  Subjob grouping")
        click.echo()
        click.echo("Flags:")
        click.echo("  START    Startable component")
        click.echo("  MULTI    Allows multiple inputs")
        click.echo("  IDEM     Idempotent operation")
        click.echo("  S/M/I    Compact flag notation")
        click.echo()
        click.echo("Triggers:")
        click.echo("  ok       Normal completion")
        click.echo("  error    Error occurred")
        click.echo("  if       Conditional execution")
        click.echo("  parallel Parallel branch start")
        click.echo("  sync     Synchronization point")
        click.echo("=" * self.width)


def visualize_dag_interactive(dag_data: Dict[str, Any], subjob_data: Dict[str, Any] = None):
    """Interactive DAG visualization with user options."""
    visualizer = DAGVisualizer(dag_data)
    
    if subjob_data:
        visualizer.set_subjob_data(subjob_data)
    
    # Show options
    click.echo("\nDAG Visualization Options:")
    click.echo("1. Tree Layout (hierarchical)")
    click.echo("2. Flow Layout (boxes with grouping)")
    click.echo("3. Network Layout (all connections)")
    click.echo("4. All layouts")
    click.echo("5. Quick summary")
    
    choice = click.prompt("Select layout", type=int, default=1)
    
    # Additional options
    show_metadata = click.confirm("Show component metadata?", default=True)
    show_ports = click.confirm("Show input/output ports?", default=True)
    compact = click.confirm("Use compact view?", default=False)
    
    # Execute visualization
    if choice == 1:
        visualizer.visualize("tree", show_metadata, show_ports, compact)
    elif choice == 2:
        visualizer.visualize("flow", show_metadata, show_ports, compact)
    elif choice == 3:
        visualizer.visualize("network", show_metadata, show_ports, compact)
    elif choice == 4:
        # Show all layouts
        visualizer.visualize("tree", show_metadata, show_ports, compact)
        click.echo("\n" + "=" * 80 + "\n")
        visualizer.visualize("flow", show_metadata, show_ports, compact)
        click.echo("\n" + "=" * 80 + "\n")
        visualizer.visualize("network", show_metadata, show_ports, compact)
    elif choice == 5:
        # Quick summary
        _show_dag_quick_summary(dag_data, subjob_data)
    else:
        click.echo("Invalid choice, showing tree layout...")
        visualizer.visualize("tree", show_metadata, show_ports, compact)


def _show_dag_quick_summary(dag_data: Dict[str, Any], subjob_data: Dict[str, Any] = None):
    """Show quick DAG summary."""
    nodes = dag_data.get('nodes', [])
    links = dag_data.get('links', [])
    
    click.echo("\nQUICK DAG SUMMARY")
    click.echo("=" * 40)
    click.echo(f"Components: {len(nodes)}")
    click.echo(f"Connections: {len(links)}")
    
    # Component types
    types = defaultdict(int)
    startable = []
    for node in nodes:
        comp_type = node.get('component_type', 'unknown')
        types[comp_type] += 1
        if node.get('startable'):
            startable.append(node['id'])
    
    click.echo(f"Component Types: {dict(types)}")
    click.echo(f"Startable: {', '.join(startable)}")
    
    # Connection types
    data_count = sum(1 for link in links if link.get('edge_type') == 'data')
    control_count = sum(1 for link in links if link.get('edge_type') == 'control')
    
    click.echo(f"Data Connections: {data_count}")
    click.echo(f"Control Connections: {control_count}")
    
    if subjob_data:
        components = subjob_data.get('components', {})
        click.echo(f"Subjobs: {len(components)}")
        execution_order = subjob_data.get('execution_order', [])
        click.echo(f"Execution Order: {' → '.join(execution_order)}")