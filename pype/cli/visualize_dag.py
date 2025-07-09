import networkx as nx
from collections import defaultdict
from typing import Dict, List, Tuple


def print_dag_as_text(dag: nx.DiGraph) -> None:
    """Print DAG structure as formatted text (no Graphviz required)."""
    
    print("=" * 60)
    print("DAG STRUCTURE (Text View)")
    print("=" * 60)
    
    # 1. Component Summary
    print("\n1. COMPONENTS:")
    print("-" * 30)
    
    components_by_category = defaultdict(list)
    startable_components = []
    
    for node, data in dag.nodes(data=True):
        category = data.get('registry_metadata', {}).get('category', 'unknown')
        comp_type = data.get('component_type', 'unknown')
        startable = data.get('startable', False)
        
        components_by_category[category].append((node, comp_type, startable))
        if startable:
            startable_components.append(node)
    
    for category, components in sorted(components_by_category.items()):
        print(f"\n  {category.upper()}:")
        for comp_name, comp_type, startable in sorted(components):
            start_flag = " [START]" if startable else ""
            print(f"    - {comp_name} ({comp_type}){start_flag}")
    
    # 2. Data Flow
    print("\n\n2. DATA FLOW:")
    print("-" * 30)
    
    data_edges = [(s, t, d) for s, t, d in dag.edges(data=True) if d.get('edge_type') == 'data']
    if data_edges:
        for source, target, edge_data in sorted(data_edges):
            src_port = edge_data.get('source_port', 'main')
            tgt_port = edge_data.get('target_port', 'main')
            print(f"  {source}.{src_port} --> {target}.{tgt_port}")
    else:
        print("  No data connections")
    
    # 3. Control Flow
    print("\n\n3. CONTROL FLOW:")
    print("-" * 30)
    
    control_edges = [(s, t, d) for s, t, d in dag.edges(data=True) if d.get('edge_type') == 'control']
    if control_edges:
        control_by_type = defaultdict(list)
        
        for source, target, edge_data in sorted(control_edges):
            trigger = edge_data.get('trigger', 'unknown')
            condition = edge_data.get('condition', '')
            
            if trigger == 'if' and condition:
                control_by_type['Conditional'].append(f"{source} --[if: {condition}]--> {target}")
            elif trigger in ['parallelise', 'synchronise']:
                control_by_type['Parallel'].append(f"{source} --[{trigger}]--> {target}")
            elif trigger in ['subjob_ok', 'subjob_error']:
                control_by_type['Subjob'].append(f"{source} --[{trigger}]--> {target}")
            else:
                control_by_type['Basic'].append(f"{source} --[{trigger}]--> {target}")
        
        for flow_type, edges in sorted(control_by_type.items()):
            print(f"\n  {flow_type}:")
            for edge in sorted(edges):
                print(f"    {edge}")
    else:
        print("  No control flow")
    
    # 4. Execution Path
    print("\n\n4. EXECUTION PATH:")
    print("-" * 30)
    
    if startable_components:
        print(f"  Starting points: {', '.join(sorted(startable_components))}")
        
        # Show topological order if acyclic
        try:
            if nx.is_directed_acyclic_graph(dag):
                topo_order = list(nx.topological_sort(dag))
                print(f"  Topological order: {' -> '.join(topo_order)}")
            else:
                print("  Warning: DAG contains cycles!")
        except:
            print("  Could not determine execution order")
    else:
        print("  No starting points found!")
    
    # 5. Statistics
    print("\n\n5. STATISTICS:")
    print("-" * 30)
    print(f"  Total components: {len(dag.nodes())}")
    print(f"  Total connections: {len(dag.edges())}")
    print(f"  Data edges: {len([e for e in dag.edges(data=True) if e[2].get('edge_type') == 'data'])}")
    print(f"  Control edges: {len([e for e in dag.edges(data=True) if e[2].get('edge_type') == 'control'])}")
    print(f"  Startable components: {len(startable_components)}")
    
    # Check for common patterns
    has_parallel = any(d.get('trigger') == 'parallelise' for _, _, d in dag.edges(data=True))
    has_conditionals = any(d.get('trigger') == 'if' for _, _, d in dag.edges(data=True))
    has_subjobs = any(d.get('trigger') in ['subjob_ok', 'subjob_error'] for _, _, d in dag.edges(data=True))
    
    print(f"  Uses parallelization: {'Yes' if has_parallel else 'No'}")
    print(f"  Uses conditionals: {'Yes' if has_conditionals else 'No'}")
    print(f"  Uses subjobs: {'Yes' if has_subjobs else 'No'}")


def print_subjob_structure(subjob_components: Dict[str, List[str]], 
                          subjob_execution_order: List[str]) -> None:
    """Print subjob structure in text format."""
    
    print("\n\n6. SUBJOB STRUCTURE:")
    print("-" * 30)
    
    if not subjob_components:
        print("  No subjobs detected")
        return
    
    print(f"  Total subjobs: {len(subjob_components)}")
    print(f"  Execution order: {' -> '.join(subjob_execution_order)}")
    print()
    
    for i, subjob_id in enumerate(subjob_execution_order, 1):
        components = subjob_components.get(subjob_id, [])
        print(f"  {i}. {subjob_id}:")
        if components:
            for comp in sorted(components):
                print(f"     - {comp}")
        else:
            print("     (empty)")
        print()


def analyze_dag_text(dag: nx.DiGraph, subjob_components: Dict[str, List[str]] = None,
                    subjob_execution_order: List[str] = None) -> None:
    """Complete text-based DAG analysis."""
    
    print_dag_as_text(dag)
    
    if subjob_components and subjob_execution_order:
        print_subjob_structure(subjob_components, subjob_execution_order)
    
    print("=" * 60)


def create_simple_ascii_dag(dag: nx.DiGraph) -> str:
    """Create a simple ASCII representation of the DAG."""
    
    # Find root nodes (no incoming edges or startable)
    roots = [n for n in dag.nodes() if dag.in_degree(n) == 0 or dag.nodes[n].get('startable', False)]
    
    if not roots:
        return "No root nodes found"
    
    # Build tree-like structure
    result = []
    visited = set()
    
    def print_node(node, depth=0, prefix=""):
        if node in visited:
            return
        visited.add(node)
        
        indent = "  " * depth
        result.append(f"{indent}{prefix}{node}")
        
        # Get children (data flow)
        children = []
        for _, target, data in dag.edges(node, data=True):
            if data.get('edge_type') == 'data':
                children.append(target)
        
        for i, child in enumerate(sorted(children)):
            is_last = i == len(children) - 1
            child_prefix = "└── " if is_last else "├── "
            print_node(child, depth + 1, child_prefix)
    
    result.append("ASCII DAG (Data Flow Only):")
    result.append("-" * 30)
    
    for root in sorted(roots):
        print_node(root)
        result.append("")
    
    return "\n".join(result)


if __name__ == "__main__":
    # Test with a simple DAG
    test_dag = nx.DiGraph()
    test_dag.add_node("start", component_type="logging", startable=True)
    test_dag.add_node("process", component_type="transform", startable=False)
    test_dag.add_node("end", component_type="logging", startable=False)
    
    test_dag.add_edge("start", "process", edge_type="data", source_port="main", target_port="input")
    test_dag.add_edge("process", "end", edge_type="data", source_port="output", target_port="main")
    test_dag.add_edge("start", "process", edge_type="control", trigger="ok")
    
    analyze_dag_text(test_dag)