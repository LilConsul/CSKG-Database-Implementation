import time

from core.message_handler import error_print, json_print, verbose_print

from .queries import SIMILAR_NODES_QUERY
from .utils import dgraph_read


def edge_types_match(edge_type1, edge_type2):
    """
    Check if edge types match, handling both string and list values.

    Returns True if:
    - Both are strings and are equal
    - Both are lists and have at least one common element
    - One is string, one is list, and the string is in the list
    """
    # Both are strings
    if isinstance(edge_type1, str) and isinstance(edge_type2, str):
        return edge_type1 == edge_type2

    # Both are lists
    elif isinstance(edge_type1, list) and isinstance(edge_type2, list):
        return any(elem in edge_type2 for elem in edge_type1)

    # One string, one list
    elif isinstance(edge_type1, str) and isinstance(edge_type2, list):
        return edge_type1 in edge_type2

    elif isinstance(edge_type1, list) and isinstance(edge_type2, str):
        return edge_type2 in edge_type1

    return False


def fetch_node_data(node_id):
    """Fetch node data from the database."""
    results = dgraph_read(
        SIMILAR_NODES_QUERY,
        variables={"$id": node_id},
    )

    nodes = results.get("node_info", [])
    if not nodes:
        error_print(f"No nodes found for ID {node_id}")
        return None

    return nodes[0]


def find_similar_nodes_via_successors(node, similar_nodes):
    """Find similar nodes by examining common successors."""
    node_id = node.get("id")

    for successor in node.get("to", []):
        successor_id = successor.get("id")
        edge_type_to_successor = successor.get("to|id")

        if successor_id == node_id:
            continue

        for sub_predecessor in successor.get("~to", []):
            sub_predecessor_id = sub_predecessor.get("id")
            edge_type_sub = sub_predecessor.get("~to|id")

            if sub_predecessor_id != node_id and edge_types_match(
                edge_type_to_successor, edge_type_sub
            ):
                add_to_similar_nodes(
                    similar_nodes,
                    sub_predecessor_id,
                    sub_predecessor.get("label"),
                    successor_id,
                    edge_type_to_successor,
                )


def find_similar_nodes_via_predecessors(node, similar_nodes):
    """Find similar nodes by examining common predecessors."""
    node_id = node.get("id")

    for predecessor in node.get("~to", []):
        predecessor_id = predecessor.get("id")
        edge_type_from_predecessor = predecessor.get("~to|id")

        if predecessor_id == node_id:
            continue

        for sub_successor in predecessor.get("to", []):
            sub_successor_id = sub_successor.get("id")
            edge_type_sub = sub_successor.get("to|id")

            if sub_successor_id != node_id and edge_types_match(
                edge_type_from_predecessor, edge_type_sub
            ):
                add_to_similar_nodes(
                    similar_nodes,
                    sub_successor_id,
                    sub_successor.get("label"),
                    predecessor_id,
                    edge_type_from_predecessor,
                )


def add_to_similar_nodes(similar_nodes, node_id, label, via_node, edge_type):
    """Add a node to the similar_nodes dictionary."""
    similar_nodes.setdefault(
        node_id,
        {
            "id": node_id,
            "label": label,
            "shared_connections": [],
        },
    )
    similar_nodes[node_id]["shared_connections"].append(
        {
            "via_node": via_node,
            "edge_type": edge_type,
        }
    )


def format_and_output_results(similar_nodes, start_time):
    """Format the results and print them."""
    result = {"similar_nodes": list(similar_nodes.values())}
    end_time = time.time()
    json_print(result)
    verbose_print(f"Query executed in {end_time - start_time:.2f} seconds")


def get_similar_nodes(node_id):
    """Find all 'similar' nodes that share common parents or children via the same edge type."""
    try:
        start_time = time.time()

        # Fetch node data
        node_data = fetch_node_data(node_id)
        if not node_data:
            return

        # Process connections to find similar nodes
        similar_nodes = {}
        find_similar_nodes_via_successors(node_data, similar_nodes)
        find_similar_nodes_via_predecessors(node_data, similar_nodes)

        # Format and output results
        format_and_output_results(similar_nodes, start_time)

    except Exception as error:
        error_print("finding similar nodes", error)
