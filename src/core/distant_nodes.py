import time
from collections import deque

from .message_handler import error_print, json_print, verbose_print
from .queries import DISTANT_SYNONYMS_ANTONYM
from .utils import dgraph_read


def dgraph_query(node_id, distance):
    """Execute the graph database query and return results."""
    try:
        return dgraph_read(
            DISTANT_SYNONYMS_ANTONYM,
            variables={"$id": str(node_id), "$distance": str(distance + 1)},
        )
    except Exception as error:
        error_print(f"querying graph for node {node_id}", error)
        raise  # Re-raise the exception to be caught by the calling function


def build_relationship_graph(results):
    """Build a graph representation from query results."""
    graph = {}
    nodes_info = {}

    # Process the root nodes from results
    distant_nodes = results.get("distant_nodes", [])
    for node in distant_nodes:
        extract_nodes_recursive(node, graph, nodes_info)

    return graph, nodes_info


def extract_nodes_recursive(data, graph, nodes_info):
    """Extract nodes and their relationships recursively."""
    if not isinstance(data, dict):
        return

    if not ("id" in data and "label" in data):
        return

    current_id = data["id"]
    nodes_info[current_id] = data["label"]

    process_relationships(
        data, current_id, ["synonym~", "synonym"], True, graph, nodes_info
    )

    process_relationships(
        data, current_id, ["antonym~", "antonym"], False, graph, nodes_info
    )


def process_relationships(data, current_id, rel_types, is_synonym, graph, nodes_info):
    """Process relationship data for a given node."""
    for rel_type in rel_types:
        if rel_type not in data:
            continue

        relationships = data[rel_type]
        if not isinstance(relationships, list):
            continue

        for related in relationships:
            if not (isinstance(related, dict) and "id" in related):
                continue

            related_id = related["id"]

            # Store node label information
            if related_id not in nodes_info and "label" in related:
                nodes_info[related_id] = related["label"]

            # Add edges to graph (both directions)
            if current_id not in graph:
                graph[current_id] = []
            if related_id not in graph:
                graph[related_id] = []

            graph[current_id].append((related_id, is_synonym))
            graph[related_id].append((current_id, is_synonym))

            # Recurse into related node
            extract_nodes_recursive(related, graph, nodes_info)


def find_nodes_at_distance(node_id, distance, graph):
    """Find all nodes at a specific distance, tracking synonym/antonym status."""
    queue = deque([(node_id, 0, True)])  # node_id, distance, is_synonym
    visited = {node_id: (0, True)}  # node_id -> (distance, is_synonym)

    while queue:
        current_id, curr_dist, is_synonym = queue.popleft()

        # Skip further traversal if we've reached our distance
        if curr_dist >= distance:
            continue

        # Process neighbors
        for neighbor_id, is_synonym_edge in graph.get(current_id, []):
            # Calculate if neighbor is synonym of root node:
            # Synonym of synonym is a synonym
            # Antonym of antonym is a synonym
            # Synonym of antonym is an antonym
            neighbor_is_synonym = is_synonym if is_synonym_edge else not is_synonym

            # Update if this is a new node or we found a shorter path
            if neighbor_id not in visited or curr_dist + 1 < visited[neighbor_id][0]:
                visited[neighbor_id] = (curr_dist + 1, neighbor_is_synonym)
                queue.append((neighbor_id, curr_dist + 1, neighbor_is_synonym))

    return visited


def filter_results(visited, nodes_info, distance, node_id, want_synonyms=True):
    """Filter nodes at target distance based on synonym/antonym status."""
    results = []
    for node, (dist, is_synonym) in visited.items():
        # Skip if not at target distance or it's the original node
        if dist != distance or node == node_id:
            continue

        # Include only synonyms or only antonyms based on want_synonyms
        if is_synonym == want_synonyms:
            results.append({"id": node, "label": nodes_info.get(node, node)})

    return results


def find_distant_relationships(node_id, distance, want_synonyms=True):
    """Core function to find distant synonyms or antonyms."""
    relationship_type = "synonyms" if want_synonyms else "antonyms"

    try:
        verbose_print(
            f"Querying for {relationship_type} of node: {node_id} with distance: {distance}",
        )
        start_time = time.time()

        results = dgraph_query(node_id, distance)
        if not results:  # Handle case when query returns no results
            verbose_print(f"No results found for node: {node_id}")
            return {f"distant_{relationship_type}": []}

        graph, nodes_info = build_relationship_graph(results)

        # Find nodes at specified distance
        visited = find_nodes_at_distance(node_id, distance, graph)

        # Filter for synonyms or antonyms
        filtered_results = filter_results(
            visited, nodes_info, distance, node_id, want_synonyms
        )

        # Output results
        verbose_print(f"Found {len(filtered_results)} distant {relationship_type}")
        result = {f"distant_{relationship_type}": filtered_results}
        end_time = time.time()
        json_print(result)
        verbose_print(f"Query executed in {end_time - start_time:.2f} seconds")

    except Exception as error:
        error_print(f"finding distant {relationship_type} for {node_id}", error)
