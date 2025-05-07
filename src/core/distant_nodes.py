import time
from collections import deque

from .message_handler import error_print, json_print, verbose_print, length_print
from .queries import DISTANT_SYNONYMS_ANTONYM
from .utils import dgraph_read, if_exist


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
    """Find all nodes at a specific distance, tracking all synonym/antonym possibilities."""
    queue = deque([(node_id, 0, True)])  # node_id, distance, is_synonym
    visited = {node_id: (0, {True})}  # node_id -> (distance, {possible relationships})

    while queue:
        current_id, curr_dist, is_synonym = queue.popleft()

        # Skip further traversal if we've reached our distance
        if curr_dist >= distance:
            continue

        for neighbor_id, is_synonym_edge in graph.get(current_id, []):
            # Calculate if neighbor is synonym of root node
            neighbor_is_synonym = is_synonym if is_synonym_edge else not is_synonym
            next_dist = curr_dist + 1

            if neighbor_id not in visited:
                # First time seeing this node
                visited[neighbor_id] = (next_dist, {neighbor_is_synonym})
                queue.append((neighbor_id, next_dist, neighbor_is_synonym))
            elif next_dist == visited[neighbor_id][0]:
                # Same distance but possibly different relationship type
                visited[neighbor_id][1].add(neighbor_is_synonym)
            elif next_dist < visited[neighbor_id][0]:
                # Found a shorter path
                visited[neighbor_id] = (next_dist, {neighbor_is_synonym})
                queue.append((neighbor_id, next_dist, neighbor_is_synonym))

    return visited


def filter_results(visited, nodes_info, distance, node_id, want_synonyms=True):
    """Filter nodes at target distance based on synonym/antonym status."""
    results = []
    for node, (dist, is_synonym_set) in visited.items():
        # Skip if not at target distance or it's the original node
        if dist != distance or node == node_id:
            continue

        # Include node if it can have the relationship we want
        if want_synonyms in is_synonym_set:
            results.append({"id": node, "label": nodes_info.get(node, node)})

    return results


def find_distant_relationships(node_id, distance, want_synonyms=True):
    """Core function to find distant synonyms or antonyms."""
    relationship_type = "synonyms" if want_synonyms else "antonyms"
    if not if_exist(node_id):
        error_print(
            f"distant_{relationship_type}",
            f"Node {node_id} does not exist in the graph.",
        )
    __myname__ = f"distant_{relationship_type}"

    try:
        start_time = time.time()
        results = dgraph_query(node_id, distance)
        if not results:
            verbose_print(f"No results found for node: {node_id}")
            return {__myname__: []}

        graph, nodes_info = build_relationship_graph(results)

        # Find nodes at specified distance
        visited = find_nodes_at_distance(node_id, distance, graph)

        # Filter for synonyms or antonyms
        filtered_results = filter_results(
            visited, nodes_info, distance, node_id, want_synonyms
        )
        end_time = time.time()

        # Output results
        result = {__myname__: filtered_results}
        length_print(__myname__, result)
        json_print(result)
        verbose_print(f"Query executed in {end_time - start_time:.2f} seconds")
        return result

    except Exception as error:
        error_print(f"finding distant {relationship_type} for {node_id}", error)
        return None
