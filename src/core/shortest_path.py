from collections import deque

from core.message_handler import error_print, verbose_print
from core.queries import NEIGHBORS_QUERY
from core.utils import dgraph_read, if_exist


def reconstruct_bidirectional_path(
    forward_visited, backward_visited, intersection, node_labels
):
    forward_path = []
    current = intersection
    while current is not None:
        forward_path.append({"id": current, "label": node_labels.get(current, current)})
        current = forward_visited[current]
    forward_path.reverse()

    # Reconstruct backward path (intersection → target, excluding intersection)
    backward_path = []
    current = backward_visited[intersection]
    while current:
        backward_path.append(
            {"id": current, "label": node_labels.get(current, current)}
        )
        current = backward_visited[current]

    return forward_path + backward_path


def shortest_path(id1: str, id2: str):
    """
    Find the shortest path between two nodes in a graph using bidirectional BFS.
    """

    if id1 == id2:
        return [{"id": id1, "label": id1}]

    if not if_exist(id1):
        error_print(shortest_path.__name__, f"Node {id1} does not exist")

    if not if_exist(id2):
        error_print(shortest_path.__name__, f"Node {id2} does not exist")

    forward_queue = deque([id1])
    forward_visited = {id1: None}
    forward_distance = {id1: 0}

    backward_queue = deque([id2])
    backward_visited = {id2: None}
    backward_distance = {id2: 0}

    node_labels = {id1: id1, id2: id2}

    best_path_length = float("inf")
    best_intersection = None

    while forward_queue and backward_queue:
        if forward_queue:
            intersection = process_level(
                forward_queue,
                forward_visited,
                forward_distance,
                backward_visited,
                backward_distance,
                node_labels,
            )

            if (
                intersection
                and forward_distance[intersection] + backward_distance[intersection]
                < best_path_length
            ):
                best_path_length = (
                    forward_distance[intersection] + backward_distance[intersection]
                )
                best_intersection = intersection

        if backward_queue:
            intersection = process_level(
                backward_queue,
                backward_visited,
                backward_distance,
                forward_visited,
                forward_distance,
                node_labels,
            )

            if (
                intersection
                and forward_distance[intersection] + backward_distance[intersection]
                < best_path_length
            ):
                best_path_length = (
                    forward_distance[intersection] + backward_distance[intersection]
                )
                best_intersection = intersection

        if (
            forward_queue
            and backward_queue
            and forward_distance[forward_queue[0]]
            + backward_distance[backward_queue[0]]
            >= best_path_length
        ):
            break

    if best_intersection:
        verbose_print(
            f"Shortest path found with length {best_path_length + 1} at {best_intersection}"
        )
        return reconstruct_bidirectional_path(
            forward_visited, backward_visited, best_intersection, node_labels
        )

    verbose_print("No path found")
    return []


def process_level(
    queue,
    current_visited,
    current_distance,
    opposite_visited,
    opposite_distance,
    node_labels,
):
    """Process all nodes at current level (same distance from source)"""
    best_intersection = None
    best_path_length = float("inf")

    level_size = len(queue)
    for _ in range(level_size):
        current_id = queue.popleft()

        # Check for intersection
        if current_id in opposite_visited:
            path_length = current_distance[current_id] + opposite_distance[current_id]
            if path_length < best_path_length:
                best_path_length = path_length
                best_intersection = current_id

        result = dgraph_read(NEIGHBORS_QUERY, variables={"$id": current_id})
        neighbors = None
        if isinstance(result, dict):
            if "neighbors" in result:
                neighbors = result["neighbors"]
            elif "data" in result and "neighbors" in result["data"]:
                neighbors = result["data"]["neighbors"]

        if not neighbors:
            continue

        for node in neighbors:
            neighbor_id = node["id"]
            node_labels[neighbor_id] = node.get("label", neighbor_id)

            if neighbor_id not in current_visited:
                current_visited[neighbor_id] = current_id
                current_distance[neighbor_id] = current_distance[current_id] + 1
                queue.append(neighbor_id)

    return best_intersection
