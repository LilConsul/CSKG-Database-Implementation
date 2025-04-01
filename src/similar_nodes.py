import time
from message_handler import error_print, verbose_print, json_print
from utils import dgraph_read
from queries import SIMILAR_NODES_QUERY


def get_similar_nodes(node_id):
    try:
        start_time = time.time()
        similar_nodes = {}

        # Fetch node data
        results = dgraph_read(
            SIMILAR_NODES_QUERY,
            variables={"$id": node_id},
        )
        nodes = results.get("node_info", [])
        if not nodes:
            error_print(f"No nodes found for ID {node_id}")
            return

        node = nodes[0]
        node_id = node.get("id")

        # Process each successor and its connections
        for successor in node.get("to", []):
            successor_id = successor.get("id")
            edge_type_to_successor = successor.get("to|id")

            if successor_id == node_id:
                continue

            # Check successor's predecessors (2nd level)
            for sub_predecessor in successor.get("~to", []):
                sub_predecessor_id = sub_predecessor.get("id")
                edge_type_sub = sub_predecessor.get("~to|id")

                if (
                    sub_predecessor_id != node_id
                    and edge_type_to_successor == edge_type_sub
                ):
                    similar_nodes.setdefault(
                        sub_predecessor_id,
                        {
                            "id": sub_predecessor_id,
                            "label": sub_predecessor.get("label"),
                            "shared_connections": [],
                        },
                    )
                    similar_nodes[sub_predecessor_id]["shared_connections"].append(
                        {"via_node": successor_id, "edge_type": edge_type_to_successor}
                    )

        # Process each predecessor and its connections
        for predecessor in node.get("~to", []):
            predecessor_id = predecessor.get("id")
            edge_type_from_predecessor = predecessor.get("~to|id")

            if predecessor_id == node_id:
                continue

            # Check predecessor's successors (2nd level)
            for sub_successor in predecessor.get("to", []):
                sub_successor_id = sub_successor.get("id")
                edge_type_sub = sub_successor.get("to|id")

                if (
                    sub_successor_id != node_id
                    and edge_type_from_predecessor == edge_type_sub
                ):
                    similar_nodes.setdefault(
                        sub_successor_id,
                        {
                            "id": sub_successor_id,
                            "label": sub_successor.get("label"),
                            "shared_connections": [],
                        },
                    )
                    similar_nodes[sub_successor_id]["shared_connections"].append(
                        {
                            "via_node": predecessor_id,
                            "edge_type": edge_type_from_predecessor,
                        }
                    )

        result = {"similar_nodes": list(similar_nodes.values())}
        end_time = time.time()
        json_print(result)
        verbose_print(f"Query executed in {end_time - start_time:.2f} seconds")

    except Exception as error:
        error_print("finding similar nodes", error)