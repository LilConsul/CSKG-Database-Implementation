import json
from collections import deque

import click

import queries
from distant_nodes import find_distant_relationships
from message_handler import error_print, json_print, verbose_print
from utils import dgraph_read, dgraph_write


@click.group()
@click.option("--verbose", is_flag=True, help="Enable verbose output")
@click.pass_context
def cli(ctx, verbose):
    """CLI entry point for Dgraph operations."""
    ctx.ensure_object(dict)
    ctx.obj["verbose"] = verbose


@click.command()
def cleanup():
    """Remove all containers, volumes, images, and clear data and storage directories."""
    pass


@click.command()
def run():
    """Start the Docker containers."""
    pass


@click.command()
def stop():
    """Stop the Docker containers."""
    pass


def query_one_arg(name, query, help_text, err_text):
    @click.command(name, help=help_text)
    @click.argument("node_id", required=True)
    def command(node_id):
        try:
            results = dgraph_read(query, variables={"$id": node_id})
            json_print(results)
        except Exception as error:
            error_print(err_text, error)

    return command


@click.command()
def count_nodes():
    """Count how many nodes there are."""
    try:
        results = dgraph_read(queries.TOTAL_NODES_QUERY)
        json_print(results)
    except Exception as error:
        error_print("counting total nodes", error)


@click.command()
def count_nodes_no_successors():
    """Count nodes which do not have any successors."""
    try:
        results = dgraph_read(queries.NODES_NO_SUCCESSORS_QUERY)
        json_print(results)
    except Exception as error:
        error_print("counting nodes without successors", error)


@click.command()
def count_nodes_no_predecessors():
    """Count nodes which do not have any predecessors."""
    try:
        results = dgraph_read(queries.NODES_NO_PREDECESSORS_QUERY)
        json_print(results)
    except Exception as error:
        error_print("counting nodes without predecessors", error)


@click.command()
def find_nodes_most_neighbors():
    """Find nodes with the most neighbors."""
    try:
        max_count_results = dgraph_read(queries.MOST_NEIGHBORS_QUERY_AMOUNT)
        max_neighbors = max_count_results.get("nodes_with_most_neighbors", [{}])[0].get(
            "total_neighbors"
        )

        if not max_neighbors:
            error_print("determining maximum neighbor count", None)

        verbose_print(f"Maximum number of neighbors found: {max_neighbors}")
        verbose_print(f"Searching for all nodes with {max_neighbors} neighbors...")

        results = dgraph_read(
            queries.NODES_MOST_NEIGHBORS_QUERY,
            variables={"$max_neighbors": str(max_neighbors)},
        )
        json_print(results)

    except Exception as error:
        error_print("finding nodes with most neighbors", error)


@click.command()
def count_nodes_single_neighbor():
    """Count nodes with a single neighbor."""
    try:
        results = dgraph_read(queries.NODES_SINGLE_NEIGHBOR_QUERY)
        json_print(results)
    except Exception as error:
        error_print("counting nodes with a single neighbor", error)


@click.command()
@click.argument("node_id", required=True)
@click.argument("new_label", required=True)
def rename_node(node_id, new_label):
    """Rename a given node by updating its label."""
    try:
        node_info = dgraph_read(
            """
            query getNode($id: string) {
                node(func: eq(id, $id)) {
                    uid
                }
            }
            """,
            variables={"$id": node_id},
        )

        if not node_info.get("node"):
            error_print(f"Node with ID {node_id} not found", None)
            return

        uid = node_info["node"][0]["uid"]
        mutation = {"set": [{"uid": uid, "label": new_label}]}
        dgraph_write(mutation)
        verbose_print(f"Successfully renamed node {node_id} to '{new_label}'")

    except Exception as error:
        error_print("renaming node", error)



@click.command()
@click.option(
    "--batch-size",
    type=int,
    default=100,
    help="Number of nodes to process in each batch",
)
def find_similar_nodes(batch_size):
    """Find all 'similar' nodes that share common parents or children via the same edge type."""
    try:
        processed_nodes = set()
        similar_pairs = {}
        offset = 0
        output_file = "similar_nodes.json"

        click.echo(f"Starting similar node search with batch size {batch_size}")

        while True:
            # Fetch a batch of nodes
            results = dgraph_read(
                queries.SIMILAR_NODES_QUERY,
                variables={"$first": str(batch_size), "$offset": str(offset)},
            )

            nodes = results.get("nodes", [])
            if not nodes:
                break

            click.echo(f"Processing batch of {len(nodes)} nodes (offset: {offset})...")

            # First pass: collect all relationships
            for node in nodes:
                node_id = node.get("id")
                if node_id in processed_nodes:
                    continue

                # Process each successor and its connections
                for successor in node.get("to", []):
                    successor_id = successor.get("id")
                    edge_type_to_successor = successor.get("to|id")
                    if successor_id in processed_nodes:
                        continue

                    # Check successor's successors (2nd level)
                    for sub_successor in successor.get("to", []):
                        sub_successor_id = sub_successor.get("id")
                        edge_type_sub = sub_successor.get("to|id")
                        if sub_successor_id in processed_nodes or sub_successor_id == node_id:
                            continue

                        # Find if original node connects to this sub_successor with same edge type
                        if edge_type_to_successor == edge_type_sub:
                            node1, node2 = sorted([node_id, sub_successor_id])
                            pair = (node1, node2)

                            if pair not in similar_pairs:
                                similar_pairs[pair] = []

                            similarity = {
                                "via_node": successor_id,
                                "edge_type": edge_type_sub,
                                "relation": "common_parent",
                            }

                            if similarity not in similar_pairs[pair]:
                                similar_pairs[pair].append(similarity)

                    # Check successor's predecessors (2nd level)
                    for sub_predecessor in successor.get("~to", []):
                        sub_predecessor_id = sub_predecessor.get("id")
                        edge_type_sub = sub_predecessor.get("~to|id")
                        if sub_predecessor_id in processed_nodes or sub_predecessor_id == node_id:
                            continue

                        # Find if original node has connection from this sub_predecessor with same edge type
                        if edge_type_to_successor == edge_type_sub:
                            node1, node2 = sorted([node_id, sub_predecessor_id])
                            pair = (node1, node2)

                            if pair not in similar_pairs:
                                similar_pairs[pair] = []

                            similarity = {
                                "via_node": successor_id,
                                "edge_type": edge_type_sub,
                                "relation": "common_child",
                            }

                            if similarity not in similar_pairs[pair]:
                                similar_pairs[pair].append(similarity)

                # Process each predecessor and its connections
                for predecessor in node.get("~to", []):
                    predecessor_id = predecessor.get("id")
                    edge_type_from_predecessor = predecessor.get("~to|id")
                    if predecessor_id in processed_nodes:
                        continue

                    # Check predecessor's successors (2nd level)
                    for sub_successor in predecessor.get("to", []):
                        sub_successor_id = sub_successor.get("id")
                        edge_type_sub = sub_successor.get("to|id")
                        if sub_successor_id in processed_nodes or sub_successor_id == node_id:
                            continue

                        # Find if original node connects to this sub_successor with same edge type
                        if edge_type_from_predecessor == edge_type_sub:
                            node1, node2 = sorted([node_id, sub_successor_id])
                            pair = (node1, node2)

                            if pair not in similar_pairs:
                                similar_pairs[pair] = []

                            similarity = {
                                "via_node": predecessor_id,
                                "edge_type": edge_type_sub,
                                "relation": "common_child",
                            }

                            if similarity not in similar_pairs[pair]:
                                similar_pairs[pair].append(similarity)

                    # Check predecessor's predecessors (2nd level)
                    for sub_predecessor in predecessor.get("~to", []):
                        sub_predecessor_id = sub_predecessor.get("id")
                        edge_type_sub = sub_predecessor.get("~to|id")
                        if sub_predecessor_id in processed_nodes or sub_predecessor_id == node_id:
                            continue

                        # Find if original node has connection from this sub_predecessor with same edge type
                        if edge_type_from_predecessor == edge_type_sub:
                            node1, node2 = sorted([node_id, sub_predecessor_id])
                            pair = (node1, node2)

                            if pair not in similar_pairs:
                                similar_pairs[pair] = []

                            similarity = {
                                "via_node": sub_predecessor_id,
                                "edge_type": edge_type_sub,
                                "relation": "common_parent",
                            }

                            if similarity not in similar_pairs[pair]:
                                similar_pairs[pair].append(similarity)

                processed_nodes.add(node_id)

            # Move to next batch
            offset += batch_size

            # Periodically save to file and clear memory if needed
            if len(processed_nodes) % (batch_size * 10) == 0:
                click.echo(
                    f"Progress: processed {len(processed_nodes)} nodes, found {len(similar_pairs)} similar pairs"
                )

                # Convert tuple keys to strings for JSON serialization
                serializable_pairs = {
                    f"{node1},{node2}": similarities
                    for (node1, node2), similarities in similar_pairs.items()
                }

                with open(output_file, "w") as f:
                    json.dump(serializable_pairs, f, indent=2)

                # Clear memory if too many pairs
                if len(similar_pairs) > 1000000:
                    click.echo(
                        "Memory usage high, saving and clearing similar pairs..."
                    )
                    similar_pairs = {}

        # Save final results
        serializable_pairs = {
            f"{node1},{node2}": similarities
            for (node1, node2), similarities in similar_pairs.items()
        }

        with open(output_file, "w") as f:
            json.dump(serializable_pairs, f, indent=2)

        click.echo(f"Completed: processed {len(processed_nodes)} nodes")
        click.echo(f"Found {len(similar_pairs)} similar node pairs")
        click.echo(f"Results saved to {output_file}")

    except Exception as error:
        error_print("finding similar nodes", error)


@click.command()
@click.argument("node_id1", required=True)
@click.argument("node_id2", required=True)
def find_shortest_path(node_id1, node_id2):
    """Find the shortest path between two nodes, ignoring edge direction."""
    try:
        results = dgraph_read(
            queries.SHORTEST_PATH_QUERY,
            variables={"$id1": str(node_id1), "$id2": str(node_id2)},
        )
        json_print(results)
    except Exception as error:
        error_print(f"finding path between {node_id1} and {node_id2}", error)


@click.command()
@click.argument("node_id", required=True)
@click.argument("distance", type=int, required=True)
def find_distant_synonyms(node_id, distance):
    """Find all distant synonyms at a specified path distance."""
    return find_distant_relationships(node_id, distance, want_synonyms=True)


@click.command()
@click.argument("node_id", required=True)
@click.argument("distance", type=int, required=True)
def find_distant_antonyms(node_id, distance):
    """Find all distant antonyms at a specified path distance."""
    return find_distant_relationships(node_id, distance, want_synonyms=False)


# Add all commands to the CLI group
cli.add_command(cleanup)
cli.add_command(run)
cli.add_command(stop)

cli.add_command(
    query_one_arg(
        name="find_successors",
        query=queries.SUCCESSORS_QUERY,
        help_text="Find all successors of a given node.",
        err_text="Failed to find successors for node",
    )
)

cli.add_command(
    query_one_arg(
        name="count_successors",
        query=queries.COUNT_SUCCESSORS_QUERY,
        help_text="Count all successors of a given node.",
        err_text="Failed to count successors for node",
    )
)

cli.add_command(
    query_one_arg(
        name="find_predecessors",
        query=queries.PREDECESSORS_QUERY,
        help_text="Find all predecessors of a given node.",
        err_text="Failed to find predecessors for node",
    )
)

cli.add_command(
    query_one_arg(
        name="count_predecessors",
        query=queries.COUNT_PREDECESSORS_QUERY,
        help_text="Count all predecessors of a given node.",
        err_text="Failed to count predecessors for node",
    )
)

cli.add_command(
    query_one_arg(
        name="find_neighbors",
        query=queries.NEIGHBORS_QUERY,
        help_text="Find all neighbors of a given node.",
        err_text="Failed to find neighbors for node",
    )
)

cli.add_command(
    query_one_arg(
        name="count_neighbors",
        query=queries.COUNT_NEIGHBORS_QUERY,
        help_text="Count all neighbors of a given node.",
        err_text="Failed to count neighbors for node",
    )
)

cli.add_command(
    query_one_arg(
        name="find_grandchildren",
        query=queries.GRANDCHILDREN_QUERY,
        help_text="Find all grandchildren (successors of successors) of a given node.",
        err_text="Failed to find grandchildren for node",
    )
)

cli.add_command(
    query_one_arg(
        name="find_grandparents",
        query=queries.GRANDPARENTS_QUERY,
        help_text="Find all grandparents (predecessors of predecessors) of a given node.",
        err_text="Failed to find grandparents for node",
    )
)

cli.add_command(count_nodes)
cli.add_command(count_nodes_no_successors)
cli.add_command(count_nodes_no_predecessors)
cli.add_command(find_nodes_most_neighbors)
cli.add_command(count_nodes_single_neighbor)
cli.add_command(rename_node)
cli.add_command(find_similar_nodes)
cli.add_command(find_shortest_path)
cli.add_command(find_distant_synonyms)
cli.add_command(find_distant_antonyms)

if __name__ == "__main__":
    cli()
