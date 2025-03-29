import click
import queries
from utils import dgraph_read, dgraph_write
from message_handler import error_print, json_print, verbose_print
from distant_nodes import find_distant_relationships
from collections import deque


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
@click.argument("node_id", required=True)
def find_similar_nodes(node_id):
    """Find all 'similar' nodes that share common parents or children via the same edge type."""
    try:
        results = dgraph_read(queries.SIMILAR_NODES_QUERY, variables={"$id": node_id})
        json_print(results)
    except Exception as error:
        error_print(f"finding similar nodes for {node_id}", error)


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
