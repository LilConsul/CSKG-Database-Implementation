import time

import click

import queries
from distant_nodes import find_distant_relationships
from message_handler import error_print, json_print, verbose_print
from similar_nodes import get_similar_nodes
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
            time_start = time.time()
            results = dgraph_read(query, variables={"$id": node_id})
            time_end = time.time()
            json_print(results)
            verbose_print(f"Query executed in {time_end - time_start:.2f} seconds")
        except Exception as error:
            error_print(err_text, error)

    return command


def query_no_arg(name, query, help_text, err_text):
    @click.command(name, help=help_text)
    def command():
        try:
            time_start = time.time()
            results = dgraph_read(query)
            time_end = time.time()
            json_print(results)
            verbose_print(f"Query executed in {time_end - time_start:.2f} seconds")
        except Exception as error:
            error_print(err_text, error)

    return command


@click.command()
def find_nodes_most_neighbors():
    """Find nodes with the most neighbors."""
    try:
        time_start = time.time()

        offset = 0
        max_neighbors = 0
        nodes_with_max = []
        more_nodes_possible = True

        while more_nodes_possible:
            results = dgraph_read(
                queries.NODES_MOST_NEIGHBORS_QUERY,
                variables={"$offset": str(offset)},
            )

            result_nodes = results.get("nodes_with_most_neighbors", [])
            if not result_nodes:
                break

            # Set the max_neighbors from the first batch
            if offset == 0:
                max_neighbors = result_nodes[0].get("total_neighbors", 0)

            # Collect nodes with max neighbors count
            for node in result_nodes:
                if node.get("total_neighbors") == max_neighbors:
                    nodes_with_max.append(node)
                else:
                    more_nodes_possible = False
                    break

            # Check if we need another page
            if more_nodes_possible and len(result_nodes) == 10:
                offset += 10
            else:
                more_nodes_possible = False

        time_end = time.time()

        result = {"nodes_with_most_neighbors": nodes_with_max}
        json_print(result)
        click.echo(
            f"Found {len(nodes_with_max)} nodes with {max_neighbors} neighbors"
        )
        verbose_print(f"Query executed in {time_end - time_start:.2f} seconds")

    except Exception as error:
        error_print("finding nodes with most neighbors", error)


@click.command()
@click.argument("node_id", required=True)
@click.argument("new_label", required=True)
def rename_node(node_id, new_label):
    """Rename a given node by updating its label."""
    try:
        start_time = time.time()
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
        end_time = time.time()
        json_print(f"Successfully renamed node {node_id} to '{new_label}'")
        verbose_print(f"Query executed in {end_time - start_time:.2f} seconds")

    except Exception as error:
        error_print("renaming node", error)


@click.command()
@click.argument("node_id", required=True)
def find_similar_nodes(node_id):
    """Find all 'similar' nodes that share common parents or children via the same edge type."""
    return get_similar_nodes(node_id)


@click.command()
@click.argument("node_id1", required=True)
@click.argument("node_id2", required=True)
def find_shortest_path(node_id1, node_id2):
    """Find the shortest path between two nodes, ignoring edge direction."""
    try:
        start_time = time.time()
        results = dgraph_read(
            queries.SHORTEST_PATH_QUERY,
            variables={"$id1": str(node_id1), "$id2": str(node_id2)},
        )
        end_time = time.time()
        json_print(results)
        verbose_print(f"Query executed in {end_time - start_time:.2f} seconds")
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
        name="find-successors",
        query=queries.SUCCESSORS_QUERY,
        help_text="Find all successors of a given node.",
        err_text="Failed to find successors for node",
    )
)

cli.add_command(
    query_one_arg(
        name="count-successors",
        query=queries.COUNT_SUCCESSORS_QUERY,
        help_text="Count all successors of a given node.",
        err_text="Failed to count successors for node",
    )
)

cli.add_command(
    query_one_arg(
        name="find-predecessors",
        query=queries.PREDECESSORS_QUERY,
        help_text="Find all predecessors of a given node.",
        err_text="Failed to find predecessors for node",
    )
)

cli.add_command(
    query_one_arg(
        name="count-predecessors",
        query=queries.COUNT_PREDECESSORS_QUERY,
        help_text="Count all predecessors of a given node.",
        err_text="Failed to count predecessors for node",
    )
)

cli.add_command(
    query_one_arg(
        name="find-neighbors",
        query=queries.NEIGHBORS_QUERY,
        help_text="Find all neighbors of a given node.",
        err_text="Failed to find neighbors for node",
    )
)

cli.add_command(
    query_one_arg(
        name="count-neighbors",
        query=queries.COUNT_NEIGHBORS_QUERY,
        help_text="Count all neighbors of a given node.",
        err_text="Failed to count neighbors for node",
    )
)

cli.add_command(
    query_one_arg(
        name="find-grandchildren",
        query=queries.GRANDCHILDREN_QUERY,
        help_text="Find all grandchildren (successors of successors) of a given node.",
        err_text="Failed to find grandchildren for node",
    )
)

cli.add_command(
    query_one_arg(
        name="find-grandparents",
        query=queries.GRANDPARENTS_QUERY,
        help_text="Find all grandparents (predecessors of predecessors) of a given node.",
        err_text="Failed to find grandparents for node",
    )
)

# Replace these individual commands with query_no_arg calls
cli.add_command(
    query_no_arg(
        name="count-nodes",
        query=queries.TOTAL_NODES_QUERY,
        help_text="Count how many nodes there are.",
        err_text="counting total nodes",
    )
)

cli.add_command(
    query_no_arg(
        name="count-nodes-no-successors",
        query=queries.NODES_NO_SUCCESSORS_QUERY,
        help_text="Count nodes which do not have any successors.",
        err_text="counting nodes without successors",
    )
)

cli.add_command(
    query_no_arg(
        name="count-nodes-no-predecessors",
        query=queries.NODES_NO_PREDECESSORS_QUERY,
        help_text="Count nodes which do not have any predecessors.",
        err_text="counting nodes without predecessors",
    )
)

cli.add_command(
    query_no_arg(
        name="count-nodes-single-neighbor",
        query=queries.NODES_SINGLE_NEIGHBOR_QUERY,
        help_text="Count nodes with a single neighbor.",
        err_text="counting nodes with a single neighbor",
    )
)

cli.add_command(find_nodes_most_neighbors)
cli.add_command(rename_node)
cli.add_command(find_similar_nodes)
cli.add_command(find_shortest_path)
cli.add_command(find_distant_synonyms)
cli.add_command(find_distant_antonyms)

if __name__ == "__main__":
    cli()
