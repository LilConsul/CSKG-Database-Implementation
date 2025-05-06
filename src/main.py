import time

import click

from core import queries
from core.distant_nodes import find_distant_relationships
from core.message_handler import error_print, json_print, verbose_print
from core.similar_nodes import get_similar_nodes
from core.utils import dgraph_read, dgraph_write


class AliasedGroup(click.Group):
    def get_command(self, ctx, cmd_name):
        rv = click.Group.get_command(self, ctx, cmd_name)
        if rv is not None:
            return rv

        for name, cmd in self.commands.items():
            if hasattr(cmd, "aliases") and cmd_name in cmd.aliases:
                return cmd
        return None

    def format_commands(self, ctx, formatter):
        commands = []
        docker_commands = []

        for subcommand in self.list_commands(ctx):
            cmd = self.get_command(ctx, subcommand)
            if cmd is None or cmd.hidden:
                continue

            numerical_alias = float("inf")
            if hasattr(cmd, "aliases") and cmd.aliases:
                for alias in cmd.aliases:
                    if alias.isdigit():
                        numerical_alias = int(alias)
                        break

                aliases_str = ", ".join(cmd.aliases)
                help_text = f"{aliases_str}. {cmd.help}"
                commands.append((subcommand, help_text, numerical_alias))
            else:
                help_text = cmd.help
                docker_commands.append((subcommand, help_text))

        commands.sort(key=lambda x: x[2])

        if commands or docker_commands:
            formatter.width = 120

            formatter.write("\nDgraph Operations:\n")

            if commands:
                rows = [
                    (subcommand, help_text) for subcommand, help_text, _ in commands
                ]
                formatter.write_dl(rows)

            if docker_commands:
                formatter.write("\n")
                formatter.write("Docker Operations:\n")
                formatter.write_dl(docker_commands)


def add_aliases(cmd, aliases):
    """Add aliases to a command"""
    if not hasattr(cmd, "aliases"):
        cmd.aliases = []
    if isinstance(aliases, str):
        cmd.aliases.append(aliases)
    else:
        cmd.aliases.extend(aliases)
    return cmd


@click.group(cls=AliasedGroup, context_settings={"help_option_names": ["-h", "--help"]})
@click.option("--verbose", "-v", is_flag=True, help="Enable verbose output.")
@click.pass_context
def cli(ctx, verbose):
    """
    Command-line interface for managing and querying Dgraph knowledge graph operations.

    This CLI provides tools to explore node relationships, query graph structures, modify data,
    and manage the underlying Docker infrastructure. Use numerical shortcuts (1-18) for faster access.
    """
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
        verbose_print(
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
        mutation = {"set": [{"uid": uid, "label": new_label, "id": new_label}]}
        dgraph_write(mutation)
        end_time = time.time()
        json_print(f"Successfully renamed node {node_id} to '{new_label}'")
        verbose_print(f"Query executed in {end_time - start_time:.2f} seconds")

    except Exception as error:
        error_print("renaming node", error)


@click.command()
@click.argument("node_id", required=True)
def find_similar_nodes(node_id):
    """Find all 'similar' nodes that share common neigbors via the same edge type."""
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

# Add query commands with aliases
find_successors_cmd = query_one_arg(
    name="find-successors",
    query=queries.SUCCESSORS_QUERY,
    help_text="Find all successors of a given node.",
    err_text="Failed to find successors for node",
)
add_aliases(find_successors_cmd, "1")
cli.add_command(find_successors_cmd)

count_successors_cmd = query_one_arg(
    name="count-successors",
    query=queries.COUNT_SUCCESSORS_QUERY,
    help_text="Count all successors of a given node.",
    err_text="Failed to count successors for node",
)
add_aliases(count_successors_cmd, "2")
cli.add_command(count_successors_cmd)

find_predecessors_cmd = query_one_arg(
    name="find-predecessors",
    query=queries.PREDECESSORS_QUERY,
    help_text="Find all predecessors of a given node.",
    err_text="Failed to find predecessors for node",
)
add_aliases(find_predecessors_cmd, "3")
cli.add_command(find_predecessors_cmd)

count_predecessors_cmd = query_one_arg(
    name="count-predecessors",
    query=queries.COUNT_PREDECESSORS_QUERY,
    help_text="Count all predecessors of a given node.",
    err_text="Failed to count predecessors for node",
)
add_aliases(count_predecessors_cmd, "4")
cli.add_command(count_predecessors_cmd)

find_neighbors_cmd = query_one_arg(
    name="find-neighbors",
    query=queries.NEIGHBORS_QUERY,
    help_text="Find all neighbors of a given node.",
    err_text="Failed to find neighbors for node",
)
add_aliases(find_neighbors_cmd, "5")
cli.add_command(find_neighbors_cmd)

count_neighbors_cmd = query_one_arg(
    name="count-neighbors",
    query=queries.COUNT_NEIGHBORS_QUERY,
    help_text="Count all neighbors of a given node.",
    err_text="Failed to count neighbors for node",
)
add_aliases(count_neighbors_cmd, "6")
cli.add_command(count_neighbors_cmd)

find_grandchildren_cmd = query_one_arg(
    name="find-grandchildren",
    query=queries.GRANDCHILDREN_QUERY,
    help_text="Find all grandchildren (successors of successors) of a given node.",
    err_text="Failed to find grandchildren for node",
)
add_aliases(find_grandchildren_cmd, "7")
cli.add_command(find_grandchildren_cmd)

find_grandparents_cmd = query_one_arg(
    name="find-grandparents",
    query=queries.GRANDPARENTS_QUERY,
    help_text="Find all grandparents (predecessors of predecessors) of a given node.",
    err_text="Failed to find grandparents for node",
)
add_aliases(find_grandparents_cmd, "8")
cli.add_command(find_grandparents_cmd)

count_nodes_cmd = query_no_arg(
    name="count-nodes",
    query=queries.TOTAL_NODES_QUERY,
    help_text="Count how many nodes there are.",
    err_text="counting total nodes",
)
add_aliases(count_nodes_cmd, "9")
cli.add_command(count_nodes_cmd)

count_nodes_no_successors_cmd = query_no_arg(
    name="count-nodes-no-successors",
    query=queries.NODES_NO_SUCCESSORS_QUERY,
    help_text="Count nodes which do not have any successors.",
    err_text="counting nodes without successors",
)
add_aliases(count_nodes_no_successors_cmd, "10")
cli.add_command(count_nodes_no_successors_cmd)

count_nodes_no_predecessors_cmd = query_no_arg(
    name="count-nodes-no-predecessors",
    query=queries.NODES_NO_PREDECESSORS_QUERY,
    help_text="Count nodes which do not have any predecessors.",
    err_text="counting nodes without predecessors",
)
add_aliases(count_nodes_no_predecessors_cmd, "11")
cli.add_command(count_nodes_no_predecessors_cmd)

add_aliases(find_nodes_most_neighbors, "12")
cli.add_command(find_nodes_most_neighbors)

count_nodes_single_neighbor_cmd = query_no_arg(
    name="count-nodes-single-neighbor",
    query=queries.NODES_SINGLE_NEIGHBOR_QUERY,
    help_text="Count nodes with a single neighbor.",
    err_text="counting nodes with a single neighbor",
)
add_aliases(count_nodes_single_neighbor_cmd, "13")
cli.add_command(count_nodes_single_neighbor_cmd)

add_aliases(rename_node, "14")
cli.add_command(rename_node)

add_aliases(find_similar_nodes, "15")
cli.add_command(find_similar_nodes)

add_aliases(find_shortest_path, "16")
cli.add_command(find_shortest_path)

add_aliases(find_distant_synonyms, "17")
cli.add_command(find_distant_synonyms)

add_aliases(find_distant_antonyms, "18")
cli.add_command(find_distant_antonyms)

if __name__ == "__main__":
    cli(prog_name="./dbcli.sh")
