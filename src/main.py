import json

import click

import queries
from utils import dgraph_read, dgraph_write


@click.group()
def cli():
    """CLI entry point for Dgraph operations."""
    pass


@click.command()
@click.argument("node_id", required=True)
def find_successors(node_id):
    """Find all successors of a given node."""
    try:
        results = dgraph_read(queries.SUCCESSORS_QUERY, variables={"$id": node_id})
        click.echo(json.dumps(results, indent=2))
    except Exception:
        click.echo(f"Failed to find successors for node {node_id}", err=True)
        exit(1)


@click.command()
@click.argument("node_id", required=True)
def count_successors(node_id):
    """Count all successors of a given node."""
    try:
        results = dgraph_read(
            queries.COUNT_SUCCESSORS_QUERY, variables={"$id": node_id}
        )
        click.echo(json.dumps(results, indent=2))
    except Exception:
        click.echo(f"Failed to count successors for node {node_id}", err=True)
        exit(1)


@click.command()
@click.argument("node_id", required=True)
def find_predecessors(node_id):
    """Find all predecessors of a given node."""
    try:
        results = dgraph_read(queries.PREDECESSORS_QUERY, variables={"$id": node_id})
        click.echo(json.dumps(results, indent=2))
    except Exception:
        click.echo(f"Failed to find predecessors for node {node_id}", err=True)
        exit(1)


@click.command()
@click.argument("node_id", required=True)
def count_predecessors(node_id):
    """Count all predecessors of a given node."""
    try:
        results = dgraph_read(
            queries.COUNT_PREDECESSORS_QUERY, variables={"$id": node_id}
        )
        click.echo(json.dumps(results, indent=2))
    except Exception:
        click.echo(f"Failed to count predecessors for node {node_id}", err=True)
        exit(1)


@click.command()
@click.argument("node_id", required=True)
def find_neighbors(node_id):
    """Find all neighbors of a given node."""
    try:
        results = dgraph_read(queries.NEIGHBORS_QUERY, variables={"$id": node_id})
        click.echo(json.dumps(results, indent=2))
    except Exception:
        click.echo(f"Failed to find neighbors for node {node_id}", err=True)
        exit(1)


@click.command()
@click.argument("node_id", required=True)
def count_neighbors(node_id):
    """Count all neighbors of a given node."""
    try:
        results = dgraph_read(queries.COUNT_NEIGHBORS_QUERY, variables={"$id": node_id})
        click.echo(json.dumps(results, indent=2))
    except Exception:
        click.echo(f"Failed to count neighbors for node {node_id}", err=True)
        exit(1)


@click.command()
@click.argument("node_id", required=True)
def find_grandchildren(node_id):
    """Find all grandchildren (successors of successors) of a given node."""
    try:
        results = dgraph_read(queries.GRANDCHILDREN_QUERY, variables={"$id": node_id})
        click.echo(json.dumps(results, indent=2))
    except Exception:
        click.echo(f"Failed to find grandchildren for node {node_id}", err=True)
        exit(1)


@click.command()
@click.argument("node_id", required=True)
def find_grandparents(node_id):
    """Find all grandparents (predecessors of predecessors) of a given node."""
    try:
        results = dgraph_read(queries.GRANDPARENTS_QUERY, variables={"$id": node_id})
        click.echo(json.dumps(results, indent=2))
    except Exception:
        click.echo(f"Failed to find grandparents for node {node_id}", err=True)
        exit(1)


@click.command()
def count_nodes():
    """Count how many nodes there are."""
    try:
        results = dgraph_read(queries.TOTAL_NODES_QUERY)
        click.echo(json.dumps(results, indent=2))
    except Exception:
        click.echo("Failed to count total nodes", err=True)
        exit(1)


@click.command()
def count_nodes_no_successors():
    """ERROR NEED TO FIX Count nodes which do not have any successors."""
    try:
        results = dgraph_read(queries.NODES_NO_SUCCESSORS_QUERY)
        click.echo(json.dumps(results, indent=2))
    except Exception:
        click.echo("Failed to count nodes without successors", err=True)
        exit(1)


@click.command()
def count_nodes_no_predecessors():
    """ERROR NEED TO FIX Count nodes which do not have any predecessors."""
    try:
        results = dgraph_read(queries.NODES_NO_PREDECESSORS_QUERY)
        click.echo(json.dumps(results, indent=2))
    except Exception:
        click.echo("Failed to count nodes without predecessors", err=True)
        exit(1)


@click.command()
def find_nodes_most_neighbors():
    """ERROR NEED TO FIX Find nodes with the most neighbors."""
    try:
        results = dgraph_read(queries.NODES_MOST_NEIGHBORS_QUERY)
        click.echo(json.dumps(results, indent=2))
    except Exception:
        click.echo("Failed to find nodes with most neighbors", err=True)
        exit(1)


@click.command()
def count_nodes_single_neighbor():
    """ERROR NEED TO FIX Count nodes with a single neighbor."""
    try:
        results = dgraph_read(queries.NODES_SINGLE_NEIGHBOR_QUERY)
        click.echo(json.dumps(results, indent=2))
    except Exception:
        click.echo("Failed to count nodes with a single neighbor", err=True)
        exit(1)


@click.command()
@click.argument("node_id", required=True)
@click.argument("new_label", required=True)
def rename_node(node_id, new_label):
    """Rename a given node by updating its label."""
    try:
        # First locate the node UID
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
            click.echo(f"Node with ID {node_id} not found", err=True)
            exit(1)

        uid = node_info["node"][0]["uid"]

        # Create mutation using the node's UID
        mutation = f"""
        {{
            set {{
                <{uid}> <label> "{new_label}" .
            }}
        }}
        """

        # Execute the mutation
        result = dgraph_write(mutation)
        click.echo(f"Successfully renamed node {node_id} to '{new_label}'")
    except Exception as e:
        click.echo(f"Failed to rename node: {str(e)}", err=True)
        exit(1)


@click.command()
@click.argument("node_id", required=True)
def find_similar_nodes(node_id):
    """Find all 'similar' nodes that share common parents or children via the same edge type."""
    try:
        results = dgraph_read(queries.SIMILAR_NODES_QUERY, variables={"$id": node_id})
        click.echo(json.dumps(results, indent=2))
    except Exception as e:
        click.echo(f"Failed to find similar nodes for {node_id}: {str(e)}", err=True)
        exit(1)


@click.command()
@click.argument("node_id1", required=True)
@click.argument("node_id2", required=True)
def find_shortest_path(node_id1, node_id2):
    """Find the shortest path between two nodes, ignoring edge direction."""
    try:
        results = dgraph_read(
            queries.SHORTEST_PATH_QUERY, variables={"$id1": node_id1, "$id2": node_id2}
        )
        click.echo(json.dumps(results, indent=2))
    except Exception as e:
        click.echo(
            f"Failed to find path between {node_id1} and {node_id2}: {str(e)}", err=True
        )
        exit(1)


@click.command()
@click.argument("node_id", required=True)
@click.argument("distance", type=int, required=True)
def find_distant_synonyms(node_id, distance):
    """Find all distant synonyms at a specified path distance."""
    try:
        results = dgraph_read(
            queries.DISTANT_SYNONYMS_QUERY,
            variables={"$id": node_id, "$distance": distance},
        )
        click.echo(json.dumps(results, indent=2))
    except Exception as e:
        click.echo(f"Failed to find distant synonyms for {node_id}: {str(e)}", err=True)
        exit(1)


@click.command()
@click.argument("node_id", required=True)
@click.argument("distance", type=int, required=True)
def find_distant_antonyms(node_id, distance):
    """Find all distant antonyms at a specified path distance."""
    try:
        results = dgraph_read(
            queries.DISTANT_ANTONYMS_QUERY,
            variables={"$id": node_id, "$distance": distance},
        )
        click.echo(json.dumps(results, indent=2))
    except Exception as e:
        click.echo(f"Failed to find distant antonyms for {node_id}: {str(e)}", err=True)
        exit(1)


# Add all commands to the CLI group
cli.add_command(find_successors)
cli.add_command(count_successors)
cli.add_command(find_predecessors)
cli.add_command(count_predecessors)
cli.add_command(find_neighbors)
cli.add_command(count_neighbors)
cli.add_command(find_grandchildren)
cli.add_command(find_grandparents)
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
