import json

import click

import queries
from utils import dgraph_read


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

if __name__ == "__main__":
    cli()
