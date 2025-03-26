import json
import os
from contextlib import contextmanager
from typing import Any, Dict, Optional

import click
import pydgraph


@contextmanager
def dgraph_service(host: Optional[str] = None, port: Optional[int] = None):
    """
    Create a Dgraph client connection as a context manager.

    Args:
        host: Dgraph Alpha host (defaults to env var DGRAPH_HOST or 'localhost')
        port: Dgraph Alpha port (defaults to env var DGRAPH_PORT or 9080)
    """
    host = host or os.environ.get("DGRAPH_HOST", "dgraph_alpha")
    port = port or int(os.environ.get("DGRAPH_PORT", "9080"))

    client_stub = pydgraph.DgraphClientStub(f"{host}:{port}")
    client = pydgraph.DgraphClient(client_stub)
    try:
        yield client
    finally:
        client_stub.close()


def dgraph_read(
    query: str, variables: Optional[Dict[str, Any]] = None
) -> Dict[str, Any]:
    """
    Execute a read-only query against Dgraph.

    Args:
        query: The GraphQL+- query string
        variables: Optional variables for the query

    Returns:
        Query results as dictionary
    """
    try:
        with dgraph_service() as client:
            txn = client.txn(read_only=True)
            try:
                response = txn.query(query, variables=variables)
                return json.loads(response.json)
            finally:
                txn.discard()
    except Exception as e:
        click.echo(f"Error executing query: {str(e)}", err=True)
        raise


def dgraph_write(mutations, commit_now: bool = True) -> Dict[str, Any]:
    """
    Execute write operations against Dgraph.

    Args:
        mutations: List of mutations to apply or a string containing a mutation
        commit_now: Whether to commit immediately

    Returns:
        Operation results
    """
    try:
        with dgraph_service() as client:
            txn = client.txn()
            try:
                if isinstance(mutations, str):
                    # Handle string mutation (DQL format)
                    response = txn.mutate(mutation=mutations, commit_now=commit_now)
                else:
                    # Handle object mutations
                    response = txn.mutate(set_obj=mutations, commit_now=commit_now)

                if not commit_now:
                    txn.commit()
                return {"uids": response.uids}
            except Exception:
                txn.discard()
                raise
            finally:
                if not commit_now and txn._state == 0:  # If still open
                    txn.discard()
    except Exception as e:
        click.echo(f"Error executing mutation: {str(e)}", err=True)
        raise
