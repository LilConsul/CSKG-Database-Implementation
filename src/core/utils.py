import json
import os
from contextlib import contextmanager
from typing import Any, Dict, Optional

import click
import pydgraph

from core.message_handler import verbose_print
from core.queries import IS_EXIST

DEBUG = os.environ.get("DEBUG", "False").lower() == "true"


@contextmanager
def dgraph_service(host: Optional[str] = None, port: Optional[int] = None):
    """
    Create a Dgraph client connection as a context manager.

    Args:
        host: Dgraph Alpha host (defaults to env var DGRAPH_HOST or 'localhost')
        port: Dgraph Alpha port (defaults to env var DGRAPH_PORT or 9080)
    """
    host = host or os.environ.get("DGRAPH_HOST", "localhost")
    port = port or int(os.environ.get("DGRAPH_PORT", "9080"))

    client_stub = pydgraph.DgraphClientStub(
        f"{host}:{port}",
        options=[
            ("grpc.max_send_message_length", 2147483647),
            ("grpc.max_receive_message_length", 2147483647),
        ],
    )
    client = pydgraph.DgraphClient(client_stub)
    try:
        yield client
    finally:
        client_stub.close()


def process_combined_values(data: Any) -> Any:
    """
    Recursively process data to convert <;> separated strings into lists.

    Args:
        data: The data to process (can be dict, list, or primitive)

    Returns:
        The processed data with <;> separated strings converted to lists
    """
    if isinstance(data, dict):
        result = {}
        for key, value in data.items():
            if isinstance(value, str) and "<;>" in value:
                if (
                    key == "id"
                    or key == "label"
                    or key.endswith("|id")
                    or key.endswith("|label")
                ):
                    result[key] = value.split("<;>")
                else:
                    result[key] = value
            else:
                result[key] = process_combined_values(value)
        return result
    elif isinstance(data, list):
        return [process_combined_values(item) for item in data]
    else:
        return data


def dgraph_read(
    query: str, variables: Optional[Dict[str, Any]] = None
) -> Dict[str, Any]:
    """
    Execute a read-only query against Dgraph.

    Args:
        query: The GraphQL+- query string
        variables: Optional variables for the query

    Returns:
        Query results as dictionary with <;> separated values converted to queryable lists
    """
    try:
        with dgraph_service() as client:
            txn = client.txn(read_only=True)
            try:
                response = txn.query(query, variables=variables)
                result = json.loads(response.json)
                # Process the combined values (with <;> separators) into lists
                processed_result = process_combined_values(result)

                return processed_result
            finally:
                txn.discard()
    except Exception as e:
        if DEBUG:
            click.echo(f"Error executing query: {str(e)}", err=True)
        raise


def dgraph_write(mutations: Any, commit_now: bool = True) -> Dict[str, Any]:
    """
    Execute write operations against Dgraph.

    Args:
        mutations: A dictionary (JSON mutation) or a string (DQL mutation)
        commit_now: Whether to commit immediately

    Returns:
        Dict containing operation results
    """
    try:
        with dgraph_service() as client:
            txn = client.txn()
            try:
                mutation_obj = pydgraph.Mutation()

                if isinstance(mutations, str):
                    mutation_obj.set_nquads = mutations.encode("utf-8")
                else:
                    mutation_obj.set_json = json.dumps(mutations).encode("utf-8")

                response = txn.mutate(mutation_obj, commit_now=commit_now)

                if not commit_now:
                    txn.commit()

                return {"uids": response.uids}

            except Exception:
                txn.discard()
                raise

            finally:
                if not commit_now and txn._state == 0:
                    txn.discard()

    except Exception as e:
        if DEBUG:
            click.echo(f"Error executing mutation: {e}", err=True)
        raise

def if_exist(node_id: str):
    """
    Check if a node exists in the database.

    Args:
        node_id: The ID of the node to check

    Returns:
        bool: True if the node exists, False otherwise
    """
    result = dgraph_read(IS_EXIST, variables={"$id": node_id})
    return bool(result["node_exists"][0]["count"])
