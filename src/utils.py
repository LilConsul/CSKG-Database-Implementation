# import json
# import os
# from contextlib import contextmanager
# from typing import Any, Dict, Optional
#
# import click
# import pydgraph
#
# DEBUG = os.environ.get("DEBUG", "False").lower() == "true"
#
#
# @contextmanager
# def dgraph_service(host: Optional[str] = None, port: Optional[int] = None):
#     """
#     Create a Dgraph client connection as a context manager.
#
#     Args:
#         host: Dgraph Alpha host (defaults to env var DGRAPH_HOST or 'localhost')
#         port: Dgraph Alpha port (defaults to env var DGRAPH_PORT or 9080)
#     """
#     host = host or os.environ.get("DGRAPH_HOST", "localhost")
#     port = port or int(os.environ.get("DGRAPH_PORT", "9080"))
#
#     client_stub = pydgraph.DgraphClientStub(
#         f"{host}:{port}",
#         options=[
#             ("grpc.max_send_message_length", 2147483647),
#             ("grpc.max_receive_message_length", 2147483647),
#         ],
#     )
#     client = pydgraph.DgraphClient(client_stub)
#     try:
#         yield client
#     finally:
#         client_stub.close()
#
#
# def dgraph_read(
#     query: str, variables: Optional[Dict[str, Any]] = None
# ) -> Dict[str, Any]:
#     """
#     Execute a read-only query against Dgraph.
#
#     Args:
#         query: The GraphQL+- query string
#         variables: Optional variables for the query
#
#     Returns:
#         Query results as dictionary
#     """
#     try:
#         with dgraph_service() as client:
#             txn = client.txn(read_only=True)
#             try:
#                 response = txn.query(query, variables=variables)
#                 return json.loads(response.json)
#             finally:
#                 txn.discard()
#     except Exception as e:
#         if DEBUG:
#             click.echo(f"Error executing query: {str(e)}", err=True)
#         raise
#
#
# def dgraph_write(mutations: Any, commit_now: bool = True) -> Dict[str, Any]:
#     """
#     Execute write operations against Dgraph.
#
#     Args:
#         mutations: A dictionary (JSON mutation) or a string (DQL mutation)
#         commit_now: Whether to commit immediately
#
#     Returns:
#         Dict containing operation results
#     """
#     try:
#         with dgraph_service() as client:
#             txn = client.txn()
#             try:
#                 mutation_obj = pydgraph.Mutation()
#
#                 if isinstance(mutations, str):
#                     mutation_obj.set_nquads = mutations.encode("utf-8")
#                 else:
#                     mutation_obj.set_json = json.dumps(mutations).encode("utf-8")
#
#                 response = txn.mutate(mutation_obj, commit_now=commit_now)
#
#                 if not commit_now:
#                     txn.commit()
#
#                 return {"uids": response.uids}
#
#             except Exception:
#                 txn.discard()
#                 raise
#
#             finally:
#                 if not commit_now and txn._state == 0:
#                     txn.discard()
#
#     except Exception as e:
#         if DEBUG:
#             click.echo(f"Error executing mutation: {e}", err=True)
#         raise

import json
import os
from typing import Any, Dict, Optional

import click
import requests

DEBUG = os.environ.get("DEBUG", "False").lower() == "true"


def dgraph_read(
    query: str, variables: Optional[Dict[str, Any]] = None
) -> Dict[str, Any]:
    """
    Execute a read-only query against Dgraph using HTTP API.

    Args:
        query: The GraphQL+- query string
        variables: Optional variables for the query

    Returns:
        Query results as dictionary
    """
    try:
        host = os.environ.get("DGRAPH_HOST", "localhost")
        port = int(os.environ.get("DGRAPH_PORT_HTTP", "8080"))

        url = f"http://{host}:{port}/query?ro=true"
        headers = {"Content-Type": "application/json"}

        # Prepare the request payload
        payload = {"query": query}
        if variables:
            payload["variables"] = variables

        response = requests.post(url, headers=headers, json=payload)

        if response.status_code == 200:
            return response.json()["data"]
        else:
            raise Exception(
                f"Query failed with status {response.status_code}: {response.text}"
            )

    except Exception as e:
        if DEBUG:
            click.echo(f"Error executing query: {str(e)}", err=True)
        raise


def dgraph_write(mutations: Any, commit_now: bool = True) -> Dict[str, Any]:
    """
    Execute write operations against Dgraph using HTTP API.

    Args:
        mutations: A dictionary (JSON mutation) or a string (DQL mutation)
        commit_now: Whether to commit immediately

    Returns:
        Dict containing operation results
    """
    try:
        host = os.environ.get("DGRAPH_HOST", "localhost")
        port = int(os.environ.get("DGRAPH_PORT_HTTP", "8080"))

        url = (
            f"http://{host}:{port}/mutate?commitNow={'true' if commit_now else 'false'}"
        )
        headers = {"Content-Type": "application/json"}

        # Prepare the mutation payload
        if isinstance(mutations, str):
            # DQL mutation (nquads)
            payload = {"nquads": mutations}
            headers["Content-Type"] = "application/rdf"
        else:
            # JSON mutation
            payload = {"set": mutations.get("set", [])}
            if "delete" in mutations:
                payload["delete"] = mutations["delete"]

        response = requests.post(url, headers=headers, json=payload)

        if response.status_code == 200:
            result = response.json()
            return {"uids": result.get("uids", {})}
        else:
            raise Exception(
                f"Mutation failed with status {response.status_code}: {response.text}"
            )

    except Exception as e:
        if DEBUG:
            click.echo(f"Error executing mutation: {e}", err=True)
        raise