import json
import pydgraph


def main():
    # Connect to the Dgraph server
    client_stub = pydgraph.DgraphClientStub("localhost:9080")
    client = pydgraph.DgraphClient(client_stub)

    # Create a new transaction
    txn = client.txn(read_only=True)

    try:
        query = """
            {
              data(func: has(id), first: 100) {
                id
                from:~to @facets(eq(id, "/r/RelatedTo"))
                  @facets(label) {
                    label
                }
                to:to @facets(eq(id, "/r/RelatedTo"))
                  @facets(label) {
                    label
                }
            
              }
            }
        """

        res1 = client.txn(read_only=True).query(query)
        ppl1 = json.loads(res1.json)

        print("response:", ppl1)

    finally:
        txn.discard()
        client_stub.close()
