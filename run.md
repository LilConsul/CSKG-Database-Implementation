To load data and schema from rdf file:
```bash
docker exec -it dgraph_alpha dgraph live -f /dgraph/data/data.rdf.gz -s /dgraph/data/main.schema --batch_size=5000 --zero=zero:5080
```

Load graphQL schema:
```bash
docker exec -it dgraph_alpha curl -X POST -H "Content-Type: application/json" --data-binary '@/dgraph/graphQL/schema.graphql' http://localhost:8080/admin/schema
```

Load data from rdf file:
```bash
docker exec -it dgraph_alpha dgraph live -f /dgraph/data/data.rdf --zero=zero:5080
```

Load data from json file:
```bash
docker exec -it dgraph_alpha dgraph live -f /dgraph/data/data-100k.json --format=json --zero=zero:5080
```

Validate JSON file:
```bash
jq empty ./data/data.json && echo "Valid JSON" || echo "Invalid JSON"
```

Bulk loader:
```bash
docker exec -it dgraph_alpha dgraph bulk -f /dgraph/data/data.json -g /dgraph/graphQL/schema.graphql --format json --zero=zero:5080
```