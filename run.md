To load data and schema from rdf file:
```bash
docker exec -it dgraph_alpha dgraph live -f /dgraph/data/data.rdf -s /dgraph/data/schema.txt --zero=zero:5080
```

Load graphQL schema:
```bash
docker exec -it dgraph_alpha curl -X POST localhost:8080/admin/schema --data-binary '@/dgraph/graphQL/schema.graphql'
```

Load data from rdf file:
```bash
docker exec -it dgraph_alpha dgraph live -f /dgraph/data/data.rdf --zero=zero:5080
```

Load data from json file:
```bash
docker exec -it dgraph_alpha dgraph live -f /dgraph/data/data.json --format=json --zero=zero:5080
```