To load data and schema from rdf file:
```bash
docker exec -it dgraph_alpha dgraph live -f /dgraph/data/data.rdf -s /dgraph/data/schema.txt --zero=zero:5080
```