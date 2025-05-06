# Load data
docker exec -it dgraph_alpha dgraph live -f ./data/data.rdf.gz -s ./graphQL/main.schema -z zero:5080 -a alpha:9080

# Drop all data
docker exec -it dgraph_alpha curl -X POST localhost:8080/alter --data '{\"drop_all\": true}'