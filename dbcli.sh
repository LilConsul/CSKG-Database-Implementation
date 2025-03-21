#!/bin/sh
docker exec -it $(docker-compose ps -q dbcli) poetry run python /app/main.py "$@"
