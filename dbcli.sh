#!/bin/sh

DIR="./data"
FILE="$DIR/cskg.tsv.gz"
STORAGE_DIR="./storage"

setup() {
  if [ -d "$STORAGE_DIR" ]; then
    echo "Storage directory exists: $STORAGE_DIR"
    echo "No need to run setup. You can try using the ./dbcli.sh run or ./dbcli.sh cleanup"
    return
  fi

  echo "Executing setup function..."
  mkdir -p "$DIR"
  if [ -f "$FILE" ]; then
    echo "File exists: $FILE"
  else
    echo "Data file not found: $FILE"
    echo "Would you like to download the file? [y/n]"
    read -r response
    if [ "$response" = "y" ]; then
      echo "Downloading file..."
      curl -L https://zenodo.org/records/4331372/files/cskg.tsv.gz?download=1 -o "$FILE"
    else
      echo "Please, download the file and put it into $DIR folder, so it will be $FILE."
      echo "After that, press any key to continue..."
      read -r
    fi
  fi
  echo "TO BE IMPLEMENTED: Convert the file to RDF format"
  docker-compose up -d
}

cleanup() {
  echo "Executing cleanup function..."
  docker-compose down -v --rmi all --remove-orphans
  docker system prune -a --volumes -f
  rm -rf "$STORAGE_DIR"
  rm -rf "$DIR"
  echo "Cleanup completed successfully"
}

run() {
  echo "Starting the docker. Please wait..."
  docker-compose up -d
}

stop() {
  echo "Stopping the docker. Please wait..."
  docker-compose down
}

if [ "$1" = "setup" ]; then
  setup
elif [ "$1" = "cleanup" ]; then
  clenaup
elif [ "$1" = "run" ]; then
  run
elif [ "$1" = "stop" ]; then
  stop
else
  docker exec -it $(docker-compose ps -q dbcli) poetry run python /app/main.py "$@"
fi