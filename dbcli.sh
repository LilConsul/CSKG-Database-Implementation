#!/bin/sh

DIR="./data"
FILE="$DIR/cskg.tsv.gz"
RDF_FILE="$DIR/data.rdf.gz"
STORAGE_DIR="./storage"

download_file() {
  echo "Downloading file..."
  curl -L https://zenodo.org/records/4331372/files/cskg.tsv.gz?download=1 -o "$FILE"
}

convert_file() {
  echo "Converting file..."
  docker-compose up -d dbcli
  docker exec -it dbcli_python uv run /code/rdf_convert.py
}

run() {
  echo "Starting the docker. Please wait..."
  docker-compose up
}

stop() {
  echo "Stopping the docker. Please wait..."
  docker-compose down
}

cleanup() {
  echo "Executing cleanup function..."
  docker-compose down -v --remove-orphans
  docker system prune -f
  echo "Docker cleaned. Removing directories..."
  sudo rm -rf "$STORAGE_DIR"
  sudo rm -rf "$DIR"
  echo "Cleanup completed successfully"
}

setup() {
  local auto_confirm="$1"

  if [ -d "$STORAGE_DIR" ]; then
    echo "Storage directory exists: $STORAGE_DIR"
    echo "No need to run setup. You can try using the ./dbcli.sh run or ./dbcli.sh cleanup"
    return
  fi

  echo "Executing setup function..."
  mkdir -p "$DIR"

  if [ ! -f "$FILE" ]; then
    echo "Data file not found: $FILE"

    if [ "$auto_confirm" = "-y" ] || {
      echo "Would you like to download the file? [y/n]";
      read -r response; [ "$response" = "y" ];
      }; then
      download_file
    else
      echo "Please, download the file and put it into $DIR folder, so it will be $FILE."
      echo "After that, press any key to continue..."
      read -r
    fi
  else
    echo "File exists: $FILE"
  fi

  if [ ! -f "$RDF_FILE" ]; then
    echo "RDF file not found: $RDF_FILE"
    convert_file
  else
    echo "RDF file exists: $RDF_FILE"
  fi

  if [ "$auto_confirm" = "-y" ] || {
    echo "Everything is now all setup, would you like to start the program? [y/n]";
    read -r response;
    [ "$response" = "y" ];
    }; then
    run
  fi
}

# Main entrypoint logic
if ! [ -x "$(command -v docker)" ]; then
  echo "Error: Docker is not installed or not in PATH."
  exit 1
fi

case "$1" in
  "setup")
    setup "$2"
    ;;
  "cleanup")
    cleanup
    ;;
  "run")
    run
    ;;
  "stop")
    stop
    ;;
  *)
    docker exec -it dbcli_python uv run /code/main.py "$@"
    ;;
esac
