#!/bin/sh

DIR="./data"
FILE="$DIR/cskg.tsv.gz"
RDF_FILE="$DIR/data.rdf.gz"
STORAGE_DIR="./storage"
SERVER_READY_MARKER="Server is ready"


download_file() {
  echo "Downloading file..."
  curl -L https://zenodo.org/records/4331372/files/cskg.tsv.gz?download=1 -o "$FILE"
}

convert_file() {
  echo "Converting file..."
  docker-compose up -d dbcli
  docker exec -it dbcli_python uv run /code/rdf_convert.py
}

delete_dir() {
  local dir="$1"
  if [ -d "$dir" ]; then
    rm -rf "$dir" 2>/dev/null || {
      echo "Failed to remove $dir with standard permissions, trying with sudo..."
      sudo rm -rf "$dir" || echo "Warning: Failed to remove $dir, please check permissions"
    }
  fi
}

start() {
  echo "Starting the docker. Please wait..."
  docker-compose up -d

  echo "Waiting for services to initialize..."
  echo "Watching logs from alpha service until ready..."
  docker-compose logs -f alpha | {
    while IFS= read -r line; do
      echo "$line"
      if echo "$line" | grep -q "$SERVER_READY_MARKER"; then
        break
      fi
    done
  }
  echo "Dgraph Alpha server is ready!"
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

  delete_dir "$STORAGE_DIR"
  delete_dir "$DIR"

  echo "Cleanup completed"
}


setup() {
  auto_confirm="$1"

  if [ -d "$STORAGE_DIR" ]; then
    echo "Storage directory exists: $STORAGE_DIR"
    start
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
    start
  fi
}

# Main entrypoint logic
if ! [ -x "$(command -v docker)" ]; then
  echo "Error: Docker is not installed or not in PATH."
  exit 1
fi

case "$1" in
  "cleanup")
    cleanup
    ;;
  "run")
    setup "$2"
    ;;
  "stop")
    stop
    ;;
*) # Default case - run the python query script
    retries=5
    while [ $retries -gt 0 ]; do
      if curl -s http://localhost:8080/health | grep -q "healthy"; then
        docker exec -it dbcli_python uv run /code/main.py "$@"
        exit 0
      else
        echo "Server not ready. Retrying in 5 seconds... ($retries retries left)"
        retries=$((retries - 1))
        sleep 5
      fi
    done
    echo "Server not ready after multiple attempts. Run <$0 setup> or <$0 run> first..."
    ;;
esac