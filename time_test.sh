#!/bin/bash

# Start the database with -y flag to skip dialogs
echo "Starting the database..."
./dbcli.sh run -y

# Path to the output file for time measurements
OUTPUT_FILE="query_times.txt"

# Clear the output file
> $OUTPUT_FILE
echo "Query Execution Times" > $OUTPUT_FILE
echo "====================" >> $OUTPUT_FILE

# Function to extract and save execution time
extract_time() {
    command=$1
    output=$2

    # Extract the time using grep
    time_line=$(echo "$output" | grep "Query executed in" | tail -1)
    if [[ -n "$time_line" ]]; then
        execution_time=$(echo "$time_line" | sed -E 's/Query executed in ([0-9]+\.[0-9]+) seconds/\1/')
        echo "$command: $execution_time seconds" >> $OUTPUT_FILE
    else
        echo "$command: No timing information found" >> $OUTPUT_FILE
    fi
}

# Run commands with no arguments
run_no_arg_command() {
    cmd=$1
    echo "Running $cmd..."
    output=$(./dbcli.sh $cmd --verbose 2>&1)
    extract_time "$cmd" "$output"
}

# Run commands with one node ID argument
run_one_arg_command() {
    cmd=$1
    node_id=$2
    echo "Running $cmd for node $node_id..."
    output=$(./dbcli.sh $cmd --verbose "$node_id" 2>&1)
    extract_time "$cmd $node_id" "$output"
}

# Run commands with two arguments
run_two_arg_command() {
    cmd=$1
    arg1=$2
    arg2=$3
    echo "Running $cmd with args $arg1 $arg2..."
    output=$(./dbcli.sh $cmd --verbose "$arg1" "$arg2" 2>&1)
    extract_time "$cmd $arg1 $arg2" "$output"
}

echo "Running all queries with --verbose flag..."

# No argument commands
run_no_arg_command "count-nodes"
run_no_arg_command "count-nodes-no-successors"
run_no_arg_command "count-nodes-no-predecessors"
run_no_arg_command "count-nodes-single-neighbor"
run_no_arg_command "find-nodes-most-neighbors"

# One node ID argument commands
EXAMPLE_NODE_ID="/c/en/happy"

run_one_arg_command "find-successors" "$EXAMPLE_NODE_ID"
run_one_arg_command "count-successors" "$EXAMPLE_NODE_ID"
run_one_arg_command "find-predecessors" "$EXAMPLE_NODE_ID"
run_one_arg_command "count-predecessors" "$EXAMPLE_NODE_ID"
run_one_arg_command "find-neighbors" "$EXAMPLE_NODE_ID"
run_one_arg_command "count-neighbors" "$EXAMPLE_NODE_ID"
run_one_arg_command "find-grandchildren" "$EXAMPLE_NODE_ID"
run_one_arg_command "find-grandparents" "$EXAMPLE_NODE_ID"
run_one_arg_command "find-similar-nodes" "$EXAMPLE_NODE_ID"

# Commands with two arguments
NODE_ID1="/c/en/happy"
NODE_ID2="/c/en/sad"
DISTANCE=2

run_two_arg_command "find-shortest-path" "$NODE_ID1" "$NODE_ID2"
run_two_arg_command "find-distant-synonyms" "$NODE_ID1" "$DISTANCE"
run_two_arg_command "find-distant-antonyms" "$NODE_ID1" "$DISTANCE"

# Skip rename-node as it modifies data

echo "All queries completed. Timing results saved to $OUTPUT_FILE"