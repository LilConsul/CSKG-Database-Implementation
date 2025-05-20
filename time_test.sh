#!/bin/bash

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
        # Use tr to remove any newlines from the extracted time
        execution_time=$(echo "$time_line" | sed -E 's/Query executed in ([0-9]+\.[0-9]+) seconds/\1/' | tr -d '\n\r')
        echo "$command: $execution_time seconds" >> $OUTPUT_FILE
    else
        echo "$command: No timing information found" >> $OUTPUT_FILE
    fi
}

# Run commands with no arguments
run_no_arg_command() {
    cmd=$1
    echo "Running $cmd..."
    output=$(./dbcli.sh --verbose $cmd 2>&1)
    extract_time "$cmd" "$output"
}

# Run commands with one node ID argument
run_one_arg_command() {
    cmd=$1
    node_id=$2
    echo "Running $cmd for node $node_id..."
    output=$(./dbcli.sh --verbose $cmd "$node_id" 2>&1)
    extract_time "$cmd $node_id" "$output"
}

# Run commands with two arguments
run_two_arg_command() {
    cmd=$1
    arg1=$2
    arg2=$3
    echo "Running $cmd with args $arg1 $arg2..."
    output=$(./dbcli.sh --verbose $cmd "$arg1" "$arg2" 2>&1)
    extract_time "$cmd $arg1 $arg2" "$output"
}

# Test rename-node (rename and then revert to avoid permanent changes)
test_rename_node() {
    node_id=$1
    temp_name="TEMP_NAME_FOR_TIMING_TEST"
    original_name=$(basename "$node_id")

    echo "Testing rename-node by renaming $node_id to $temp_name and back..."
    # First rename
    echo "Renaming $node_id to $temp_name..."
    output1=$(./dbcli.sh --verbose rename-node "$node_id" "$temp_name" 2>&1)
    echo "$output1"  # Display output during execution
    extract_time "rename-node (first rename)" "$output1"

    # Check if rename was successful
    if echo "$output1" | grep -q "Successfully renamed"; then
        # Verify new node exists
        renamed_node="$temp_name"
        check_output=$(./dbcli.sh find-neighbors "$renamed_node" 2>&1)

        if ! echo "$check_output" | grep -q "does not exist"; then
            # Rename back to preserve original state
            echo "Renaming back to original name..."
            output2=$(./dbcli.sh --verbose rename-node "$renamed_node" "$original_name" 2>&1)
            echo "$output2"  # Display output during execution
            extract_time "rename-node (revert)" "$output2"

            echo "Node renamed back to original name."
        else
            echo "WARNING: Could not find renamed node $renamed_node"
        fi
    else
        echo "WARNING: Failed to rename $node_id to $temp_name"
    fi
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

# Test rename-node with a test node
TEST_NODE="/c/en/happy"
test_rename_node "$TEST_NODE"

echo "All queries completed. Timing results saved to $OUTPUT_FILE"

