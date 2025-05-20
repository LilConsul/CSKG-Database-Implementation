#!/bin/bash

OUTPUT_FILE="work_test_results.txt"
TIMING_FILE="work_test_timings.txt"
> "$OUTPUT_FILE"
> "$TIMING_FILE"
echo "Work Test Results with Validation" > "$OUTPUT_FILE"
echo "==================================" >> "$OUTPUT_FILE"
echo "Query Execution Times" > "$TIMING_FILE"
echo "====================" >> "$TIMING_FILE"

pass() { echo -e "✅ $1" | tee -a "$OUTPUT_FILE"; }
fail() { echo -e "❌ $1" | tee -a "$OUTPUT_FILE"; }
alt()  { echo -e "⚠️  $1" | tee -a "$OUTPUT_FILE"; }

# Function to extract and save execution time
log_time() {
    command=$1
    output=$2

    # Extract the time using grep
    time_line=$(echo "$output" | grep "Query executed in" | tail -1)
    if [[ -n "$time_line" ]]; then
        # Preserve all decimal places in the execution time
        execution_time=$(echo "$time_line" | sed -E 's/Query executed in ([0-9]+\.[0-9]+) seconds/\1/' | tr -d '\n\r')

        # Save to file without color codes
        echo "⏱️  TIMING: $command completed in $execution_time seconds" >> "$OUTPUT_FILE"

        # Print to console with color
        echo -e "\033[1;36m⏱️  TIMING: $command completed in $execution_time seconds\033[0m"

        # Save timing to timing file
        echo "$command: $execution_time seconds" >> "$TIMING_FILE"
        return 0
    else
        # Save to file without color codes
        echo "⏱️  TIMING: $command - No timing information found" >> "$OUTPUT_FILE"

        # Print to console with color
        echo -e "\033[1;33m⏱️  TIMING: $command - No timing information found\033[0m"

        # Save to timing file
        echo "$command: No timing information found" >> "$TIMING_FILE"
        return 1
    fi
}

normalize() {
    echo "$1" | tr ',' '\n' | sed 's/^ *//;s/ *$//' | sort | tr '\n' ',' | sed 's/,$//'
}

check_list_match() {
    actual="$1"
    expected="$2"
    label="$3"

    norm_actual=$(normalize "$actual")
    norm_expected=$(normalize "$expected")

    if [[ "$norm_actual" == "$norm_expected" ]]; then
        pass "$label matches expected: $norm_actual"
    else
        fail "$label does NOT match.\nExpected: $norm_expected\nGot:      $norm_actual"
    fi
}

check_count_match() {
    actual="$1"
    expected="$2"
    label="$3"

    if [[ "$actual" == "$expected" ]]; then
        pass "$label count is correct: $actual"
    else
        fail "$label count is incorrect. Expected: $expected, Got: $actual"
    fi
}

check_path_length() {
    path="$1"
    expected_length="$2"
    label="$3"

    # Count nodes in path (items separated by commas)
    node_count=$(echo "$path" | tr ',' '\n' | wc -l)

    if [[ "$node_count" == "$expected_length" ]]; then
        pass "$label path length is correct: $node_count nodes. Path: $path"
    else
        fail "$label path length is incorrect. Expected: $expected_length nodes, Got: $node_count nodes. Path: $path"
    fi
}

# Extract labels from JSON format
extract_labels() {
    echo "$1" | grep -o '"label": "[^"]*"' | sed 's/"label": "\([^"]*\)"/\1/g' | tr '\n' ',' | sed 's/,$//'
}

# Extract count from JSON format
extract_count() {
    echo "$1" | grep -o '"count": [0-9]*' | sed 's/"count": \([0-9]*\)/\1/g' | head -1
}

# Run command and log timing
run_command() {
    cmd=$1
    args=$2
    command_label=$3

    echo "Running $cmd $args..."
    full_output=$(./dbcli.sh --verbose $cmd $args 2>&1)
    log_time "$command_label" "$full_output"

    echo "$full_output"
}

# Test rename-node (rename and then revert to avoid permanent changes)
test_rename_node() {
    node_id=$1
    temp_name="TEMP_NAME_FOR_TEST"
    original_label=$(basename "$node_id")

    # First rename
    echo "Renaming $node_id to $temp_name..."
    output1=$(./dbcli.sh --verbose rename-node "$node_id" "$temp_name" 2>&1)
    echo "$output1"  # Display output during execution
    log_time "Task 14 (rename)" "$output1"

    # Check if rename was successful
    if echo "$output1" | grep -q "Successfully renamed"; then
        # Verify new node exists
        renamed_node="$temp_name"
        check_output=$(./dbcli.sh find-neighbors "$renamed_node" 2>&1)

        if ! echo "$check_output" | grep -q "does not exist"; then
            # Rename back to preserve original state
            echo "Renaming back to original name..."
            output2=$(./dbcli.sh --verbose rename-node "$renamed_node" "$original_label" 2>&1)
            echo "$output2"  # Display output during execution
            log_time "Task 14 (revert)" "$output2"

            if echo "$output2" | grep -q "Successfully renamed"; then
                pass "Task 14 - Successfully renamed $node_id to $temp_name and back"
                return 0
            else
                fail "Task 14 - Failed to revert rename from $temp_name back to $original_label"
            fi
        else
            fail "Task 14 - Failed to find renamed node $renamed_node"
        fi
    else
        fail "Task 14 - Failed to rename $node_id to $temp_name"
    fi

    return 1
}

# ---------------- TASKS ---------------- #

# 1. Successors of /c/en/steam_locomotive
expected1="locomotive, steam, steam locomotive"
out1=$(run_command "find-successors" "/c/en/steam_locomotive" "Task 1")
out1_labels=$(extract_labels "$out1")
check_list_match "$out1_labels" "$expected1" "Task 1"

# 2. Count successors of /c/en/value
expected2="177"
out2=$(run_command "count-successors" "/c/en/value" "Task 2")
out2_count=$(extract_count "$out2")
check_count_match "$out2_count" "$expected2" "Task 2"

# 3. Predecessors of Q40157
expected3="primary magma, lava lamp, magma, lava lake, the floor is lava"
out3=$(run_command "find-predecessors" "Q40157" "Task 3")
out3_labels=$(extract_labels "$out3")
check_list_match "$out3_labels" "$expected3" "Task 3"

# 4. Count predecessors of /c/en/country
expected4="735"
out4=$(run_command "count-predecessors" "/c/en/country" "Task 4")
out4_count=$(extract_count "$out4")
check_count_match "$out4_count" "$expected4" "Task 4"

# 5. Neighbors of /c/en/spectrogram
expected5="scalogram, voicegram, hypnospectrogram, sonogram, spectrograms, spectrograph, gram, spectrograph"
out5=$(run_command "find-neighbors" "/c/en/spectrogram" "Task 5")
out5_labels=$(extract_labels "$out5")
check_list_match "$out5_labels" "$expected5" "Task 5"

# 6. Count neighbors of /c/en/jar
expected6="373"
out6=$(run_command "count-neighbors" "/c/en/jar" "Task 6")
out6_count=$(extract_count "$out6")
check_count_match "$out6_count" "$expected6" "Task 6"

# 7. Grandchildren of Q676 (prose)
expected7="creative work, literary class, genre fiction, written work, form, genre, literary genre, literary form, group of literary works, serial, art genre"
out7=$(run_command "find-grandchildren" "Q676" "Task 7")
out7_labels=$(extract_labels "$out7")
check_list_match "$out7_labels" "$expected7" "Task 7"

# 8. Grandparents of /c/en/ms_dos
expected8="dr dos, dos, pc dos, 8.3, ms dos, print screen, drive letter"
out8=$(run_command "find-grandparents" "/c/en/ms_dos" "Task 8")
out8_labels=$(extract_labels "$out8")
check_list_match "$out8_labels" "$expected8" "Task 8"

# 9. Total node count
expected9="2160968"
out9=$(run_command "count-nodes" "" "Task 9")
out9_count=$(extract_count "$out9")
check_count_match "$out9_count" "$expected9" "Task 9"

# 10. Nodes with no successors
expected10="649184"
out10=$(run_command "count-nodes-no-successors" "" "Task 10")
out10_count=$(extract_count "$out10")
check_count_match "$out10_count" "$expected10" "Task 10"

# 11. Nodes with no predecessors
expected11="1129781"
out11=$(run_command "count-nodes-no-predecessors" "" "Task 11")
out11_count=$(extract_count "$out11")
check_count_match "$out11_count" "$expected11" "Task 11"

# 12. Nodes with most neighbors
expected12="/c/en/slang"
out12=$(run_command "find-nodes-most-neighbors" "" "Task 12")
out12_id=$(echo "$out12" | grep -o '"id": "[^"]*"' | sed 's/"id": "\([^"]*\)"/\1/g' | head -1)
[[ "$out12_id" == "$expected12" ]] && pass "Task 12 - Node with most neighbors is $out12_id" || fail "Task 12 - Expected: $expected12, Got: $out12_id"

# 13. Count of single neighbor nodes
expected13="1276217"
out13=$(run_command "count-nodes-single-neighbor" "" "Task 13")
out13_count=$(extract_count "$out13")
check_count_match "$out13_count" "$expected13" "Task 13"

# 14. Test rename functionality
test_rename_node "/c/en/test_node"

# 15. Similar nodes to /c/en/emission_nebula
expected15_count="13"
out15=$(run_command "find-similar-nodes" "/c/en/emission_nebula" "Task 15")
out15_count=$(echo "$out15" | grep -o '"id": "[^"]*"' | wc -l)
check_count_match "$out15_count" "$expected15_count" "Task 15"

# 16. Check path between /c/en/flower and /c/en/spacepower - just validate path length = 4
expected16_length="4"
out16=$(run_command "find-shortest-path" "/c/en/flower /c/en/spacepower" "Task 16.1")
out16_labels=$(extract_labels "$out16")
check_path_length "$out16_labels" "$expected16_length" "Task 16.1"

# Check path between /c/en/uchuva and /c/en/square_sails/n - just validate path length = 8
expected16b_length="8"
out16b=$(run_command "find-shortest-path" "/c/en/uchuva /c/en/square_sails/n" "Task 16.2")
out16b_labels=$(extract_labels "$out16b")
check_path_length "$out16b_labels" "$expected16b_length" "Task 16.2"

# 17. Distant synonyms of /c/en/defeatable
expected17="attainable, surmountable, superable, conquerable, beatable, possible, weak, surmountable, subduable, subjugable, vanquishable, vincible"
out17=$(run_command "find-distant-synonyms" "/c/en/defeatable 2" "Task 17")
out17_labels=$(extract_labels "$out17")
check_list_match "$out17_labels" "$expected17" "Task 17"

# 18. Distant antonyms of /c/en/automate
expected18="civilize, cultivate, personify, refine, temper, teach, tame"
out18=$(run_command "find-distant-antonyms" "/c/en/automate 3" "Task 18")
out18_labels=$(extract_labels "$out18")
check_list_match "$out18_labels" "$expected18" "Task 18"

echo -e "\nAll tasks completed. Results saved to $OUTPUT_FILE"
echo -e "Timing information saved to $TIMING_FILE"
