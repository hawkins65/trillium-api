#!/bin/bash

# Set locale to enable thousands separator
export LC_NUMERIC="en_US.UTF-8"

# Function to log messages with timestamp and level
log_message() {
    local level=$1
    local message=$2
    printf "[%s] %s - %s\n" "$(date +'%Y-%m-%d %H:%M:%S')" "$level" "$message"
}


# Directory containing the JSON files
DIR="/home/smilax/block-production/leaderboard/production/static/json"
PREFIX="skip_analysis_epoch_"
SUFFIX=".json"

# Progress bar function
print_progress() {
    local current=$1
    local total=$2
    local width=50  # Width of the progress bar
    local progress=$((current * width / total))
    local percent=$((current * 100 / total))

    # Build the progress bar
    printf "\r["
    for ((i = 0; i < progress; i++)); do printf "="; done
    for ((i = progress; i < width; i++)); do printf " "; done
    printf "] %d%% (%d/%d)" "$percent" "$current" "$total"
}

# Initialize variables for summary
total_validators=0
count_files=0
total_leader_groups_4_slots=0
total_blocks_produced=0
total_slots_skipped=0
total_slots=0
total_slot_1_skipped=0
total_slot_2_skipped=0
total_slot_3_skipped=0
total_slot_4_skipped=0
total_groups_with_1_skip=0
total_groups_with_2_skips=0
total_groups_with_3_skips=0
total_groups_with_4_skips=0

# Track epochs for range and skipped epochs
epochs=()

# Count total files to process
total_files=$(ls "$DIR/$PREFIX"???$SUFFIX 2>/dev/null | wc -l)
current_file=0

# Loop through all matching files
for file in "$DIR/$PREFIX"???$SUFFIX; do
    if [ -f "$file" ]; then
        current_file=$((current_file + 1))
        log_progress "$current_file" "$total_files"

        # Extract epoch number
        epoch=$(basename "$file" | sed -E "s/[^0-9]*([0-9]+).*/\1/")
        epochs+=("$epoch")
        
        # Extract values using jq
        epoch_summary=$(jq '.summary' "$file")
        total_validators=$((total_validators + $(echo "$epoch_summary" | jq '.total_validators')))
        total_leader_groups_4_slots=$((total_leader_groups_4_slots + $(echo "$epoch_summary" | jq '.total_leader_groups_4_slots')))
        total_blocks_produced=$((total_blocks_produced + $(echo "$epoch_summary" | jq '.total_blocks_produced')))
        total_slots_skipped=$((total_slots_skipped + $(echo "$epoch_summary" | jq '.total_slots_skipped')))
        total_slots=$((total_slots + $(echo "$epoch_summary" | jq '.total_slots')))
        total_slot_1_skipped=$((total_slot_1_skipped + $(echo "$epoch_summary" | jq '.total_slot_1_skipped')))
        total_slot_2_skipped=$((total_slot_2_skipped + $(echo "$epoch_summary" | jq '.total_slot_2_skipped')))
        total_slot_3_skipped=$((total_slot_3_skipped + $(echo "$epoch_summary" | jq '.total_slot_3_skipped')))
        total_slot_4_skipped=$((total_slot_4_skipped + $(echo "$epoch_summary" | jq '.total_slot_4_skipped')))
        total_groups_with_1_skip=$((total_groups_with_1_skip + $(echo "$epoch_summary" | jq '.total_groups_with_1_skip')))
        total_groups_with_2_skips=$((total_groups_with_2_skips + $(echo "$epoch_summary" | jq '.total_groups_with_2_skips')))
        total_groups_with_3_skips=$((total_groups_with_3_skips + $(echo "$epoch_summary" | jq '.total_groups_with_3_skips')))
        total_groups_with_4_skips=$((total_groups_with_4_skips + $(echo "$epoch_summary" | jq '.total_groups_with_4_skips')))
    fi
done

# Finalize progress bar
log_progress "$total_files" "$total_files"
log_message "INFO" ""  # Move to the next line

# Sort and determine the range of epochs
sorted_epochs=($(echo "${epochs[@]}" | tr ' ' '\n' | sort -n))
epoch_start=${sorted_epochs[0]}
epoch_end=${sorted_epochs[-1]}

# Detect skipped epochs
skipped_epochs=()
for ((epoch=epoch_start; epoch<=epoch_end; epoch++)); do
    if [[ ! " ${sorted_epochs[@]} " =~ " $epoch " ]]; then
        skipped_epochs+=("$epoch")
    fi
done

# Format skipped epochs as a comma-separated list
skipped_epochs_list=$(IFS=', '; echo "${skipped_epochs[*]}")

# Calculate averages and percentages using bc for floating-point arithmetic
average_validators=$(echo "scale=0; ($total_validators + $total_files/2) / $total_files" | bc)
overall_slot_skip_percentage=$(echo "scale=2; ($total_slots_skipped * 100) / $total_slots" | bc)
percentage_slot_1_skipped=$(echo "scale=2; ($total_slot_1_skipped * 100) / $total_slots" | bc)
percentage_slot_2_skipped=$(echo "scale=2; ($total_slot_2_skipped * 100) / $total_slots" | bc)
percentage_slot_3_skipped=$(echo "scale=2; ($total_slot_3_skipped * 100) / $total_slots" | bc)
percentage_slot_4_skipped=$(echo "scale=2; ($total_slot_4_skipped * 100) / $total_slots" | bc)
percentage_groups_with_1_skip=$(echo "scale=2; ($total_groups_with_1_skip * 100) / $total_leader_groups_4_slots" | bc)
percentage_groups_with_2_skips=$(echo "scale=2; ($total_groups_with_2_skips * 100) / $total_leader_groups_4_slots" | bc)
percentage_groups_with_3_skips=$(echo "scale=2; ($total_groups_with_3_skips * 100) / $total_leader_groups_4_slots" | bc)
percentage_groups_with_4_skips=$(echo "scale=2; ($total_groups_with_4_skips * 100) / $total_leader_groups_4_slots" | bc)

# Display results
log_message "INFO" "Summary across epochs:"
log_message "INFO" "Processed epochs: $(printf "%\'d" "$total_files")"
log_message "INFO" "Epoch range: $epoch_start to $epoch_end"
if [ -z "$skipped_epochs_list" ]; then
    log_message "INFO" "No skipped epochs detected."
else
    log_message "INFO" "Skipped epochs: $skipped_epochs_list"
fi
log_message "INFO" "Average total_validators: $(printf "%\'d" "$average_validators")"
log_message "INFO" "Total total_leader_groups_4_slots: $(printf "%\'d" "$total_leader_groups_4_slots")"
log_message "INFO" "Total total_blocks_produced: $(printf "%\'d" "$total_blocks_produced")"
log_message "INFO" "Total total_slots_skipped: $(printf "%\'d" "$total_slots_skipped")"
log_message "INFO" "Overall slot skip percentage: $(printf "%.2f%%" "$overall_slot_skip_percentage")"
log_message "INFO" "Total and percentage of total_slot_1_skipped: $(printf "%\'d (%.2f%%)" "$total_slot_1_skipped" "$percentage_slot_1_skipped")"
log_message "INFO" "Total and percentage of total_slot_2_skipped: $(printf "%\'d (%.2f%%)" "$total_slot_2_skipped" "$percentage_slot_2_skipped")"
log_message "INFO" "Total and percentage of total_slot_3_skipped: $(printf "%\'d (%.2f%%)" "$total_slot_3_skipped" "$percentage_slot_3_skipped")"
log_message "INFO" "Total and percentage of total_slot_4_skipped: $(printf "%\'d (%.2f%%)" "$total_slot_4_skipped" "$percentage_slot_4_skipped")"
log_message "INFO" "Total and percentage of total_groups_with_1_skip: $(printf "%\'d (%.2f%%)" "$total_groups_with_1_skip" "$percentage_groups_with_1_skip")"
log_message "INFO" "Total and percentage of total_groups_with_2_skips: $(printf "%\'d (%.2f%%)" "$total_groups_with_2_skips" "$percentage_groups_with_2_skips")"
log_message "INFO" "Total and percentage of total_groups_with_3_skips: $(printf "%\'d (%.2f%%)" "$total_groups_with_3_skips" "$percentage_groups_with_3_skips")"
log_message "INFO" "Total and percentage of total_groups_with_4_skips: $(printf "%\'d (%.2f%%)" "$total_groups_with_4_skips" "$percentage_groups_with_4_skips")"}