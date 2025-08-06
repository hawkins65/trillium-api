#!/bin/bash

# Source path initialization
source "$(dirname "$0")/000_init_paths.sh" || {
    echo "❌ Failed to source path initialization script" >&2
    exit 1
}

source $HOME/trillium_api/scripts/bash/999_common_log.sh

# Parse command line arguments
if [[ "$#" -ne 1 ]]; then
    log_message "ERROR" "Usage: $0 <epoch-number>"
    exit 1
fi

epoch_number="$1"

# You can now use $epoch_number as needed
log_message "INFO" "Epoch number: $epoch_number"

# Save the current directory so we can come back at the end
ORIGINAL_DIR=$(pwd)

# Switch to the directory to run the script
target_dir="$TRILLIUM_DATA_EPOCHS/epoch$epoch_number"
log_message "INFO" "Switching to $target_dir"

if ! cd "$target_dir"; then
    log_message "ERROR" "Failed to change directory to $target_dir"
    exit 1
fi

# Function to execute a command and check for errors
execute_and_check() {
    local cmd="$1"
    log_message "INFO" "Executing: $cmd"
    eval "$cmd"
    local exit_code=$?

    if [[ $exit_code -ne 0 ]]; then
        log_message "ERROR" "Command failed with exit code $exit_code: $cmd"
        exit $exit_code
    fi
    return $exit_code
}

# Main loop to check slots every 5 minutes
while true; do
    # Get current timestamp
    current_timestamp=$(date '+%Y-%m-%d %H:%M:%S')

    # Read previous slots and timestamp (if exists)
    PREV_SLOTS_FILE="/tmp/previous_slots_to_process_epoch${epoch_number}.txt"
    if [[ -f "$PREV_SLOTS_FILE" ]]; then
        read previous_slots previous_timestamp < <(cat "$PREV_SLOTS_FILE" | tr ',' ' ')
    else
        previous_slots=0
        previous_timestamp="$current_timestamp"
    fi

    # Run the Python script to get the current number of slots
    execute_and_check "python3 $ORIGINAL_DIR/999_monitor_get_slots.py $epoch_number"

    # Read the current number of slots
    CURRENT_SLOTS_FILE="/tmp/slots_to_process_epoch${epoch_number}.txt"
    if [[ -f "$CURRENT_SLOTS_FILE" ]]; then
        current_slots=$(cat "$CURRENT_SLOTS_FILE")
    else
        log_message "ERROR" "Temporary file $CURRENT_SLOTS_FILE not found"
        exit 1
    fi

    # Store the current number of slots and timestamp for the next run
    echo "$current_slots,$current_timestamp" > "$PREV_SLOTS_FILE"

    # Calculate the difference (slots processed)
    difference=$((previous_slots - current_slots))

    # Calculate time difference in minutes
    previous_epoch=$(date -d "$previous_timestamp" +%s)
    current_epoch=$(date -d "$current_timestamp" +%s)
    time_diff_minutes=$(echo "scale=2; ($current_epoch - $previous_epoch) / 60" | bc)

    # Calculate slots per minute
    if (( $(echo "$time_diff_minutes > 0" | bc -l) )); then
        slots_per_minute=$(echo "scale=2; $difference / $time_diff_minutes" | bc)
    else
        slots_per_minute=0
    fi

    # Estimate epoch completion time
    TOTAL_SLOTS_PER_EPOCH=432000
    if [[ $current_slots -eq 0 ]]; then
        # Epoch is complete
        estimated_completion="$current_timestamp"
        estimated_completion_duration="00:00:00"
    elif (( $(echo "$slots_per_minute > 0" | bc -l) )); then
        # Calculate minutes to complete remaining slots
        minutes_to_complete=$(echo "scale=2; $current_slots / $slots_per_minute" | bc)
        # Convert to seconds for timestamp
        seconds_to_complete=$(echo "($minutes_to_complete * 60) / 1" | bc)
        # Debug: Log intermediate values
        log_message "DEBUG" "minutes_to_complete=$minutes_to_complete, seconds_to_complete=$seconds_to_complete"
        # Calculate future timestamp
        future_epoch=$((current_epoch + seconds_to_complete))
        estimated_completion=$(date -d "@$future_epoch" '+%Y-%m-%d %H:%M:%S' 2>/dev/null || echo "null")
        # Convert to dd:hh:mm
        days=$(echo "$minutes_to_complete / (24 * 60)" | bc)
        hours=$(echo "($minutes_to_complete % (24 * 60)) / 60" | bc)
        minutes=$(echo "$minutes_to_complete % 60" | bc)
        # Format with leading zeros
        estimated_completion_duration=$(printf "%02d:%02d:%02d" $days $hours $minutes)
    else
        # Cannot estimate without a processing rate
        estimated_completion="null"
        estimated_completion_duration="null"
    fi

    # Show estimated completion duration format and value
    log_message "INFO" "Estimated completion duration (dd:hh:mm): $estimated_completion_duration"

    # Create JSON output
    json_output=$(cat <<EOF
{
  "epoch_number": $epoch_number,
  "previous_slots": $previous_slots,
  "current_slots": $current_slots,
  "slots_processed": $difference,
  "slots_per_minute": $slots_per_minute,
  "previous_timestamp": "$previous_timestamp",
  "current_timestamp": "$current_timestamp",
  "estimated_completion": "$estimated_completion",
  "estimated_completion_duration": "$estimated_completion_duration"
}
EOF
)

    # Write JSON to epoch-specific file
    json_file="999_monitor_get_slots_epoch${epoch_number}.json"
    echo "$json_output" > "$json_file"
    log_message "INFO" "Wrote monitoring data to $json_file"

    # Call copy-pages-to-web.sh to publish the JSON file
    execute_and_check "bash $ORIGINAL_DIR/copy-pages-to-web.sh $json_file"

    # Sleep for 5 minutes (300 seconds) before the next iteration
    log_message "INFO" "Sleeping for 5 minutes..."
    sleep 300
done

# Note: The script runs indefinitely, so it won't reach this point
cd "$ORIGINAL_DIR" || {
    log_message "ERROR" "Failed to return to the original directory $ORIGINAL_DIR"
    exit 1
}

log_message "INFO" "Script completed successfully."