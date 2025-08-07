#!/bin/bash

# Source path initialization
source "$(dirname "$0")/../bash/000_init_paths.sh" || {
    echo "❌ Failed to source path initialization script" >&2
    exit 1
}

# Source common logging (already sourced by init_paths but ensure it's available)
source "$COMMON_LOG_SCRIPT"

# Initialize logging
init_logging

# Check if epoch number is provided
if [[ "$#" -ne 1 ]]; then
    log_error "Usage: $0 <epoch-number>"
    exit 1
fi

epoch_number="$1"
target_dir="${TRILLIUM_DATA_EPOCHS}/epoch$epoch_number"

log_info "Processing epoch $epoch_number"
log_debug "Target directory: $target_dir"

# Save the current directory
ORIGINAL_DIR=$(pwd)

# Create and change to the epoch directory
mkdir -p "$target_dir"
if ! safe_cd "$target_dir" "epoch directory"; then
    log_error "Failed to change to epoch directory: $target_dir"
    exit 1
fi

# Get Shinobi vote latency and consensus voting data
log_info "Getting Shinobi vote latency and consensus voting data"
bash "${TRILLIUM_SCRIPTS_GETSLOTS}/get_shin_voting.sh"

# Get epoch data in CSV files
log_info "Getting epoch data in CSV files"
python3 "${TRILLIUM_SCRIPTS_GETSLOTS}/get_epoch_data_csv.py" "$epoch_number"
exit_status=$?

if [ $exit_status -ne 0 ]; then
    log_warn "get_epoch_data_csv.py exited with status $exit_status"
fi

# Create next run directory and move files
max_run=0
for dir in run*; do
    if [[ -d $dir ]]; then
        run_num=$(echo $dir | sed 's/run//')
        if ((run_num > max_run)); then
            max_run=$run_num
        fi
    fi
done

next_run=$((max_run + 1))
next_run_dir="run$next_run"
log_info "Creating run directory: $next_run_dir"
mkdir "$next_run_dir"
mv *.csv solana_rpc_errors.log "$next_run_dir" 2>/dev/null

# Check if re-run is needed based on remaining slots
last_slots_file="last_slots_to_process.txt"
if [ -f "$last_slots_file" ]; then
    remaining_slots=$(cat "$last_slots_file" 2>/dev/null || echo "0")
    if [ "$remaining_slots" -gt 0 ]; then
        # Re-run the script
        log_info "Re-running script - $remaining_slots slots remaining"
        exec "${TRILLIUM_SCRIPTS_GETSLOTS}/rpc_get_block_data.sh" "$epoch_number"
    fi
fi

# Return to the original directory
cd "$ORIGINAL_DIR"

log_success "Script completed successfully for epoch $epoch_number"
exit 0
