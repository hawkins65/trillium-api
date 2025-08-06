#!/bin/bash

# Source common logging
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/../bash/999_common_log.sh"

# Initialize logging
init_logging

# Find the highest run directory
max_run=0
for dir in run*; do
    if [[ -d $dir ]]; then
        run_num=$(echo $dir | sed 's/run//')
        if ((run_num > max_run)); then
            max_run=$run_num
        fi
    fi
done

# Create the next run directory
next_run=$((max_run + 1))
next_run_dir="run$next_run"
mkdir "$next_run_dir"
log_info "📁 Created directory: $next_run_dir"

# Move the specified files to the new directory
mv *.csv solana_rpc_errors.log last_slots_to_process.txt "$next_run_dir"
log_info "📦 Moved files to $next_run_dir"

# Cleanup logging
cleanup_logging

