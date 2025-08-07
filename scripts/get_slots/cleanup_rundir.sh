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

