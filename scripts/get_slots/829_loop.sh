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

# Loop forever
while true; do
  # Run the script with parameter 829
  log_info "Starting execution of rpc_get_block_data.sh for epoch 829"
  ./rpc_get_block_data.sh 829
  
  # Wait for 20 minutes (1200 seconds)
  log_info "Waiting 20 minutes before next execution of ./rpc_get_block_data.sh 829..."
  sleep 1200
done