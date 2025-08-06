#!/bin/bash

# Source path initialization
source "$(dirname "$0")/000_init_paths.sh" || {
    echo "❌ Failed to source path initialization script" >&2
    exit 1
}
# Test script for unified bash logging

# Source the common logging
source ./999_common_log.sh

# Initialize logging
init_logging

# Test different log levels
log_info "🚀 Testing Bash unified logging configuration"
log_debug "This is a debug message"
log_warn "This is a warning message"
log_error "This is an error message"

# Test execution timing
start_time=$(date +%s.%N)
sleep 0.1  # Simulate work
end_time=$(date +%s.%N)

if command -v bc >/dev/null 2>&1; then
    duration=$(echo "$end_time - $start_time" | bc)
    log_info "⏱️ Test operation completed in ${duration}s"
else
    duration=$((${end_time%.*} - ${start_time%.*}))
    log_info "⏱️ Test operation completed in ~${duration}s"
fi

log_info "🏁 Bash logging test completed"

# Cleanup
cleanup_logging