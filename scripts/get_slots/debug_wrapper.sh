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

log_debug "Starting debug wrapper"
log_debug "Current shell: $SHELL"
log_debug "Bash version: $BASH_VERSION"
log_debug "Environment variables containing 'bash':"
env | grep -i bash | while read line; do
    log_debug "  $line"
done

# Run the actual script with strace to catch any exec calls
log_info "Running with strace to catch exec calls..."
strace -e execve -f -o /tmp/rpc_strace.log bash "${TRILLIUM_SCRIPTS_GETSLOTS}/rpc_get_block_data.sh" "$@"

log_info "Script completed. Checking strace log for 'environment' calls..."
if grep -i environment /tmp/rpc_strace.log; then
    log_info "Found 'environment' calls in strace log"
else
    log_info "No 'environment' calls found in strace"
fi