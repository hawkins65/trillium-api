#!/bin/bash

# Default log directory
LOG_BASE_DIR="$HOME/log"

# Function to log messages to both the screen and a script-specific log file
log() {
    local level=$1
    local message=$2
    local script_name_no_ext=$(basename "$0" | sed 's/\.sh$//')
    local log_file="$LOG_BASE_DIR/${script_name_no_ext}.log"

    # Ensure the log directory exists
    mkdir -p "$LOG_BASE_DIR"

    printf "[%s] %s - %s\n" "$(date +'%Y-%m-%d %H:%M:%S')" "$level" "$message" | tee -a "$log_file"
}
