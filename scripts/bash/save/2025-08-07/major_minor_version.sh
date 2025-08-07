#!/bin/bash

# Source path initialization
source "$(dirname "$0")/000_init_paths.sh" || {
    echo "❌ Failed to source path initialization script" >&2
    exit 1
}

# Source common logging
source $TRILLIUM_SCRIPTS_BASH/999_common_log.sh

# Initialize logging
init_logging

log_info "🔍 Running major minor version check"

# Execute the Python script
cd /home/smilax/trillium_api
if python3 scripts/python/major_minor_version.py; then
    log_info "✅ Major minor version check completed successfully"
    
    # Copy the output file to web directory
    if [ -f "major_minor_version.txt" ]; then
        log_info "📁 Copying major_minor_version.txt to web"
        if bash $TRILLIUM_SCRIPTS_BASH/copy-pages-to-web.sh major_minor_version.txt; then
            log_info "✅ Successfully copied major_minor_version.txt to web"
        else
            log_error "❌ Failed to copy major_minor_version.txt to web"
            cleanup_logging
            exit 1
        fi
    else
        log_error "❌ Output file major_minor_version.txt not found"
        cleanup_logging
        exit 1
    fi
else
    log_error "❌ Major minor version check failed"
    cleanup_logging
    exit 1
fi

# Cleanup logging
cleanup_logging