#!/bin/bash

# Source path initialization
source "$(dirname "$0")/000_init_paths.sh" || {
    echo "❌ Failed to source path initialization script" >&2
    exit 1
}

# Source the common logging functions
source $TRILLIUM_SCRIPTS_BASH/999_common_log.sh
# Initialize enhanced logging
init_logging

# Get the basename of the script for Discord notifications
script_name=$(basename "$0")

log "INFO" "🚀 Starting epoch tarball extraction process"

# Check if an epoch number is provided as a parameter
if [ -n "$1" ]; then
    epoch_number="$1"
    log "INFO" "📊 Using epoch number from parameter: $epoch_number"
else
    read -p "Enter the epoch number: " epoch_number
    log "INFO" "📊 Using epoch number from user input: $epoch_number"
fi

# Construct the base filename
base_filename="epoch${epoch_number}"
log "INFO" "📁 Looking for tarball with base filename: $base_filename"

# Search for matching tarballs (allows for potential variations)
log "INFO" "🔍 Searching for matching tarballs..."
tarball_files=$(ls "${base_filename}"*.tar.zst 2>/dev/null)

log "INFO" "🎯 Expected tarball name: ${base_filename}.tar.zst"

# Check if any matching tarballs were found
if [[ -z "$tarball_files" ]]; then
    log "ERROR" "❌ No tarball matching ${base_filename}.tar.zst found"
    echo "Error: No tarball matching ${base_filename}.tar.zst found"
    exit 1
fi

log "INFO" "✅ Found matching tarball(s): $tarball_files"

# Handle potential multiple matches
if [[ $(echo "$tarball_files" | wc -w) -gt 1 ]]; then
    log "INFO" "⚠️ Multiple matching tarballs found. User selection required."
    echo "Multiple matching tarballs found. Please specify the full filename."
    select tarball_file in $tarball_files; do
        if [[ -n $tarball_file ]]; then
            log "INFO" "👤 User selected tarball: $tarball_file"
            break  # Valid selection made
        fi
        echo "Invalid selection."
    done
    # Set the tarball_file variable
    tarball_file=$tarball_file
elif [[ $(echo "$tarball_files" | wc -w) -eq 1 ]]; then
    # Set the tarball_file variable when there's only one matching tarball
    tarball_file=${tarball_files}
    log "INFO" "📦 Using single matching tarball: $tarball_file"
fi

# Extract the destination directory name (same as the base filename)
base_dir="$base_filename"

log "INFO" "📂 Extracting to directory: $base_dir"
log "INFO" "🔄 Starting extraction process..."

# Extract the tarball into the newly created directory
if zstd -d "$tarball_file" -c | tar -xvf - -C .; then
    log "INFO" "✅ Successfully extracted $tarball_file"
    log "INFO" "📁 Files extracted to current directory"
else
    exit_code=$?
    log "ERROR" "❌ Failed to extract $tarball_file (exit code: $exit_code)"
    
    # Send error notification using centralized script
    bash "$DISCORD_NOTIFY_SCRIPT" error "$script_name" "Tarball extraction failed" "zstd -d \"$tarball_file\" -c | tar -xvf - -C ." "$exit_code" "$epoch_number"
    
    exit $exit_code
fi

log "INFO" "🎉 Epoch tarball extraction completed successfully for epoch $epoch_number"
