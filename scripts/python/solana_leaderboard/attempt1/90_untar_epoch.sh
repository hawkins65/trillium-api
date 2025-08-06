#!/bin/bash

source 999_common_log.sh
# Initialize enhanced logging
init_logging

# Check if an epoch number is provided as a parameter
if [ -n "$1" ]; then
    epoch_number="$1"
else
    read -p "Enter the epoch number: " epoch_number
fi

# Construct the base filename
base_filename="epoch${epoch_number}"

# Search for matching tarballs (allows for potential variations)
tarball_files=$(ls "${base_filename}"*.tar.zst)
log_message "INFO" "Tarball name ${base_filename}.tar.zst"
# Check if any matching tarballs were found
if [[ -z "$tarball_files" ]]; then
    log_message "ERROR" "No tarball matching ${base_filename}.tar.zst found"
    exit 1
fi

# Handle potential multiple matches
if [[ $(echo "$tarball_files" | wc -w) -gt 1 ]]; then
    log_message "INFO" "Multiple matching tarballs found. Please specify the full filename."
    select tarball_file in $tarball_files; do
        if [[ -n $tarball_file ]]; then
            break  # Valid selection made
        fi
        log_message "ERROR" "Invalid selection."
    done
    # Set the tarball_file variable
    tarball_file=$tarball_file
elif [[ $(echo "$tarball_files" | wc -w) -eq 1 ]]; then
    # Set the tarball_file variable when there's only one matching tarball
    tarball_file=${tarball_files}
fi

# Extract the destination directory name (same as the base filename)
base_dir="$base_filename"

# Extract the tarball into the newly created directory
log_message "INFO" "Creating compressed archive with progress..."
zstd -d "$tarball_file" -c | tar -xvf - -C .
log_message "INFO" "Archive created: ${base_filename}.tar.zst"
