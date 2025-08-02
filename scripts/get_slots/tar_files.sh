#!/bin/bash

# Check if an epoch number is provided as a parameter
if [ -n "$1" ]; then
    epoch_number="$1"
else
    read -p "Enter the epoch number: " epoch_number
fi

# Construct the base filename
base_filename="epoch${epoch_number}"

# Notify user and start compression with progress indicator
echo "Creating compressed archive with progress..."
tar -cf - ./"${base_filename}"/run*/ | zstd -9 --long -T0 -o "${base_filename}.tar.zst"
echo "Archive created: ${base_filename}.tar.zst"
