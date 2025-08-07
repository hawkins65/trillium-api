#!/bin/bash

# Check if epoch number is provided
if [[ "$#" -lt 1 ]]; then
    echo "Usage: $0 <epoch-number>"
    exit 1
fi

epoch_number="$1"
target_dir="/home/smilax/trillium_api/data/epochs/epoch$epoch_number"
script_path="/home/smilax/trillium_api/scripts/get_slots/rpc_get_block_data.sh"

# Save the current directory
ORIGINAL_DIR=$(pwd)

# Create and change to the epoch directory
mkdir -p "$target_dir"
cd "$target_dir"

# Execute block data collection
bash "$script_path" "$epoch_number"

# Process and copy tar files
cd "/home/smilax/trillium_api/data/epochs"
bash "/home/smilax/trillium_api/scripts/get_slots/tar_files.sh" "$epoch_number"
bash "/home/smilax/trillium_api/scripts/get_slots/copy_tar.sh" "$epoch_number"

# Return to the original directory
cd "$ORIGINAL_DIR"

echo "Script completed successfully."
exit 0