#!/bin/bash

echo "Testing environment error reproduction"
echo "Number of arguments: $#"
echo "Arguments: $@"

# Try to simulate the error
if [[ $# -eq 1 ]]; then
    echo "Correct number of arguments"
    # Call the rpc_get_block_data.sh script
    echo "Calling rpc_get_block_data.sh..."
    bash /home/smilax/trillium_api/scripts/get_slots/rpc_get_block_data.sh "$1"
else
    echo "Wrong number of arguments"
fi