#!/bin/bash

# Loop forever
while true; do
  # Run the script with parameter 829
  ./rpc_get_block_data.sh 829
  
  # Wait for 20 minutes (1200 seconds)
  echo "Waiting 20 minutes before next execution of ./rpc_get_block_data.sh 829..."
  sleep 1200
done