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

log_message "INFO" "Processing epoch $epoch_number with 91_load_consolidated_csv.py"
echo $epoch_number | python3 91_load_consolidated_csv.py
