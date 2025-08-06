#!/bin/bash

source 999_common_log.sh
# Initialize enhanced logging
init_logging

# Prompt for epoch if not provided
if [ -z "$1" ]; then
    read -p "Enter epoch number: " epoch
else
    epoch=$1
fi

# Run node script and rename output
log_message "INFO" "Running node script for all validators..."
node 90_xshin.js all all
mv all_all_validators.json 90_xshin_all_validators_${epoch}.json
log_message "INFO" "Running node script for award winners..."
node 90_xshin.js award
mv all_award_winners.json 90_xshin_all_award_winners_${epoch}.json
log_message "INFO" "Running python script to load xshin data..."
python3 90_xshin_load_data.py ${epoch}
log_message "INFO" "Completed 90_xshin_load_data.sh for epoch $epoch"
