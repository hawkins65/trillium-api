#!/bin/bash

source 999_common_log.sh
# Initialize enhanced logging
init_logging

# Create a temporary file for responses
response_file=$(mktemp)

# Ensure the temporary file is deleted after execution
trap 'rm -f "$response_file"' EXIT

# Check if an epoch number is provided as the parameter
if [ -n "$1" ]; then
    epoch_number="$1"
else
    read -p "Enter the epoch number: " epoch_number
fi

# 2024-11-25 run skip analysis for epoch
log_message "INFO" ""
log_message "INFO" "➡️ Running 93_skip_analysis.py"
python3 93_skip_analysis.py $epoch_number
log_message "INFO" "✅ Completed 93_skip_analysis.py"

#jrh In Feb 2024 I realized this no longer makes sense -- stake weighted numbers show all about even
#log_message "INFO" "➡️ Running 93_skip_blame.py"
#python3 93_skip_blame.py $epoch_number
#log_message "INFO" "✅ Completed 93_skip_blame.py"

log_message "INFO" ""
log_message "INFO" "➡️ Running 93_vote_latency_json.py"
python3 93_vote_latency_json.py $epoch_number
log_message "INFO" "✅ Completed 93_vote_latency_json.py"

log_message "INFO" ""
log_message "INFO" "➡️ Running 93_vote_latency.sh"
bash 93_vote_latency.sh $epoch_number
log_message "INFO" "✅ Completed 93_vote_latency.py"

log_message "INFO" ""
log_message "INFO" "➡️ Running 93_build_leaderboard_json-jito-by_count.sh"
bash 93_build_leaderboard_json-jito-by_count.sh $epoch_number
log_message "INFO" "✅ Completed 93_build_leaderboard_json-jito-by_count.sh"

log_message "INFO" ""
log_message "INFO" "➡️ Running python3 93_build_leaderboard_json.py"
python3 93_build_leaderboard_json.py $epoch_number $epoch_number
log_message "INFO" "✅ Completed 93_build_leaderboard_json.py"
