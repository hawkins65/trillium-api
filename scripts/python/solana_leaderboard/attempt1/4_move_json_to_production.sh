#!/bin/bash

source 999_common_log.sh
# Initialize enhanced logging
init_logging

VERBOSE=1
LOGFILE="$HOME/log/$(basename "$0" .sh).log"

# Function to handle errors
handle_error() {
    local command="$1"
    log_message "ERROR" "Command failed: $command"
    exit 1
}

# Function to run a command with logging and error handling
run_command() {
    local command="$1"
    if [ "$VERBOSE" -eq 1 ]; then
        log_message "INFO" "Running: $command"
    fi
    eval "$command" || handle_error "$command"
}

log_message "INFO" "Script started."

# Move website JSON files
if ls epoch*.json 1> /dev/null 2>&1; then
    run_command "mv -v epoch*.json /home/smilax/block-production/leaderboard/production/validator_rewards/static/json"
else
    log_message "INFO" "No 'epoch*.json' files found to move."
fi

if [ -e "last_ten_epoch_aggregate_data.json" ]; then
    run_command "mv -v last_ten_epoch_aggregate_data.json /home/smilax/block-production/leaderboard/production/validator_rewards/static/json"
else
    log_message "INFO" "File not found: last_ten_epoch_aggregate_data.json"
fi

if ls ten_epoch_*.json 1> /dev/null 2>&1; then
    run_command "mv -v ten_epoch_*.json /home/smilax/block-production/leaderboard/production/validator_rewards/static/json"
else
    log_message "INFO" "No 'ten_epoch_*.json' files found to move."
fi

if [ -e "recency_weighted_average_validator_rewards.json" ]; then
    run_command "mv -v recency_weighted_average_validator_rewards.json /home/smilax/block-production/leaderboard/production/validator_rewards/static/json"
else
    log_message "INFO" "File not found: recency_weighted_average_validator_rewards.json"
fi

for file in vote_latency*.{json,txt}; do
    if [ -f "$file" ]; then
        if ./copy-pages-to-web.sh "$file"; then
            if rm "$file"; then
                log_message "INFO" "Successfully processed and removed: $file"
            else
                log_message "ERROR" "Failed to remove $file"
            fi
        else
            log_message "ERROR" "Failed to process: $file"
        fi
    fi
done

bash /home/smilax/api/cloudflare-purge-cache.sh

log_message "INFO" "Script completed successfully."
