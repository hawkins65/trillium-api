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
    # No exit here; just log the error
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

# Copy and set ownership of images
run_command "sudo cp -v /home/smilax/block-production/api/static/images/* /var/www/html/images/"
run_command "sudo chown -R www-data:www-data /var/www/html/images/"

# Clean up current run of geo-related CSV files
if ls epoch*.csv 1> /dev/null 2>&1; then
    run_command "mv -v epoch*.csv ./geolite2"
else
    log_message "INFO" "No epoch*.csv files found to move."
fi

# Process and remove images only if successfully copied
process_and_remove() {
    local file="$1"
    run_command "bash copy-images-to-web.sh \"$file\""
    if [ $? -eq 0 ]; then
        log_message "INFO" "Successfully processed $file. Removing it."
        run_command "rm -f \"$file\""
    else
        log_message "WARNING" "Failed to process $file. Not removing it."
    fi
}

# Update image files in various categories
process_images() {
    local pattern="$1"
    for file in $pattern; do
        if [ -e "$file" ]; then
            process_and_remove "$file"
        else
            log_message "INFO" "File not found: $file"
        fi
    done
}

process_images "*stake*.png"
process_images "epoch*slot_duration_*.png"
process_images "combined_*_chart.png"

# Update epoch metrics charts
for chart in epoch_metrics_chart.png epoch_comparison_charts.png latency_and_consensus_charts.png votes_cast_metrics_chart.png; do
    if [ -e "$chart" ]; then
        process_and_remove "$chart"
    else
        log_message "INFO" "File not found: $chart"
    fi
done

# Clean up slot duration intermediate data
if ls visualizations_* 1> /dev/null 2>&1; then
    run_command "rm -r visualizations_*"
else
    log_message "INFO" "No visualizations data to remove."
fi

if ls avg_slot_duration_epochs_*.csv 1> /dev/null 2>&1; then
    run_command "rm -r avg_slot_duration_epochs_*.csv"
else
    log_message "INFO" "No slot duration CSV files to remove."
fi

# Ensure correct ownership and permissions for all files in the images folder
log_message "INFO" "Setting ownership and permissions for all files in /var/www/html/images/"
run_command "sudo chown -R www-data:www-data /var/www/html/images/*"
run_command "sudo chmod -R 644 /var/www/html/images/*"
log_message "INFO" "Ownership and permissions updated for all files in /var/www/html/images/"

log_message "INFO" "Script completed successfully."
