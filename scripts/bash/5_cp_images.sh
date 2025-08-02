#!/bin/bash

# Source the common logging functions
source /home/smilax/api/999_common_log.sh
# Initialize enhanced logging
init_logging

# Get the basename of the script for Discord notifications
script_name=$(basename "$0")

VERBOSE=1

log "INFO" "🚀 Starting image copying and processing operations"

# Function to handle errors
handle_error() {
    local command="$1"
    log "ERROR" "❌ Command failed: $command"
    # No exit here; just log the error
}

# Function to run a command with logging and error handling
run_command() {
    local command="$1"
    if [ "$VERBOSE" -eq 1 ]; then
        log "INFO" "⚡ Running: $command"
    fi
    eval "$command" || handle_error "$command"
}

log "INFO" "🖼️ Copying and setting ownership of images"

# Copy and set ownership of images
run_command "sudo cp -v /home/smilax/block-production/api/static/images/* /var/www/html/images/"
run_command "sudo chown -R www-data:www-data /var/www/html/images/"

log "INFO" "📊 Cleaning up geo-related CSV files"

# Clean up current run of geo-related CSV files
if ls epoch*.csv 1> /dev/null 2>&1; then
    log "INFO" "📈 Found epoch*.csv files to move to geolite2 directory"
    run_command "mv -v epoch*.csv ./geolite2"
else
    log "INFO" "ℹ️ No epoch*.csv files found to move"
fi

# Process and remove images only if successfully copied
process_and_remove() {
    local file="$1"
    log "INFO" "🔄 Processing image file: $file"
    run_command "bash copy-images-to-web.sh \"$file\""
    if [ $? -eq 0 ]; then
        log "INFO" "✅ Successfully processed $file. Removing it."
        run_command "rm -f \"$file\""
    else
        log "ERROR" "❌ Failed to process $file. Not removing it."
    fi
}

# Process and remove html only if successfully copied
process_and_remove_html() {
    local file="$1"
    log "INFO" "🌐 Processing HTML file: $file"
    run_command "bash copy-pages-to-web.sh \"$file\""
    if [ $? -eq 0 ]; then
        log "INFO" "✅ Successfully processed $file. Removing it."
        run_command "rm -f \"$file\""
    else
        log "ERROR" "❌ Failed to process $file. Not removing it."
    fi
}

# Update image files in various categories
process_images() {
    local pattern="$1"
    local description="$2"
    log "INFO" "🖼️ Processing $description images with pattern: $pattern"
    
    local found_files=false
    for file in $pattern; do
        if [ -e "$file" ]; then
            found_files=true
            process_and_remove "$file"
        fi
    done
    
    if [ "$found_files" = false ]; then
        log "INFO" "ℹ️ No files found matching pattern: $pattern"
    fi
}

process_images "*stake*.png" "stake-related"
process_images "epoch*slot_duration_*.png" "slot duration"
process_images "combined_*_chart.png" "combined chart"

log "INFO" "📊 Processing epoch metrics charts"

# Update epoch metrics charts
chart_files=(
    "epoch[0-9]*_*.html"
    "epoch_metrics_chart.html"
    "epoch_comparison_charts.html"
    "latency_and_consensus_charts.html"
    "votes_cast_metrics_chart.html"
    "./html/stake_distribution_charts.html"
    "./html/stake_distribution_charts_metro.html"
    "./html/epoch[0-9]*_stake_distribution_charts.html"
    "./html/epoch[0-9]*_stake_distribution_charts_metro.html"
)

for chart_pattern in "${chart_files[@]}"; do
    found_charts=false
    for chart in $chart_pattern; do
        if [ -e "$chart" ]; then
            found_charts=true
            process_and_remove_html "$chart"
        fi
    done
    
    if [ "$found_charts" = false ]; then
        log "INFO" "ℹ️ No files found matching pattern: $chart_pattern"
    fi
done

log "INFO" "🧹 Cleaning up slot duration intermediate data"

# Clean up slot duration intermediate data
if ls visualizations_* 1> /dev/null 2>&1; then
    log "INFO" "📊 Removing visualization data files"
    run_command "rm -r visualizations_*"
else
    log "INFO" "ℹ️ No visualizations data to remove"
fi

if ls avg_slot_duration_epochs_*.csv 1> /dev/null 2>&1; then
    log "INFO" "📈 Removing slot duration CSV files"
    run_command "rm -r avg_slot_duration_epochs_*.csv"
else
    log "INFO" "ℹ️ No slot duration CSV files to remove"
fi

log "INFO" "🔐 Setting final ownership and permissions for images"

# Ensure correct ownership and permissions for all files in the images folder
log "INFO" "👤 Setting ownership and permissions for all files in /var/www/html/images/"
run_command "sudo chown -R www-data:www-data /var/www/html/images/*"
run_command "sudo chmod -R 644 /var/www/html/images/*"
log "INFO" "✅ Ownership and permissions updated for all files in /var/www/html/images/"

log "INFO" "🎉 Image copying and processing completed successfully"
