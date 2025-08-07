#!/bin/bash

# Source path initialization
source "$(dirname "$0")/000_init_paths.sh" || {
    echo "❌ Failed to source path initialization script" >&2
    exit 1
}

# Source the common logging functions
source $TRILLIUM_SCRIPTS_BASH/999_common_log.sh
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
    if eval "$command"; then
        return 0
    else
        handle_error "$command"
        return 1
    fi
}

log "INFO" "🖼️ Copying and setting ownership of images"

# Copy and set ownership of images (check if static/images directory exists first)
if [ -d "/home/smilax/trillium_api/static/images" ] && [ "$(ls -A /home/smilax/trillium_api/static/images 2>/dev/null)" ]; then
    run_command "sudo cp -v /home/smilax/trillium_api/static/images/* /var/www/html/images/"
    run_command "sudo chown -R www-data:www-data /var/www/html/images/"
else
    log "INFO" "ℹ️ No static images directory found or directory is empty, skipping image copy"
fi

log "INFO" "📊 Cleaning up CSV files"

# Define the leaderboard CSV directory
LEADERBOARD_CSV_DIR="${TRILLIUM_DATA}/leaderboard/csv"

# Remove CSV files from leaderboard output directory
if ls ${LEADERBOARD_CSV_DIR}/epoch*.csv 1> /dev/null 2>&1; then
    log "INFO" "📈 Found epoch*.csv files in leaderboard output directory to remove"
    run_command "rm -v ${LEADERBOARD_CSV_DIR}/epoch*.csv"
else
    log "INFO" "ℹ️ No epoch*.csv files found in ${LEADERBOARD_CSV_DIR}"
fi

# Also remove any CSV files in current directory
if ls epoch*.csv 1> /dev/null 2>&1; then
    log "INFO" "📈 Found epoch*.csv files in current directory to remove"
    run_command "rm -v epoch*.csv"
else
    log "INFO" "ℹ️ No epoch*.csv files found in current directory"
fi

# Process and remove images only if successfully copied
process_and_remove() {
    local file="$1"
    log "INFO" "🔄 Processing image file: $file"
    # Use absolute path to the script
    run_command "bash ${TRILLIUM_SCRIPTS_BASH}/copy-images-to-web.sh \"$file\""
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
    # Use absolute path to the script
    run_command "bash ${TRILLIUM_SCRIPTS_BASH}/copy-pages-to-web.sh \"$file\""
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
    local source_dir="$3"
    log "INFO" "🖼️ Processing $description images with pattern: $pattern from $source_dir"
    
    local found_files=false
    for file in "$source_dir"/$pattern; do
        if [ -e "$file" ]; then
            found_files=true
            process_and_remove "$file"
        fi
    done
    
    if [ "$found_files" = false ]; then
        log "INFO" "ℹ️ No files found matching pattern: $pattern in $source_dir"
    fi
}

process_images "*stake*.png" "stake-related" "${TRILLIUM_DATA_IMAGES}"
process_images "epoch*slot_duration_*.png" "slot duration" "${TRILLIUM_DATA_IMAGES}"
process_images "combined_*_chart.png" "combined chart" "${TRILLIUM_DATA_IMAGES}"

log "INFO" "📊 Processing epoch metrics charts"

# Define the leaderboard HTML directory
LEADERBOARD_HTML_DIR="${TRILLIUM_DATA}/leaderboard/html"

# Update epoch metrics charts
chart_files=(
    "epoch[0-9]*_*.html"
    "epoch_metrics_chart.html"
    "epoch_comparison_charts.html"
    "latency_and_consensus_charts.html"
    "votes_cast_metrics_chart.html"
    "stake_distribution_charts.html"
    "stake_distribution_charts_metro.html"
    "epoch[0-9]*_stake_distribution_charts.html"
    "epoch[0-9]*_stake_distribution_charts_metro.html"
)

# First check the leaderboard HTML output directory
for chart_pattern in "${chart_files[@]}"; do
    found_charts=false
    for chart in "${LEADERBOARD_HTML_DIR}"/$chart_pattern; do
        if [ -e "$chart" ]; then
            found_charts=true
            process_and_remove_html "$chart"
        fi
    done
    
    if [ "$found_charts" = false ]; then
        log "INFO" "ℹ️ No files found matching pattern: $chart_pattern in ${LEADERBOARD_HTML_DIR}"
    fi
done

# Also check the old location if TRILLIUM_DATA_CHARTS is defined
if [ -n "${TRILLIUM_DATA_CHARTS}" ]; then
    for chart_pattern in "${chart_files[@]}"; do
        for chart in "${TRILLIUM_DATA_CHARTS}"/$chart_pattern; do
            if [ -e "$chart" ]; then
                log "INFO" "📊 Found chart in old location: $chart"
                process_and_remove_html "$chart"
            fi
        done
    done
fi

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
