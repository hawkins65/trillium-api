#!/bin/bash

# Source path initialization
source "$(dirname "$0")/000_init_paths.sh" || {
    echo "❌ Failed to source path initialization script" >&2
    exit 1
}

# Source common logging
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/999_common_log.sh"

# Initialize logging
init_logging

# Function to check if a file exists
file_exists() {
    if [ ! -f "$1" ]; then
        log_error "❌ File '$1' does not exist"
        exit 1
    fi
}

# Check if a filename was provided as an argument
if [ $# -eq 0 ]; then
    # If no argument was provided, prompt the user for a filename
    read -p "Please enter the filename to copy: " filename
else
    # If an argument was provided, use it as the filename
    filename="$1"
fi

# Verify that the file exists
file_exists "$filename"

# Get the current date and time
current_datetime=$(date "+%Y-%m-%d-%H-%M")

# Create the new filename with date and time
new_filename="${filename%.*}-${current_datetime}.${filename##*.}"

# Copy the file to the current directory with the new filename
cp "$filename" "$new_filename"

# Copy the file to the web directory
sudo cp "$filename" "/var/www/html/json/"

# Set the ownership of the copied file
sudo chown www-data:www-data "/var/www/html/json/$filename"

# Set the permissions of the copied file
sudo chmod 755 "/var/www/html/json/$filename"

log_info "📄 Timestamped copy created: $new_filename"
log_info "✅ File '$filename' copied to /var/www/html/json/"
log_info "🌐 Access at: https://trillium.so/json/$filename"

# Cleanup logging
cleanup_logging
