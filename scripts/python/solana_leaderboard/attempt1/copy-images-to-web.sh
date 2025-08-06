#!/bin/bash

source common_log.sh

# Function to check if a file exists
file_exists() {
    if [ ! -f "$1" ]; then
        log_message "ERROR" "File '$1' does not exist."
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

# Copy the file to the web directory
sudo cp "$filename" "/var/www/html/images/"

# Set the ownership of the copied file
sudo chown www-data:www-data "/var/www/html/images/$filename"

# Set the permissions of the copied file
sudo chmod 644 "/var/www/html/images/$filename"

log_message "INFO" "File '$filename' has been successfully copied to /var/www/html/images/"
log_message "INFO" "You can access it as: https://trillium.so/images/$filename"

bash cloudflare-purge-cache.sh https://trillium.so/images/$filename
EXIT_CODE=$?

if [ $EXIT_CODE -eq 0 ]; then
    log_message "INFO" "Cloudflare cache purged successfully"
else
    log_message "ERROR" "Cloudflare cache purge failed with exit code $EXIT_CODE"
fi

exit 0