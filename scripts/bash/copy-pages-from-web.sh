#!/bin/bash

source $HOME/api/999_common_log.sh

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
file_exists "/var/www/html/pages/$filename"

# Copy the file to the web directory
sudo cp "/var/www/html/pages/$filename" "."

# Set the ownership of the copied file
sudo chown smilax:smilax "./$filename"

log_message "INFO" "File '/var/www/html/pages$filename' has been successfully copied from /var/www/html/pages/"

