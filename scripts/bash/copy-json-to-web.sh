#!/bin/bash

# Function to check if a file exists
file_exists() {
    if [ ! -f "$1" ]; then
        echo "Error: File '$1' does not exist."
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

echo "A copy with timestamp has been created in the current directory"
echo "File '$filename' has been successfully copied to /var/www/html/json/"
echo "You can access it as: https://trillium.so/json/$filename"
