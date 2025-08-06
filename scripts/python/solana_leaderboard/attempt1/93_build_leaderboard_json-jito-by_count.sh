#!/bin/bash

source 999_common_log.sh
# Initialize enhanced logging
init_logging

# Check if epoch number is provided
if [ -z "$1" ]; then
    log_message "ERROR" "No epoch number provided."
    log_message "ERROR" "Usage: $0 <epoch_number>"
    exit 1
fi

EPOCH="$1"

# Define file paths
FILE_200="epoch${EPOCH}_validator_counts_charts-jito-200.html"
FILE_350="epoch${EPOCH}_validator_counts_charts-jito-350.html"
FILE_200_DEFAULT="validator_counts_charts-jito-200.html"
FILE_350_DEFAULT="validator_counts_charts-jito-350.html"

# Step 1: Run Python script
log_message "INFO" "Running Python script for epoch $EPOCH..."
python3 93_build_leaderboard_json-jito-by_count.py "$EPOCH"
if [ $? -ne 0 ]; then
    log_message "ERROR" "Python script failed for epoch $EPOCH."
    exit 1
fi

# Check if both files were created
if [ ! -f "$FILE_200" ] || [ ! -f "$FILE_350" ]; then
    log_message "ERROR" "One or both HTML files were not created."
    log_message "ERROR" "Expected files: $FILE_200 and $FILE_350"
    ls -l "$FILE_200" "$FILE_350" 2>/dev/null || true
    exit 1
fi

# Step 2: Copy first file to web
log_message "INFO" "Copying $FILE_200 to web..."
bash copy-pages-to-web.sh "$FILE_200"
if [ $? -ne 0 ]; then
    log_message "ERROR" "Failed to copy $FILE_200 to web."
    exit 1
fi

# Step 2.1: Copy first default file to web
log_message "INFO" "Copying $FILE_200_DEFAULT to web..."
cp "$FILE_200" "$FILE_200_DEFAULT"
bash copy-pages-to-web.sh "$FILE_200_DEFAULT"
if [ $? -ne 0 ]; then
    log_message "ERROR" "Failed to copy $FILE_200_DEFAULT to web."
    exit 1
fi

# Step 3: Copy second file to web
log_message "INFO" "Copying $FILE_350 to web..."
bash copy-pages-to-web.sh "$FILE_350"
if [ $? -ne 0 ]; then
    log_message "ERROR" "Failed to copy $FILE_350 to web."
    exit 1
fi

# Step 3.1: Copy second default file to web
log_message "INFO" "Copying $FILE_350_DEFAULT to web..."
cp "$FILE_350" "$FILE_350_DEFAULT"
bash copy-pages-to-web.sh "$FILE_350_DEFAULT"
if [ $? -ne 0 ]; then
    log_message "ERROR" "Failed to copy $FILE_350_DEFAULT to web."
    exit 1
fi

# All steps succeeded, delete local files
log_message "INFO" "All steps completed successfully. Deleting local files..."
rm -f "$FILE_200" "$FILE_350" "$FILE_200_DEFAULT" "$FILE_350_DEFAULT"
if [ $? -eq 0 ]; then
    log_message "INFO" "Local files $FILE_200 and $FILE_350 and $FILE_200_DEFAULT and $FILE_350_DEFAULT deleted."
else
    log_message "WARNING" "Failed to delete one or all local files."
    ls -l "$FILE_200" "$FILE_350" "$FILE_200_DEFAULT" "$FILE_350_DEFAULT" 2>/dev/null || true
fi

log_message "INFO" "Deployment complete for epoch $EPOCH!"
log_message "INFO" "Files are accessible at:"
log_message "INFO" "https://trillium.so/pages/$FILE_200"
log_message "INFO" "https://trillium.so/pages/$FILE_350"
log_message "INFO" "https://trillium.so/pages/$FILE_200_DEFAULT"
log_message "INFO" "https://trillium.so/pages/$FILE_350_DEFAULT"

exit 0