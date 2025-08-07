#!/bin/bash

# Source path initialization
source "$(dirname "$0")/000_init_paths.sh" || {
    echo "❌ Failed to source path initialization script" >&2
    exit 1
}

source $HOME/trillium_api/scripts/bash/999_common_log.sh

# Database connection details
DB_HOST="localhost"
DB_PORT="5432"
DB_USER="smilax"
DB_NAME="sol_blocks"

# List of SQL files to process (alphabetically ordered)
SQL_FILES=(
    "vote_latency_by_asn_by_city-30.sql"
)

# Check if epoch was passed as parameter, otherwise fetch latest
if [ $# -eq 1 ]; then
    EPOCH=$1
else
    # Fetch the latest epoch
    EPOCH=$(psql -h "$DB_HOST" -p "$DB_PORT" -U "$DB_USER" -d "$DB_NAME" -t -c "
        SELECT MAX(epoch)
        FROM validator_stats;
    ")
    # Remove any whitespace from the epoch number
    EPOCH=$(echo "$EPOCH" | tr -d '[:space:]')
fi

if [[ -z "$EPOCH" ]]; then
    log_message "ERROR" "Failed to retrieve or set epoch"
    exit 1
fi

log_message "INFO" "Using epoch: $EPOCH"

# re-run city to metro mapping
bash 92_map-city-to-metro.sh

# Iterate over each SQL file
for sql_file in "${SQL_FILES[@]}"; do
    # Extract the base name (without extension) for output file
    base_name=$(basename "$sql_file" .sql)

    # Generate the epoch-based output filename
    epoch_output_file="${base_name}_${EPOCH}.txt"

    # First write the filename to the output file
    log_message "INFO" "File: ${base_name}.txt, Epoch: ${EPOCH}" > "$epoch_output_file"
    log_message "INFO" "" >> "$epoch_output_file"

    # Run the query and append the output to the epoch-based file, passing EPOCH as a variable
    psql -h "$DB_HOST" -p "$DB_PORT" -U "$DB_USER" -d "$DB_NAME" -v epoch="$EPOCH" -f "$sql_file" >> "$epoch_output_file"

    # Append UTC timestamp
    log_message "INFO" "Generated at: $(date -u '+%Y-%m-%d %H:%M:%S') UTC" >> "$epoch_output_file"

    # Check if the command succeeded
    if [[ $? -eq 0 ]]; then
        log_message "INFO" "Output for $sql_file saved to $epoch_output_file"
        bash copy-pages-to-web.sh $epoch_output_file
        # Create a static copy named base_name.txt
        static_output_file="${base_name}.txt"
        cp "$epoch_output_file" "$static_output_file"
        log_message "INFO" "Static copy saved to $static_output_file"
        bash copy-pages-to-web.sh $static_output_file
    else
        log_message "ERROR" "Error running $sql_file"
    fi
done