#!/bin/bash

source common_log.sh

# Save the original command-line arguments (jrh 2024-10-15 -- script does not seem to save original command line args after running the python program)
original_args="$@"
log_message "INFO" "Running with all parameters: $0 $original_args"
sleep 5

# Parse command line arguments
if [[ "$#" -ne 1 ]]; then
    log_message "ERROR" "Usage: $0 <epoch-number>"
    exit 1
fi

epoch_number="$1"

# You can now use $epoch_number as needed
log_message "INFO" "Epoch number: $epoch_number"

# Switch to the directory to save slot data
target_dir="/home/smilax/block-production/get_slots/epoch$epoch_number"
log_message "INFO" "Switching to $target_dir"

if ! cd "$target_dir"; then
    log_message "ERROR" "Failed to change directory to $target_dir"
    exit 1
fi

# get shinobi vote latency and consensus voting data
bash get_shin_voting.sh 

# get epoch data in csv files
python3 get_epoch_data_csv.py "$epoch_number"
exit_status=$?

# Check if the Python script exited cleanly or slots didn't change
if [ $exit_status -eq 99 ]; then
    log_message "INFO" "No new slots to process. Exiting script."
    exit 0
elif [ $exit_status -ne 0 ]; then
    log_message "ERROR" "The Python script did not exit cleanly."
    read -p "Do you want to quit the script or continue? (q/c): " choice
    if [[ $choice =~ ^[Qq]$ ]]; then
        log_message "INFO" "Script execution aborted."
        exit 1
    fi
fi

#python3 /home/smilax/api/92_update_vs_inflation_reward.py

# Find the highest run directory
max_run=0
for dir in run*; do
    if [[ -d $dir ]]; then
        run_num=$(echo $dir | sed 's/run//')
        if ((run_num > max_run)); then
            max_run=$run_num
        fi
    fi
done

# Create the next run directory
next_run=$((max_run + 1))
next_run_dir="run$next_run"
mkdir "$next_run_dir"
log_message "INFO" "Created directory: $next_run_dir"

# Move the specified files to the new directory
mv *.csv solana_rpc_errors.log "$next_run_dir"
log_message "INFO" "Moved files to $next_run_dir"

# Prompt the user to re-run the script

read -t 5 -p "Do you want to re-run the script? (y/n): " choice
if [ -z "$choice" ]; then
  choice="y"
  log_message "INFO" "No input received, proceeding as if you pressed 'y'"
else
  log_message "INFO" "You entered: $choice"
fi

if [[ $choice =~ ^[Yy]$ ]]; then
    log_message "INFO" "Re-running the script with all parameters: $0 $original_args"
    exec "$0" $original_args
else
    bash update_discord_channel.sh $epoch_number
    log_message "INFO" "Script execution completed."
fi
