import os
import csv
import json
import requests
import logging
import sys
import argparse
from datetime import datetime
import glob

# Add the directory containing rpc_config.py to sys.path
sys.path.append("/home/smilax/api")
from rpc_config import RPC_ENDPOINT  # Import the centralized RPC endpoint

# Alchemy RPC from Kiln
RPC_ENDPOINT_1 = RPC_ENDPOINT
headers = {'Content-Type': 'application/json'}

# Setup logging similar to the original script
def setup_logging():
    logger = logging.getLogger()
    logger.setLevel(logging.DEBUG)
    now = datetime.now()
    formatted_time = now.strftime('%Y-%m-%d_%H-%M')
    script_name = os.path.basename(__file__).replace('.py', '')
    log_dir = '/home/smilax/log'
    os.makedirs(log_dir, exist_ok=True)
    filename = f'{log_dir}/{script_name}_log_{formatted_time}.log'
    fh = logging.FileHandler(filename)
    fh.setLevel(logging.DEBUG)
    fh_formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    fh.setFormatter(fh_formatter)
    ch = logging.StreamHandler(sys.stdout)
    ch.setLevel(logging.INFO)
    ch_formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    ch.setFormatter(ch_formatter)
    logger.addHandler(fh)
    logger.addHandler(ch)
    return logger

logger = setup_logging()

def get_epoch_info(epoch_number=None):
    payload = {
        "jsonrpc": "2.0",
        "id": 1,
        "method": "getEpochInfo",
        "params": [None]
    }
    response = requests.post(RPC_ENDPOINT_1, headers=headers, json=payload)
    epoch_info = response.json()["result"]

    if epoch_number is None:
        epoch_number = epoch_info["epoch"]

    if epoch_number < epoch_info["epoch"]:
        first_slot_of_epoch = epoch_info["absoluteSlot"] - epoch_info["slotIndex"] - (epoch_info["epoch"] - epoch_number) * epoch_info["slotsInEpoch"]
        last_slot_of_epoch = first_slot_of_epoch + epoch_info["slotsInEpoch"] - 1
    else:
        first_slot_of_epoch = epoch_info["absoluteSlot"] - epoch_info["slotIndex"]
        last_slot_of_epoch = first_slot_of_epoch + epoch_info["slotsInEpoch"] - 1

    logger.debug(f"Epoch info retrieved: {epoch_info}")
    return {
        "epoch_number": epoch_number,
        "start_slot": first_slot_of_epoch,
        "end_slot": last_slot_of_epoch,
        "current_slot": epoch_info["absoluteSlot"],
        "slotIndex": epoch_info["slotIndex"],
        "slotsInEpoch": epoch_info["slotsInEpoch"]
    }

def find_missing_slots(epoch_start_slot, epoch_end_slot):
    processed_slots = set()

    logger.info("Iterating over all run directories and current directory...")
    # Check run* directories
    run_dirs = glob.glob('run*')
    for run_dir in run_dirs:
        csv_files = glob.glob(os.path.join(run_dir, "slot_data_thread_*.csv"))
        for file in csv_files:
            with open(file, "r") as f:
                csv_reader = csv.DictReader(f)
                for row in csv_reader:
                    if 'block_slot' in row:
                        try:
                            slot = int(row['block_slot'])
                            processed_slots.add(slot)
                        except ValueError as e:
                            logger.error(f"Failed to parse slot from CSV {file}, row: {row['block_slot']} - Error: {e}")
                            continue

        log_files = glob.glob(os.path.join(run_dir, "solana*rpc*errors.log"))
        for file in log_files:
            with open(file, "r") as f:
                for line in f:
                    if "-32007" in line or "-32009" in line:
                        first_column = line.split(',')[0]
                        if first_column.isdigit():
                            slot = int(first_column)
                            processed_slots.add(slot)
                        else:
                            logger.warning(f"Skipping non-numeric slot value in {file}: '{line.strip()}'")
                            continue

    # Check CSV files in the current directory
    current_dir_csv_files = glob.glob("slot_data_thread_*.csv")
    for file in current_dir_csv_files:
        with open(file, "r") as f:
            csv_reader = csv.DictReader(f)
            for row in csv_reader:
                if 'block_slot' in row:
                    try:
                        slot = int(row['block_slot'])
                        processed_slots.add(slot)
                    except ValueError as e:
                        logger.error(f"Failed to parse slot from CSV {file}, row: {row['block_slot']} - Error: {e}")
                        continue

    # Check error log files in the current directory
    current_dir_log_files = glob.glob("solana*rpc*errors.log")
    for file in current_dir_log_files:
        with open(file, "r") as f:
            for line in f:
                if "-32007" in line or "-32009" in line:
                    first_column = line.split(',')[0]
                    if first_column.isdigit():
                        slot = int(first_column)
                        processed_slots.add(slot)
                    else:
                        logger.warning(f"Skipping non-numeric slot value in {file}: '{line.strip()}'")
                        continue

    logger.info("Generating range of slots for the epoch...")
    epoch_slots = set(range(epoch_start_slot, epoch_end_slot + 1))

    logger.info("Finding missing slots...")
    missing_slots = list(epoch_slots - processed_slots)
    missing_slots.sort()

    logger.info(f"Number of missing slots: {len(missing_slots)}")
    if missing_slots:
        logger.info(f"Range of missing slots: {missing_slots[0]} to {missing_slots[-1]}")
    else:
        logger.info("No missing slots found.")

    return missing_slots

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('epoch_number', type=int, help='Epoch number to fetch')
    args = parser.parse_args()

    epoch_number = args.epoch_number
    logger.info(f"Fetching slots to process for epoch {epoch_number}")

    # Get epoch information
    epoch_info = get_epoch_info(epoch_number)
    logger.info(f"Epoch Info: {epoch_info}")

    start_slot = epoch_info["start_slot"]
    end_slot = epoch_info["end_slot"]

    # Adjust end_slot for the current epoch
    current_epoch_info = get_epoch_info()
    current_epoch = current_epoch_info["epoch_number"]
    if epoch_number == current_epoch:
        if epoch_info["slotIndex"] / epoch_info["slotsInEpoch"] < 0.9:
            end_slot = start_slot + epoch_info["slotIndex"]
        else:
            end_slot = epoch_info["end_slot"]

    # Find slots to process
    slots_to_process = find_missing_slots(start_slot, end_slot)
    num_slots_to_process = len(slots_to_process)

    logger.info(f"Epoch {epoch_number} Number of slots to process: {num_slots_to_process}")
    logger.info(f"Epoch {epoch_number} Range of slots: {start_slot} to {end_slot}")

    # Write the number of slots to an epoch-specific temporary file
    with open(f"/tmp/slots_to_process_epoch{epoch_number}.txt", "w") as f:
        f.write(str(num_slots_to_process))

    # Exit with 0 to indicate successful execution
    sys.exit(0)

if __name__ == "__main__":
    main()