import os
import csv
import json
import requests
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
import argparse
import math
import glob
import select
import re
import logging
import sys
from datetime import datetime

# Add the directory containing rpc_config.py to sys.path
sys.path.append("/home/smilax/api")
from rpc_config import RPC_ENDPOINT  # Import the centralized RPC endpoint
from logging_config import setup_logging

# Alchemy RPC from Kiln 
RPC_ENDPOINT_1 = RPC_ENDPOINT
RPC_ENDPOINT_2 = RPC_ENDPOINT
RPC_ENDPOINT_3 = RPC_ENDPOINT

headers = {'Content-Type': 'application/json'}
error_log_file = "solana_rpc_errors.log"

logger = setup_logging(os.path.basename(__file__).replace('.py', ''))

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

def process_slot_data(thread_id, slots, file_index, epoch_number, rpc_endpoint):
    slot_data_file = f"slot_data_thread_{thread_id}_file_{file_index}.csv"
    vote_data_file = f"epoch_votes_thread_{thread_id}_file_{file_index}.csv"

    logger.info(f"Epoch {epoch_number} Thread {thread_id} processing slots to {slot_data_file} and {vote_data_file}")

    with open(slot_data_file, 'w', newline='') as slot_file, open(vote_data_file, 'w', newline='') as vote_file:
        fieldnames = ["pubkey", "epoch", "block_slot", "block_hash", "block_time", "rewards", "post_balance", "reward_type", "commission",
                      "total_user_tx", "total_vote_tx", "total_cu", "total_signature_fees", "total_priority_fees", "total_fees",
                      "total_tx", "total_signatures", "total_validator_fees", "total_validator_signature_fees", "total_validator_priority_fees",
                      "block_height", "parent_slot", "previous_block_hash"]
        slot_writer = csv.DictWriter(slot_file, fieldnames=fieldnames)
        slot_writer.writeheader()

        vote_writer = csv.DictWriter(vote_file, fieldnames=["epoch", "block_slot", "block_hash", "identity_pubkey", "vote_account_pubkey"])
        vote_writer.writeheader()

        total_slots = len(slots)
        processed_slots = 0

        for slot in slots:
            logger.info(f"Epoch {epoch_number} Thread {thread_id} processing slot {slot} ({processed_slots + 1}/{total_slots}) - RPC: {rpc_endpoint[:44]}")

            payload_block = {
                "jsonrpc": "2.0",
                "id": thread_id,
                "method": "getBlock",
                "params": [
                    slot,
                    {
                        "encoding": "json",
                        "transactionDetails": "full",
                        "rewards": True,
                        "maxSupportedTransactionVersion": 1
                    }
                ]
            }

            retry_count = 0
            retry_delay = 1
            while retry_count < 5:
                try:
                    response_block = requests.post(rpc_endpoint, headers=headers, json=payload_block)
                    if response_block.status_code == 200:
                        response_json = response_block.json()
                        if "result" in response_json:
                            block_info = response_json["result"]
                            logger.debug(f"Epoch {epoch_number} Thread {thread_id} - Slot {slot}: Block info retrieved successfully")
                            slot_data_entry = extract_slot_data(slot, block_info, epoch_number)
                            if slot_data_entry:
                                slot_writer.writerow(slot_data_entry)
                                logger.debug(f"Epoch {epoch_number} Thread {thread_id} - Slot {slot}: Slot data written to CSV")
                            else:
                                log_error(slot, -666, response_block.json())
                                logger.debug(f"Epoch {epoch_number} Thread {thread_id} - Slot {slot}: No slot data entry found")
                                logger.debug(f"Thread {thread_id} - Slot {slot}: Response result: {response_block.json()['result']}")
                                logger.debug(f"Thread {thread_id} - Slot {slot}: Block info: {block_info}")

                            vote_data = extract_vote_data(slot, block_info, epoch_number)
                            for vote_entry in vote_data:
                                vote_writer.writerow(vote_entry)
                            logger.debug(f"Epoch {epoch_number} Thread {thread_id} - Slot {slot}: Vote data written to CSV")
                            break
                        else:
                            error_code = response_json.get("error", {}).get("code")
                            log_error(slot, error_code, response_json)
                            logger.debug(f"Epoch {epoch_number} Thread {thread_id} - Slot {slot}: Error code {error_code}")
                            if error_code in [-32009, -32007]:
                                break
                            else:
                                retry_count += 1
                                time.sleep(retry_delay)
                                retry_delay *= 2
                    else:
                        log_error(slot, response_block.status_code, response_block.text)
                        logger.debug(f"Epoch {epoch_number} Thread {thread_id} - Slot {slot}: Response status code {response_block.status_code}")
                        retry_count += 1
                        time.sleep(retry_delay)
                        retry_delay *= 2
                except Exception as e:
                    log_error(slot, -333, str(e))
                    logger.debug(f"Epoch {epoch_number} Thread {thread_id} - Slot {slot}: Exception occurred: {str(e)}")
                    logger.debug(f"Thread {thread_id} - Slot {slot}: RPC Endpoint: {rpc_endpoint[:44]}")
                    logger.debug(f"Thread {thread_id} - Slot {slot}: Response Content: {response_block.text}")
                    logger.debug(f"Thread {thread_id} - Slot {slot}: Response Status Code: {response_block.status_code}")
                    logger.debug(f"Thread {thread_id} - Slot {slot}: Response Headers: {response_block.headers}")
                    retry_count += 1
                    time.sleep(retry_delay)
                    retry_delay *= 2

            processed_slots += 1

        processed_slots += 1

    return slot_data_file, vote_data_file

def extract_slot_data(slot, block_data, epoch_number):
    if not block_data['rewards']:
        return None

    block_time = block_data['blockTime']
    block_hash = block_data['blockhash']
    block_height = block_data['blockHeight']
    parent_slot = block_data['parentSlot']
    previous_blockhash = block_data['previousBlockhash']
    transactions = block_data.get('transactions', [])

    total_fees = sum(tx['meta']['fee'] for tx in transactions)
    total_tx = len(transactions)
    total_signatures = sum(len(tx['transaction']['signatures']) for tx in transactions)
    
    total_signature_fees = total_signatures * 5000
    total_priority_fees = total_fees - total_signature_fees
    total_validator_signature_fees = total_signatures * 2500
    if epoch_number > 740:
        total_validator_priority_fees = total_priority_fees
    else:
        total_validator_priority_fees = total_priority_fees / 2

    total_validator_fees = total_validator_signature_fees + total_validator_priority_fees

    total_vote_tx = sum(1 for tx in transactions if any(
        pk == "Vote111111111111111111111111111111111111111" for pk in
        tx['transaction']['message']['accountKeys']))
    total_user_tx = total_tx - total_vote_tx
    total_cu = sum(tx['meta']['computeUnitsConsumed'] for tx in transactions)

    reward = block_data['rewards'][0]
    slot_info = {
        "pubkey": reward['pubkey'],
        "epoch": epoch_number,
        "block_slot": slot,
        "block_hash": block_hash,
        "block_time": block_time,
        "rewards": reward['lamports'],
        "post_balance": reward['postBalance'],
        "reward_type": reward['rewardType'],
        "commission": reward['commission'],
        "total_user_tx": total_user_tx,
        "total_vote_tx": total_vote_tx,
        "total_cu": total_cu,
        "total_signature_fees": total_signature_fees,
        "total_priority_fees": total_priority_fees,
        "total_fees": total_fees,
        "total_tx": total_tx,
        "total_signatures": total_signatures,
        "total_validator_fees": total_validator_fees,
        "total_validator_signature_fees": total_validator_signature_fees,
        "total_validator_priority_fees": total_validator_priority_fees,
        "block_height": block_height,
        "parent_slot": parent_slot,
        "previous_block_hash": previous_blockhash
    }

    return slot_info

def extract_vote_data(slot, block_data, epoch_number):
    vote_data = []
    transactions = block_data.get('transactions', [])
    for tx in transactions:
        account_keys = tx['transaction']['message']['accountKeys']
        if "Vote111111111111111111111111111111111111111" in account_keys:
            vote_index = account_keys.index("Vote111111111111111111111111111111111111111")
            if vote_index >= 2:
                vote_authority = account_keys[vote_index - 2]
                vote_account = account_keys[vote_index - 1]
                vote_data.append({
                    "epoch": epoch_number,
                    "block_slot": slot,
                    "block_hash": block_data['blockhash'],
                    "identity_pubkey": vote_authority,
                    "vote_account_pubkey": vote_account
                })
    return vote_data

def log_error(slot, error_code, error_details):
    with open(error_log_file, 'a') as f:
        if isinstance(error_details, dict):
            error_details = json.dumps(error_details)
        f.write(f"{slot},{error_code},{error_details}\n")

def find_missing_slots(epoch_start_slot, epoch_end_slot):
    processed_slots = set()

    logger.info("Iterating over all run directories...")
    run_dirs = glob.glob('run*')
    if not run_dirs:
        logger.info("No run directories found.")
        return list(range(epoch_start_slot, epoch_end_slot + 1))

    for run_dir in run_dirs:
        csv_files = glob.glob(os.path.join(run_dir, "slot_data_thread_*.csv"))
        for file in csv_files:
            with open(file, "r") as f:
                csv_reader = csv.DictReader(f)
                for row in csv_reader:
                    if 'block_slot' in row:
                        slot = int(row['block_slot'])
                        processed_slots.add(slot)

        log_files = glob.glob(os.path.join(run_dir, "solana*rpc*errors.log"))
        for file in log_files:
            with open(file, "r") as f:
                for line in f:
                    if "-32007" in line or "-32009" in line:
                        slot = int(line.split(',')[0])
                        processed_slots.add(slot)

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

    epoch_info = get_epoch_info(epoch_number)
    logger.info(f"Epoch Info: {epoch_info}")

    start_slot = epoch_info["start_slot"]
    end_slot = epoch_info["end_slot"]

    current_epoch_info = get_epoch_info()
    current_epoch = current_epoch_info["epoch_number"]

    if epoch_number == current_epoch:
        if epoch_info["slotIndex"] / epoch_info["slotsInEpoch"] < 0.9:
            end_slot = start_slot + epoch_info["slotIndex"]
    else:
        end_slot = epoch_info["end_slot"]

    slots_to_process = find_missing_slots(epoch_info["start_slot"], end_slot)

    all_slots = 0
    if slots_to_process:
        logger.info(f"Epoch {epoch_number} Number of slots to process: {len(slots_to_process)}")
        min_slot = min(slots_to_process)
        max_slot = max(slots_to_process)
        all_slots = max_slot - min_slot + 1
    else:
        logger.info("Epoch {epoch_number} No slots to process.")

    num_slots_to_process = len(slots_to_process)

    logger.info(f"Epoch {epoch_number} Total slots: {all_slots}, Range of slots: {start_slot} to {end_slot}, Number of slots to process: {num_slots_to_process}")

    last_slots_file = "last_slots_to_process.txt"

    if os.path.exists(last_slots_file):
        with open(last_slots_file, 'r') as f:
            last_num_slots = f.read().strip()
            logger.info(f"Epoch {epoch_number} Last run number of slots to process: {last_num_slots}")
            if str(num_slots_to_process) == last_num_slots:
                logger.info("Epoch {epoch_number} No new slots to process. Exiting.")
                exit(99)

    with open(last_slots_file, 'w') as f:
        f.write(str(num_slots_to_process))

    logger.info("")
    logger.info("█" * 100)
    logger.info("")

    logger.info("Press Enter to continue or wait for 1 minute...")
    timeout = 60
    start_time = time.time()
    while True:
        if time.time() - start_time >= timeout:
            break
        if os.sys.stdin in select.select([os.sys.stdin], [], [], 0)[0]:
            break
        time.sleep(0.1)

    num_threads = max(1, num_slots_to_process // 5000)
    if num_threads > 32: num_threads = 32
    slots_per_file = 500
    futures = []
    file_indices = [0] * num_threads

    with ThreadPoolExecutor(max_workers=num_threads) as executor:
        for i in range(0, len(slots_to_process), slots_per_file):
            thread_id = (i // slots_per_file) % num_threads + 1
            thread_slots = slots_to_process[i:i + slots_per_file]
            rpc_endpoint = RPC_ENDPOINT_1 if thread_id % 3 == 1 else (RPC_ENDPOINT_2 if thread_id % 3 == 2 else RPC_ENDPOINT_3)
            future = executor.submit(process_slot_data, thread_id, thread_slots, file_indices[thread_id - 1], epoch_info["epoch_number"], rpc_endpoint)
            futures.append(future)
            file_indices[thread_id - 1] += 1

        for future in as_completed(futures):
            slot_data_file, vote_data_file = future.result()
            logger.info(f"Epoch {epoch_number} Slot data written to: {slot_data_file}")
            logger.info(f"Epoch {epoch_number} Vote data written to: {vote_data_file}")

    logger.info("Epoch {epoch_number} Data retrieval and CSV file creation completed.")

if __name__ == "__main__":
    main()