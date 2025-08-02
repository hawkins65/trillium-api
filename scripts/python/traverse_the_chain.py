import csv
import glob
import argparse
import requests

headers = {'Content-Type': 'application/json'}
RPC_ENDPOINT = "https://api.mainnet-beta.solana.com"

def get_epoch_info(epoch_number=None):
    payload = {
        "jsonrpc": "2.0",
        "id": 1,
        "method": "getEpochInfo",
        "params": [None]  # Optional `commitment` parameter
    }
    response = requests.post(RPC_ENDPOINT, headers=headers, json=payload)
    epoch_info = response.json()["result"]

    if epoch_number is None:
        epoch_number = epoch_info["epoch"]

    if epoch_number < epoch_info["epoch"]:
        first_slot_of_epoch = epoch_info["absoluteSlot"] - epoch_info["slotIndex"] - (epoch_info["epoch"] - epoch_number) * epoch_info["slotsInEpoch"]
        last_slot_of_epoch = first_slot_of_epoch + epoch_info["slotsInEpoch"] - 1
    else:
        first_slot_of_epoch = epoch_info["absoluteSlot"] - epoch_info["slotIndex"]
        last_slot_of_epoch = first_slot_of_epoch + epoch_info["slotsInEpoch"] - 1

    return {
        "epoch_number": epoch_number,
        "start_slot": first_slot_of_epoch,
        "end_slot": last_slot_of_epoch
    }

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--epoch-number', type=int, default=None, help='Epoch number to process')
    args = parser.parse_args()

    epoch_number = args.epoch_number
    epoch_info = get_epoch_info(epoch_number)
    start_slot = epoch_info["start_slot"]
    end_slot = epoch_info["end_slot"]

    slot_data = {}
    for run_dir in glob.glob('run*'):
        for csv_file in glob.glob(f"{run_dir}/slot_data_thread_*.csv"):
            with open(csv_file, 'r') as f:
                reader = csv.DictReader(f)
                for row in reader:
                    block_slot = row['block_slot']
                    parent_slot = row['parent_slot']
                    slot_data[block_slot] = parent_slot

    missing_slots = []
    current_slot = str(end_slot)
    lsd = len(slot_data)
    fnd = 0
    msg_slots = 0
    print(f"len(slot_data) {lsd}")
    while int(current_slot) >= start_slot:
        if current_slot in slot_data:
            fnd = fnd + 1
            #print(f"found current_slot {current_slot}")
            parent_slot = slot_data[current_slot]
            current_slot = parent_slot
        else:  
            msg_slots = msg_slots + 1
            #print(f"missing current_slot {current_slot}")
            missing_slots.append(current_slot)
            current_slot = str(int(current_slot) - 1)
    
    print(f"found {fnd} slots")
    print(f"missing {msg_slots} slots")
    msg_len = len(missing_slots)
    print(f"len(missing_slots) {msg_len}")
    
    with open(f"_missing_slots_epoch_{epoch_number}.log", 'w') as f:
        for slot in missing_slots:
            f.write(str(slot) + "\n")

    print(f"\nMissing slots for epoch {epoch_number} logged to _missing_slots_epoch_{epoch_number}.log")

if __name__ == "__main__":
    main()
