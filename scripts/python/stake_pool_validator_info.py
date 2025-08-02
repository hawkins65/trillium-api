#!/usr/bin/env python3

import json
import csv
import os
import webbrowser

# API URLs
STAKEPOOL_API = "https://www.jito.network/api/stakepools/getAllSplStakePoolValidators/"
LST_API = "https://www.jito.network/api/loadLstList/"

# File names from script basename
BASENAME = os.path.splitext(os.path.basename(__file__))[0]
OUTPUT = f"{BASENAME}.csv"
STAKEPOOL_JSON = f"{BASENAME}_stakepool.json"
LST_JSON = f"{BASENAME}_lst.json"

# Prompt user to download JSON files
print("This script requires you to manually download JSON files from the following URLs:")
print(f"1. {STAKEPOOL_API}")
print(f"   Save the response as: {STAKEPOOL_JSON}")
print(f"2. {LST_API}")
print(f"   Save the response as: {LST_JSON}")
print("\nInstructions:")
print("- Open each URL in a browser on another machine (since this is a server without a display).")
print("- Right-click the page, select 'Save As', and save as a .json file with the exact names above.")
print("- Ensure 'Save as type' is 'All Files' or 'JSON', not HTML.")
print("- Transfer the files to this server's current directory.")
print("\nAlternatively, use curl/wget on another machine and copy the files:")
print(f"   curl -o {STAKEPOOL_JSON} {STAKEPOOL_API}")
print(f"   curl -o {LST_JSON} {LST_API}")

# Since no display, skip webbrowser and wait for user
input("\nAfter saving and transferring both files to this directory, press Enter to proceed...")

# Check if files exist
if not os.path.exists(STAKEPOOL_JSON):
    print(f"Error: '{STAKEPOOL_JSON}' not found")
    exit(1)
if not os.path.exists(LST_JSON):
    print(f"Error: '{LST_JSON}' not found")
    exit(1)

# Read JSON files
try:
    with open(STAKEPOOL_JSON, 'r') as f:
        stakepool_data = json.load(f)
    with open(LST_JSON, 'r') as f:
        lst_data = json.load(f)
except Exception as e:
    print(f"Error: Failed to read or parse JSON files - {e}")
    print("Ensure the files contain valid JSON (check by opening in a text editor).")
    exit(1)

# Write CSV
with open(OUTPUT, 'w', newline='') as csvfile:
    writer = csv.writer(csvfile)
    writer.writerow(["voteAccountAddress", "poolMint", "lastUpdateEpoch", "activeStakeLamports", "name", "symbol", "logo_uri"])

    # Process data
    lst_map = {item["mint"]: item for item in lst_data.get("sanctum_lst_list", [])}
    for pool_mint, pool_data in stakepool_data.items():
        lst_info = lst_map.get(pool_mint, {})
        for validator in pool_data.get("simpleValidatorList", []):
            writer.writerow([
                validator.get("voteAccountAddress", ""),
                pool_mint,
                validator.get("lastUpdateEpoch", ""),
                validator.get("activeStakeLamports", ""),
                lst_info.get("name", ""),
                lst_info.get("symbol", ""),
                lst_info.get("logo_uri", "")
            ])

print(f"CSV file '{OUTPUT}' created successfully")