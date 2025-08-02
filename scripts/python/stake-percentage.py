import json
import subprocess
import os
import logging
import datetime
import sys
import argparse

# ---------------------------
# Logging Setup: Log to both console and a file.
# ---------------------------
logger = logging.getLogger()
logger.setLevel(logging.INFO)
formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")

# Console handler
console_handler = logging.StreamHandler(sys.stdout)
console_handler.setFormatter(formatter)
logger.addHandler(console_handler)

# Parse Command-Line Argument (positional, defaults to mainnet)
parser = argparse.ArgumentParser(
    description="Generate validator stake ranking for mainnet or testnet."
)
parser.add_argument(
    "network",
    nargs="?",
    default="mainnet",
    choices=["mainnet", "testnet"],
    help="Network to process: mainnet (default) or testnet."
)
args = parser.parse_args()
network = args.network

# Get the basename of the script (without path or extension)
script_basename = os.path.splitext(os.path.basename(__file__))[0]

# Set up logging
# Get the basename of the current script
script_name = os.path.basename(sys.argv[0]).replace('.py', '')
# Set log directory in home folder
log_dir = os.path.expanduser('~/log')
# Construct the full log file path
log_file = os.path.join(log_dir, f"{script_name}.log")
file_handler = logging.FileHandler(log_file)
file_handler.setFormatter(formatter)
logger.addHandler(file_handler)

logger.info(f"Network selected: {network}")

# Set the Solana flag and file prefix based on the network.
if network == "mainnet":
    solana_flag = "--url https://side-silent-county.solana-mainnet.quiknode.pro/2ffa9d32adcd0102e7b78a8ba107f5c49b9420d8/"
    file_prefix = "mainnet"
else:
    solana_flag = "--url https://shy-yolo-sheet.solana-testnet.quiknode.pro/696dfc16996feaa4bc5f97cf207aafa02bcbdb9c/"
    file_prefix = "testnet"

SOLANA_CMD = "/home/smilax/.local/share/solana/install/active_release/bin/solana"
LAMPORTS_PER_SOL = 1_000_000_000

# ---------------------------
# Utility: Replace any non-ASCII character with an asterisk.
# ---------------------------
def replace_non_ascii_with_asterisk(text):
    """
    Replace any character that is not in the ASCII range (0-127)
    with an asterisk (*).
    """
    return ''.join(ch if ord(ch) < 128 else '*' for ch in text)

# ---------------------------
# Utility: Format a validator line using the non-ASCII replacement.
# ---------------------------
def print_validator_line(overall_rank, sub_rank, vote_key, name, version, stake_sol, cumulative_stake, total_stake_sol):
    # Replace any non-ASCII characters in the name with an asterisk.
    name = replace_non_ascii_with_asterisk(name)
    name = name[:44].ljust(44)
    cumulative_percentage = (cumulative_stake / total_stake_sol) * 100
    if version.startswith("0."):
        line = (
            f"{overall_rank:>4} {sub_rank:>8} {vote_key:<44} {name} {version:<10}"
            f"{stake_sol:>20,.2f}{cumulative_stake:>25,.2f}{cumulative_percentage:>11.2f}%"
        )
    else:
        line = (
            f"{overall_rank:>4} {sub_rank:>8} {vote_key:<44} {name} {version:<10} "
            f"{stake_sol:>20,.2f}{cumulative_stake:>25,.2f}{cumulative_percentage:>11.2f}%"
        )
    return line

logger.info("Starting validator processing script.")

# ---------------------------
# Get Epoch Info
# ---------------------------
try:
    logger.info("Running 'solana epoch-info' command to retrieve epoch info.")
    # Define the command as a list
    epoch_cmd = [SOLANA_CMD, "epoch-info", "--url", solana_flag.split()[1], "--output", "json"]
    # Log the command as a string for debugging
    logger.info(f"Executing command: {' '.join(epoch_cmd)}")
    epoch_result = subprocess.run(
        epoch_cmd,
        capture_output=True, text=True, check=True
    )    
    epoch_info = json.loads(epoch_result.stdout)
    epoch = epoch_info.get("epoch")
    epoch_completed_percent = epoch_info.get("epochCompletedPercent", 0)
    epoch_completed_percent_int = int(epoch_completed_percent)
    logger.info(f"Epoch info retrieved: Epoch {epoch}, Completed {epoch_completed_percent_int}%")
except Exception as e:
    logger.error("Failed to retrieve epoch info.", exc_info=True)
    epoch = "N/A"
    epoch_completed_percent_int = 0

# ---------------------------
# Create JSON Files by Running Solana Commands
# ---------------------------
validators_filename = f"{file_prefix}-validators.json"
validator_info_filename = f"{file_prefix}-validator-info.json"

logger.info(f"Running solana validators command to create {validators_filename}.")
with open(validators_filename, "w") as outfile:
    # Define the command as a list
    validators_cmd = [SOLANA_CMD, "validators", "--url", solana_flag.split()[1], "--output", "json"]
    # Log the command as a string for debugging
    logger.info(f"Executing command: {' '.join(validators_cmd)}")
    subprocess.run(validators_cmd, stdout=outfile, check=True)
logger.info(f"{validators_filename} file created successfully.")

logger.info(f"Running solana validator-info command to create {validator_info_filename}.")
with open(validator_info_filename, "w") as outfile:
    # Define the command as a list
    validator_info_cmd = [SOLANA_CMD, "validator-info", "--url", solana_flag.split()[1], "get", "--output", "json"]
    # Log the command as a string for debugging
    logger.info(f"Executing command: {' '.join(validator_info_cmd)}")
    subprocess.run(validator_info_cmd, stdout=outfile, check=True)
logger.info(f"{validator_info_filename} file created successfully.")

# ---------------------------
# Load and Process JSON Data
# ---------------------------
logger.info(f"Loading validators data from {validators_filename}.")
with open(validators_filename, "r") as f:
    data = json.load(f)

logger.info(f"Loading validator info data from {validator_info_filename}.")
with open(validator_info_filename, "r") as f:
    info_data = json.load(f)

validators = data["validators"]
logger.info(f"Total validators loaded: {len(validators)}.")

# Filter out delinquent validators.
logger.info("Filtering out delinquent validators.")
valid_validators = [v for v in validators if not v["delinquent"]]
non_delinquent_count = len(valid_validators)
logger.info(f"Non-delinquent validators count: {non_delinquent_count}.")

# Calculate total active stake (SOL) from non-delinquent validators.
total_active_stake_lamports = sum(v["activatedStake"] for v in valid_validators)
total_active_stake = total_active_stake_lamports / LAMPORTS_PER_SOL
logger.info(f"Total active stake (SOL): {total_active_stake:,.2f}")

# Build a mapping from identityPubkey to validator name.
logger.info("Building validator name map.")
validator_name_map = {
    entry["identityPubkey"]: entry["info"].get("name", entry["identityPubkey"])
    for entry in info_data
}

# Sort validators by version (ascending) and activated stake (descending).
logger.info("Sorting validators.")
sorted_validators = sorted(
    valid_validators,
    key=lambda v: (v["version"], -v["activatedStake"])
)
logger.info("Validators sorted successfully.")

# ---------------------------
# Write Output to File
# ---------------------------
output_filename = f"{file_prefix}-validators-stake-rank.txt"
logger.info(f"Writing output to {output_filename}.")
with open(output_filename, "w") as outfile:
    # Write header line with current date/time, epoch info, non-delinquent count, and total active stake.
    current_time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    header_line = (
        f"{current_time} - Epoch: {epoch}, Completed: {epoch_completed_percent_int}%, "
        f"Non-delinquent validators: {non_delinquent_count}, Total active stake (SOL): {total_active_stake:,.2f}"
    )
    outfile.write(header_line + "\n\n")
    
    # Write table header.
    table_header = (
        f"{'Rank':>4} {'Sub Rank':>8} {'VoteAccountPubkey':<44} {'Name':<44} "
        f"{'Version':<10} {'ActivatedStake(SOL)':>20} {'CumulativeStake(SOL)':>25} {'Cumulative%':>12}"
    )
    outfile.write(table_header + "\n")
    
    cumulative_stake = 0.0
    overall_rank = 1
    sub_rank = 1
    previous_version = None

    # Process each validator.
    for v in sorted_validators:
        stake_sol = v["activatedStake"] / LAMPORTS_PER_SOL
        cumulative_stake += stake_sol
        identity = v["identityPubkey"]
        name = validator_name_map.get(identity, identity)
        
        # Reset sub-rank when the version changes.
        if v["version"] != previous_version:
            sub_rank = 1
            previous_version = v["version"]
        
        line = print_validator_line(
            overall_rank, sub_rank, v["voteAccountPubkey"], name,
            v["version"], stake_sol, cumulative_stake, total_active_stake
        )
        outfile.write(line + "\n")
        overall_rank += 1
        sub_rank += 1

logger.info(f"Validator processing completed successfully. Output written to {output_filename}.")
