import os
import json
import subprocess
import psycopg2
import requests
import importlib.util

# Setup unified logging
script_dir = os.path.dirname(os.path.abspath(__file__))
logging_config_path = os.path.join(script_dir, "999_logging_config.py")
spec = importlib.util.spec_from_file_location("logging_config", logging_config_path)
logging_config = importlib.util.module_from_spec(spec)
spec.loader.exec_module(logging_config)
logger = logging_config.setup_logging(os.path.basename(__file__).replace('.py', ''))
import time
from db_config import db_params
from rpc_config import RPC_ENDPOINT  # Import the centralized RPC endpoint

# Use the imported RPC endpoint
RPC_URL = RPC_ENDPOINT
RPC_URL2 = "https://silent-frequent-firefly.solana-mainnet.quiknode.pro/2059a05165e13886cb8226c6b87081ad579860e3/"

# Directory configurations
SCRIPT_DIR = "/home/smilax/api"
LOG_DIR = os.getenv("LOG_DIR", os.path.expanduser("~/log"))
LOG_FILE = os.path.join(LOG_DIR, "99_solana-stakes.log")
SOLANA_BIN = "/home/smilax/agave/bin/solana"

# Constants
MAX_RETRIES = 3
RETRY_DELAY = 5  # Seconds between retries

headers = {"Content-Type": "application/json"}

# Configure logging
# Logger setup moved to unified configuration
logger.setLevel(logging.INFO)

# Clear any existing handlers
logger.handlers = []

# File handler
file_handler = logging.FileHandler(LOG_FILE)
file_handler.setLevel(logging.INFO)
file_formatter = logging.Formatter("[%(asctime)s] [Epoch %(epoch)s] %(message)s", datefmt="%Y-%m-%d %H:%M:%S")
file_handler.setFormatter(file_formatter)

# Console handler
console_handler = logging.StreamHandler()
console_handler.setLevel(logging.INFO)
console_formatter = logging.Formatter("[%(asctime)s] [Epoch %(epoch)s] %(message)s", datefmt="%Y-%m-%d %H:%M:%S")
console_handler.setFormatter(console_formatter)

# Add handlers
logger.addHandler(file_handler)
logger.addHandler(console_handler)

# Custom logging function
def log_with_epoch(message, epoch, level="info"):
    extra = {"epoch": epoch if epoch is not None else "N/A"}
    if level == "info":
        logger.info(message, extra=extra)
    elif level == "error":
        logger.error(message, extra=extra)
    elif level == "warning":
        logger.warning(message, extra=extra)

def get_db_connection_string(db_params):
    return f"postgresql://{db_params['user']}@{db_params['host']}:{db_params['port']}/{db_params['database']}?sslmode={db_params['sslmode']}"

def get_epoch_info(rpc_url):
    payload = {
        "jsonrpc": "2.0",
        "id": 1,
        "method": "getEpochInfo",
        "params": []
    }
    for attempt in range(MAX_RETRIES):
        try:
            response = requests.post(rpc_url, headers=headers, json=payload)
            log_with_epoch(f"getEpochInfo attempt {attempt + 1} on {rpc_url}: status {response.status_code}", None)
            if response.status_code == 200 and "result" in response.json():
                epoch = response.json()["result"]["epoch"]
                log_with_epoch(f"Fetched epoch info: {epoch}", epoch)
                return epoch
            else:
                log_with_epoch(f"Failed to fetch epoch info from {rpc_url}: {response.text}", None, "error")
        except Exception as e:
            log_with_epoch(f"Error fetching epoch info from {rpc_url} (attempt {attempt + 1}): {e}", None, "error")
        if attempt < MAX_RETRIES - 1:
            log_with_epoch(f"Retrying getEpochInfo on {rpc_url} after {RETRY_DELAY} seconds", None)
            time.sleep(RETRY_DELAY)
    raise Exception(f"Failed to fetch epoch info from {rpc_url} after retries")

def load_stake_data():
    # Ensure log directory exists
    os.makedirs(LOG_DIR, exist_ok=True)

    # Fetch the current epoch
    epoch = None
    try:
        try:
            epoch = get_epoch_info(RPC_URL)
        except Exception as e:
            log_with_epoch(f"WARNING: Failed to fetch epoch info from {RPC_URL}: {e}. Falling back to {RPC_URL2}", None, "warning")
            epoch = get_epoch_info(RPC_URL2)

        OUTPUT_FILE = os.path.join(SCRIPT_DIR, f"solana-stakes_{epoch}.json")
        solana_command = f"{SOLANA_BIN} stakes --output json --url {RPC_URL}"

        # Try RPC_URL first
        success = False
        last_error = None
        try:
            log_with_epoch(f"Running command: {solana_command} > {OUTPUT_FILE}", epoch)
            for attempt in range(MAX_RETRIES):
                try:
                    result = subprocess.run(
                        solana_command,
                        shell=True,
                        check=True,
                        capture_output=True,
                        text=True
                    )
                    with open(OUTPUT_FILE, 'w') as f:
                        f.write(result.stdout)
                    log_with_epoch(f"Successfully executed solana command, output written to {OUTPUT_FILE}", epoch)
                    success = True
                    break
                except subprocess.CalledProcessError as e:
                    error_msg = f"Error executing solana command (attempt {attempt + 1}): exit code {e.returncode}, stderr: {e.stderr}, stdout: {e.stdout}"
                    log_with_epoch(error_msg, epoch, "error")
                    last_error = error_msg
                    if attempt < MAX_RETRIES - 1:
                        log_with_epoch(f"Retrying solana command after {RETRY_DELAY} seconds", epoch)
                        time.sleep(RETRY_DELAY)
                except Exception as e:
                    log_with_epoch(f"Unexpected error executing solana command (attempt {attempt + 1}): {e}", epoch, "error")
                    last_error = str(e)
                    if attempt < MAX_RETRIES - 1:
                        log_with_epoch(f"Retrying solana command after {RETRY_DELAY} seconds", epoch)
                        time.sleep(RETRY_DELAY)
            if not success:
                raise Exception(f"Failed to execute solana command with {RPC_URL}: {last_error}")
        except Exception as e:
            log_with_epoch(f"WARNING: Failed to execute solana command with {RPC_URL}: {e}. Falling back to {RPC_URL2}", epoch, "warning")
            solana_command = f"{SOLANA_BIN} stakes --output json --url {RPC_URL2}"
            log_with_epoch(f"Running command: {solana_command} > {OUTPUT_FILE}", epoch)
            for attempt in range(MAX_RETRIES):
                try:
                    result = subprocess.run(
                        solana_command,
                        shell=True,
                        check=True,
                        capture_output=True,
                        text=True
                    )
                    with open(OUTPUT_FILE, 'w') as f:
                        f.write(result.stdout)
                    log_with_epoch(f"Successfully executed solana command with {RPC_URL2}, output written to {OUTPUT_FILE}", epoch)
                    success = True
                    break
                except subprocess.CalledProcessError as e:
                    error_msg = f"Error executing solana command with {RPC_URL2} (attempt {attempt + 1}): exit code {e.returncode}, stderr: {e.stderr}, stdout: {e.stdout}"
                    log_with_epoch(error_msg, epoch, "error")
                    if attempt < MAX_RETRIES - 1:
                        log_with_epoch(f"Retrying solana command with {RPC_URL2} after {RETRY_DELAY} seconds", epoch)
                        time.sleep(RETRY_DELAY)
                    else:
                        raise Exception(f"Failed to execute solana command with {RPC_URL2} after {MAX_RETRIES} attempts: {error_msg}")
                except Exception as e:
                    log_with_epoch(f"Unexpected error executing solana command with {RPC_URL2} (attempt {attempt + 1}): {e}", epoch, "error")
                    if attempt < MAX_RETRIES - 1:
                        log_with_epoch(f"Retrying solana command with {RPC_URL2} after {RETRY_DELAY} seconds", epoch)
                        time.sleep(RETRY_DELAY)
                    else:
                        raise

        # Read the generated JSON file
        log_with_epoch(f"Reading stake data from {OUTPUT_FILE}", epoch)
        try:
            with open(OUTPUT_FILE, 'r') as file:
                data = json.load(file)
        except json.JSONDecodeError as e:
            log_with_epoch(f"Error decoding JSON from {OUTPUT_FILE}: {e}", epoch, "error")
            raise
        except Exception as e:
            log_with_epoch(f"Error reading {OUTPUT_FILE}: {e}", epoch, "error")
            raise

        # Log number of stake accounts
        log_with_epoch(f"Found {len(data)} stake accounts", epoch)
        if not data:
            log_with_epoch("No stake accounts found in output", epoch, "error")
            raise Exception("No stake accounts found")

        # Connect to the database
        log_with_epoch("Connecting to database", epoch)
        connection = psycopg2.connect(get_db_connection_string(db_params))
        cursor = connection.cursor()

        # Clear existing data for this epoch
        log_with_epoch(f"Deleting existing stake data for epoch {epoch}", epoch)
        delete_query = "DELETE FROM stake_accounts WHERE epoch = %s;"
        cursor.execute(delete_query, (epoch,))

        # Prepare the SQL insert statement
        insert_query = """
            INSERT INTO stake_accounts (
                stake_pubkey, epoch, stake_type, account_balance, credits_observed,
                delegated_stake, delegatedVoteAccountAddress, activation_epoch,
                staker, withdrawer, rent_exempt_reserve, active_stake,
                activating_stake, deactivation_epoch, deactivating_stake,
                unix_timestamp, custodian
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (stake_pubkey, epoch) DO NOTHING;
        """

        # Insert each record
        log_with_epoch(f"Inserting {len(data)} stake records into database", epoch)
        for record in data:
            cursor.execute(insert_query, (
                record.get("stakePubkey"),
                epoch,
                record.get("stakeType"),
                record.get("accountBalance"),
                record.get("creditsObserved"),
                record.get("delegatedStake"),
                record.get("delegatedVoteAccountAddress"),
                record.get("activationEpoch"),
                record.get("staker"),
                record.get("withdrawer"),
                record.get("rentExemptReserve"),
                record.get("activeStake"),
                record.get("activatingStake"),
                record.get("deactivationEpoch"),
                record.get("deactivatingStake"),
                record.get("unixTimestamp"),
                record.get("custodian")
            ))

        # Commit the transaction
        connection.commit()
        log_with_epoch(f"Successfully loaded stake data for epoch {epoch}", epoch)
        
    except Exception as e:
        log_with_epoch(f"Error loading stake data: {e}", epoch, "error")
        raise
    finally:
        if 'cursor' in locals():
            cursor.close()
        if 'connection' in locals():
            connection.close()

if __name__ == "__main__":
    try:
        load_stake_data()
    except Exception as e:
        log_with_epoch(f"Script failed: {e}", None, "error")
        exit(1)