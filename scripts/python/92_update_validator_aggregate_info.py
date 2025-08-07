# Standard Library Imports
import asyncio
import csv
import filetype
import ipaddress
import json
import logging
import os
import importlib.util

# Setup unified logging
script_dir = os.path.dirname(os.path.abspath(__file__))
logging_config_path = os.path.join(script_dir, "999_logging_config.py")
spec = importlib.util.spec_from_file_location("logging_config", logging_config_path)
logging_config = importlib.util.module_from_spec(spec)
spec.loader.exec_module(logging_config)
logger = logging_config.setup_logging(os.path.basename(__file__).replace('.py', ''))
import mimetypes
import os
import re
import subprocess
import sys
import tempfile
import threading
import time
import traceback
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from decimal import Decimal
from io import StringIO
from urllib.parse import unquote, urlparse

# Third-Party Library Imports
import magic
import psycopg2
import requests
import urllib3
from psycopg2 import sql
from psycopg2.extras import execute_batch, execute_values
from requests.adapters import HTTPAdapter
from requests.exceptions import ConnectionError, RequestException, Timeout, TooManyRedirects
from urllib3.util.retry import Retry

from rpc_config import RPC_ENDPOINT  # Import the centralized RPC endpoint

DEBUG = True  # Set to False to disable debug printing

# Use the imported RPC endpoint
RPC_URL = RPC_ENDPOINT

# Global variables for thread-safe access
city_reader = None
asn_reader = None
country_region_map = {}
thread_local = threading.local()

SOLANA_CMD = "/home/smilax/agave/bin/solana"
PSQL_CMD = '/usr/bin/psql'
CURL_CMD = '/usr/bin/curl'
VALIDATOR_HISTORY_CLI = '/home/smilax/stakenet/target/release/validator-history-cli'
MAX_WORKERS = 10  # Adjust based on system resources and API rate limits

# PostgreSQL database connection parameters
from db_config import db_params

def get_db_connection_string(db_params):
    return f"postgresql://{db_params['user']}@{db_params['host']}:{db_params['port']}/{db_params['database']}?sslmode={db_params['sslmode']}"

# Replace the hardcoded connection string with this function call
db_connection_string = get_db_connection_string(db_params)

def get_db_connection(db_params):
    conn = psycopg2.connect(**db_params)
    logger.debug(f"Created new DB connection: {conn}")
    return conn

def setup_logging():
    # Logger setup moved to unified configuration
    logger.setLevel(logging.DEBUG)  # Set the root logger level to the lowest level you want to capture

    now = datetime.now()
    formatted_time = now.strftime('%Y-%m-%d_%H-%M')
    script_name = os.path.basename(__file__).replace('.py', '')  # Gets script name without .py
    filename = f'/home/smilax/log/{script_name}_log_{formatted_time}.log'

    # File handler
    fh = logging.FileHandler(filename)
    fh.setLevel(logging.DEBUG)  # Capture all logs to the file
    fh_formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    fh.setFormatter(fh_formatter)

    # Console handler
    ch = logging.StreamHandler(sys.stdout)
    ch.setLevel(logging.WARNING)  # Only display WARNING and above on the console
    ch_formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    ch.setFormatter(ch_formatter)

    # Add handlers to the logger
    logger.addHandler(fh)
    logger.addHandler(ch)

    return logger

# Create a logger
logger = setup_logging()

# Disable warnings and set logging levels
urllib3.disable_warnings()
logging.getLogger("requests").setLevel(logging.WARNING)
logging.getLogger("urllib3").setLevel(logging.WARNING)
      
def import_leader_schedule(db_params, file_path):    
    logger.info(f"Attempting to open file: {file_path}")
    try:
        with open(file_path, 'r') as file:
            logger.info(f"File opened successfully: {file_path}")
            data = json.load(file)
    except FileNotFoundError:
        logger.error(f"File not found: {file_path}")
        return
    except json.JSONDecodeError:
        logger.error(f"Invalid JSON in file: {file_path}")
        return
    except Exception as e:
        logger.error(f"Unexpected error opening {file_path}: {str(e)}")
        return

    logger.info(f"JSON data loaded. Keys in data: {list(data.keys())}")

    if 'epoch' not in data or 'leaderScheduleEntries' not in data:
        logger.error(f"Missing required keys in JSON data: {file_path}")
        return

    epoch = data['epoch']
    entries = data['leaderScheduleEntries']

    logger.info(f"Epoch: {epoch}, Number of entries in schedule: {len(entries)}")

    conn = psycopg2.connect(**db_params)
    cur = conn.cursor()

    try:
        # Query to get all produced blocks for the epoch
        cur.execute("""
            SELECT block_slot, identity_pubkey
            FROM validator_data 
            WHERE epoch = %s
        """, (epoch,))
        produced_blocks = {row[0]: row[1] for row in cur.fetchall()}

        logger.info(f"Number of produced blocks for epoch {epoch}: {len(produced_blocks)}")

        insert_query = sql.SQL("""
            INSERT INTO leader_schedule 
            (epoch, block_slot, identity_pubkey, block_produced)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (epoch, block_slot) DO UPDATE SET
            identity_pubkey = EXCLUDED.identity_pubkey,
            block_produced = EXCLUDED.block_produced
        """)

        insert_entries = []
        mismatches = []

        for entry in entries:
            slot = entry['slot']
            identity_pubkey = entry['leader']
            block_produced = slot in produced_blocks
                        
            # Perform the comparison directly
            if block_produced and identity_pubkey != produced_blocks[slot]:
                mismatches.append((epoch, slot, identity_pubkey, produced_blocks[slot]))

            insert_entries.append((
                epoch,
                slot,
                identity_pubkey,
                block_produced
            ))

        logger.info(f"Prepared {len(insert_entries)} entries for insertion")

        execute_batch(cur, insert_query, insert_entries)
        conn.commit()
        logger.info(f"Imported {len(insert_entries)} entries for epoch {epoch} from file {file_path}")

        if mismatches:
            logger.info(f"{len(mismatches)} Identity pubkey mismatches found in epoch {epoch} - will be set to leader_schedule pubkey later")
            for mismatch in mismatches: 
                logger.info(f"Mismatch - Slot: {mismatch[1]}, Expected: {mismatch[2]}, Found: {mismatch[3]}")

        # Add a query to check if leader_slots were updated
        cur.execute("""
            SELECT COUNT(*) 
            FROM leader_schedule 
            WHERE epoch = %s
        """, (epoch,))
        leader_slots_count = cur.fetchone()[0]
        logger.info(f"Number of entries in leader_schedule for epoch {epoch}: {leader_slots_count}")

    except Exception as e:
        logger.error(f"An error occurred while processing {file_path}: {e}")
        conn.rollback()
        raise
    finally:
        cur.close()
        conn.close()

def process_leader_schedules(db_params, directory, start_epoch, end_epoch):   
    logger.info(f"Processing leader schedules from {directory}")
    logger.info(f"Looking for schedules between epochs {start_epoch} and {end_epoch}")
    
    filename_pattern = re.compile(r'epoch(\d+)-leaderschedule\.json')
    
    for filename in [f for f in os.listdir(directory) if f.endswith('.json')]:
        match = filename_pattern.match(filename)
        if match:
            file_epoch = int(match.group(1))
            
            if start_epoch <= file_epoch <= end_epoch:
                file_path = os.path.join(directory, filename)
                logger.info(f"Processing file: {filename}")
                
                try:
                    with open(file_path, 'r') as file:
                        data = json.load(file)
                    
                    if 'epoch' in data and data['epoch'] == file_epoch:
                        logger.info(f"Importing data for epoch {file_epoch}")
                        import_leader_schedule(db_params, file_path)
                    else:
                        logger.warning(f"Epoch mismatch in {filename}. "
                                        f"Filename epoch: {file_epoch}, Content epoch: {data.get('epoch', 'Not found')}")                
                except Exception as e:
                    logger.warning(f"An unexpected error occurred while processing {filename}: {str(e)}")
            else:
                # jrh -- no need to inform on these -- logger.info(f"Skipping {filename} (epoch {file_epoch} out of range)")
                continue
        else:
            # jrh -- no need to inform on these -- logger.info(f"Skipping file with non-matching name pattern: {filename}")
            continue

    logger.info("Finished processing leader schedules")

def update_validator_skip_rates(conn, epoch):
    cur = conn.cursor()

    try:
        update_query = sql.SQL("""
            WITH skip_rate_calc AS (
                SELECT 
                    identity_pubkey,
                    epoch,
                    COUNT(*) AS total_slots,
                    SUM(CASE WHEN block_produced THEN 1 ELSE 0 END) AS blocks_produced,
                    ROUND(
                        (CAST(COUNT(*) - SUM(CASE WHEN block_produced THEN 1 ELSE 0 END) AS numeric) / NULLIF(COUNT(*), 0)) * 100, 
                        2
                    ) AS skip_rate
                FROM leader_schedule
                WHERE epoch = {}
                GROUP BY identity_pubkey, epoch
            )
            UPDATE validator_stats vs
            SET 
                skip_rate = src.skip_rate,
                blocks_produced = src.blocks_produced,
                leader_slots = src.total_slots
            FROM skip_rate_calc src
            WHERE 
                vs.identity_pubkey = src.identity_pubkey 
                AND vs.epoch = src.epoch
            RETURNING vs.identity_pubkey, vs.leader_slots, vs.blocks_produced, vs.skip_rate;
        """).format(sql.Literal(epoch))

        cur.execute(update_query)
        updated_rows = cur.fetchall()
        conn.commit()
        logger.info(f"Updated validator_stats table for epoch {epoch}")
        logger.info(f"Number of rows updated: {len(updated_rows)}")
        logger.info(f"Sample of updated rows: {updated_rows[:5]}")

        # Check if leader_slots were updated
        cur.execute("""
            SELECT COUNT(*) 
            FROM validator_stats 
            WHERE epoch = %s AND leader_slots > 0
        """, (epoch,))
        leader_slots_count = cur.fetchone()[0]
        logger.info(f"Number of validators with leader_slots > 0 for epoch {epoch}: {leader_slots_count}")

    except Exception as e:
        logger.error(f"An error occurred while updating skip rates for epoch {epoch}: {e}")
        conn.rollback()
    finally:
        cur.close()

def fetch_icon_url_from_keybase(keybase_username):
    url = f"https://keybase.io/_/api/1.0/user/lookup.json?username={keybase_username}"
    headers = {"Content-Type": "application/json"}

    try:
        response = requests.get(url, headers=headers, timeout=5)
        if response.status_code == 200:
            json_response = response.json()
            pictures = json_response.get("them", {}).get("pictures", {})
            primary_url = pictures.get("primary", {}).get("url")
            if primary_url:
                return primary_url
            else:
                return None
        else:
            return None
    except requests.exceptions.RequestException as e:
        return None

def fetch_and_store_icons(conn, cur):
    icon_dir = "/home/smilax/trillium_api/static/images"
    os.makedirs(icon_dir, exist_ok=True)

    # Read existing 92_icon_url_errors.list to create a set of identity_pubkeys to skip
    skip_pubkeys = set()
    error_list_path = "./data/configs/92_icon_url_errors.list"
    os.makedirs("./data/configs", exist_ok=True)
    try:
        with open(error_list_path, "r", newline="") as error_file:
            csv_reader = csv.reader(error_file)
            next(csv_reader)  # Skip header row
            for row in csv_reader:
                if row:  # Check if row is not empty
                    continue # jrh -- let's try them all every epoch
                    #skip_pubkeys.add(row[0])  # Add identity_pubkey to skip set
    except FileNotFoundError:
        logger.info(f"{error_list_path} not found. Creating a new file.")

    # Open 92_icon_url_errors.list in append mode
    csv_file = open(error_list_path, "a", newline="")
    csv_writer = csv.writer(csv_file)

    # Write header only if the file is empty
    if csv_file.tell() == 0:
        csv_writer.writerow(["identity_pubkey", "icon_url", "error"])

    cur.execute("SELECT identity_pubkey, icon_url, keybase_username FROM validator_info")
    rows = cur.fetchall()

    for row in rows:
        identity_pubkey, icon_url, keybase_username = row

        if identity_pubkey in skip_pubkeys:
            logger.info(f"Skipping {identity_pubkey} (in error list)")
            continue

        if not icon_url:
            if keybase_username:
                icon_url = fetch_icon_url_from_keybase(keybase_username)
                if icon_url:
                    cur.execute(
                        "UPDATE validator_info SET icon_url = %s WHERE identity_pubkey = %s",
                        (icon_url, identity_pubkey),
                    )
                    conn.commit()
                else:
                    csv_writer.writerow([identity_pubkey, "", f"No icon_url found for {keybase_username}"])
                    cur.execute(
                        "UPDATE validator_info SET logo = %s WHERE identity_pubkey = %s",
                        ("no-image-available12.webp", identity_pubkey),
                    )
                    conn.commit()
                    continue
            else:
                csv_writer.writerow([identity_pubkey, "", "No icon_url and no keybase_username found"])
                cur.execute(
                    "UPDATE validator_info SET logo = %s WHERE identity_pubkey = %s",
                    ("no-image-available12.webp", identity_pubkey),
                )
                conn.commit()
                continue

        # Validate icon_url format
        parsed = urlparse(icon_url)
        if not (parsed.scheme in ('http', 'https') and parsed.netloc):
            error_message = f"Invalid URL format: {icon_url}"
            logger.error(error_message)
            csv_writer.writerow([identity_pubkey, icon_url, error_message])
            cur.execute(
                "UPDATE validator_info SET logo = %s WHERE identity_pubkey = %s",
                ("no-image-available12.webp", identity_pubkey),
            )
            conn.commit()
            continue

        filename = os.path.basename(unquote(parsed.path))
        file_extension = os.path.splitext(filename)[1]

        # If no file extension, we will determine it later
        filename = f"{identity_pubkey}{file_extension}"
        file_path = os.path.join(icon_dir, filename)
        headers = {
            'Accept': 'image/png,image/*',
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/129.0.0.0 Safari/537.36'
        }
        try:
            response = requests.get(icon_url, headers=headers, timeout=5, allow_redirects=True)

            if response.status_code == 200:
                # Step 1: Try to determine the extension using the Content-Type header
                content_type = response.headers.get("Content-Type")
                extension = None
                
                if content_type:
                    extension = mimetypes.guess_extension(content_type)

                # Step 2: If no extension is found from Content-Type, use filetype
                if not extension:
                    kind = filetype.guess(response.content)
                    if kind and kind.mime.startswith('image/'):
                        extension = f".{kind.extension}"
                    else:
                        extension = ""  # No extension if filetype fails

                # Build the final filename
                filename = f"{identity_pubkey}{extension}"
                file_path = os.path.join(icon_dir, filename)

                # Save the image with the determined extension
                with open(file_path, "wb") as file:
                    file.write(response.content)

                # Sanitize logo filename
                if not filename.endswith(('.png', '.jpg', '.jpeg', '.webp', '.gif')):
                    filename = "no-image-available12.webp"
                    logger.info(f"Invalid image extension for {identity_pubkey}, using default logo")

                cur.execute(
                    "UPDATE validator_info SET logo = %s WHERE identity_pubkey = %s",
                    (filename, identity_pubkey),
                )
                conn.commit()

            else:
                error_message = f"Error: Failed to fetch '{icon_url}' (status code: {response.status_code})"
                logger.error(error_message)
                csv_writer.writerow([identity_pubkey, icon_url, error_message])
                cur.execute(
                    "UPDATE validator_info SET logo = %s WHERE identity_pubkey = %s",
                    ("no-image-available12.webp", identity_pubkey),
                )
                conn.commit()

        except requests.exceptions.RequestException as e:
            error_message = f"Error: {e}"
            logger.info(error_message)
            csv_writer.writerow([identity_pubkey, icon_url, error_message])
            cur.execute(
                "UPDATE validator_info SET logo = %s WHERE identity_pubkey = %s",
                ("no-image-available12.webp", identity_pubkey),
            )
            conn.commit()

    csv_file.close()
    logger.info("Validator icons processing completed.")

def fetch_and_store_gossip_data(rpc_url, output_file="92_gossip.json"):
    """Fetch gossip data using solana gossip command and save to a JSON file."""
    cmd = [SOLANA_CMD, "gossip", "--url", rpc_url, "--output", "json"]
    
    logger.info(f"Executing solana gossip command: {' '.join(cmd)}")
    try:
        with open(output_file, 'w') as f:
            subprocess.run(cmd, stdout=f, check=True, text=True)
        logger.info(f"Gossip data saved to {output_file}")
    except subprocess.CalledProcessError as e:
        logger.error(f"Error executing solana gossip command: {e}")
        raise
    except Exception as e:
        logger.error(f"Unexpected error while saving gossip data: {e}")
        raise

def update_validator_stats_with_gossip(db_params, start_epoch, end_epoch, gossip_file="92_gossip.json"):
    """Update validator_stats with IP addresses and versions from gossip data."""
    logger.info(f"Processing gossip data from {gossip_file} for epochs {start_epoch} to {end_epoch}")
    
    # Load gossip JSON data
    try:
        with open(gossip_file, 'r') as f:
            gossip_data = json.load(f)
        logger.info(f"Loaded {len(gossip_data)} gossip entries from {gossip_file}")
    except FileNotFoundError:
        logger.error(f"Gossip file {gossip_file} not found")
        return
    except json.JSONDecodeError:
        logger.error(f"Invalid JSON in {gossip_file}")
        return
    except Exception as e:
        logger.error(f"Unexpected error reading {gossip_file}: {e}")
        return

    # Connect to the database
    conn = psycopg2.connect(**db_params)
    cur = conn.cursor()

    try:
        # Create a temporary table to hold gossip data
        cur.execute("""
            CREATE TEMPORARY TABLE temp_gossip_data (
                identity_pubkey TEXT,
                ip TEXT,
                version TEXT
            ) ON COMMIT DROP;
        """)

        # Prepare data for bulk insertion
        gossip_entries = [
            (entry["identityPubkey"], entry["ipAddress"], entry["version"])
            for entry in gossip_data
            if "identityPubkey" in entry and "ipAddress" in entry and "version" in entry
        ]
        logger.info(f"Prepared {len(gossip_entries)} valid gossip entries for insertion")

        # Use copy_from for efficient bulk loading
        with tempfile.NamedTemporaryFile(mode='w', delete=False) as temp_file:
            writer = csv.writer(temp_file)
            for entry in gossip_entries:
                writer.writerow(entry)
            temp_file_path = temp_file.name

        with open(temp_file_path, 'r') as temp_file:
            cur.copy_from(temp_file, 'temp_gossip_data', sep=',', columns=('identity_pubkey', 'ip', 'version'))
        
        os.unlink(temp_file_path)  # Clean up temporary file

        # Update validator_stats for the specified epoch range
        for epoch in range(start_epoch, end_epoch + 1):
            logger.info(f"Updating validator_stats with gossip data for epoch {epoch}")
            cur.execute("""
                UPDATE validator_stats vs
                SET
                    ip = COALESCE(tgd.ip, vs.ip),
                    version = COALESCE(tgd.version, vs.version)
                FROM temp_gossip_data tgd
                WHERE
                    vs.identity_pubkey = tgd.identity_pubkey
                    AND vs.epoch = %s
            """, (epoch,))
            affected_rows = cur.rowcount
            logger.info(f"Updated {affected_rows} rows in validator_stats for epoch {epoch}")

        conn.commit()
        logger.info("Successfully updated validator_stats with gossip data")

    except Exception as e:
        logger.error(f"Error updating validator_stats with gossip data: {e}")
        conn.rollback()
    finally:
        cur.close()
        conn.close()

def fetch_validator_history(vote_pubkey):
    cmd = [VALIDATOR_HISTORY_CLI, '--json-rpc-url', RPC_URL, 'history', '--print-json', vote_pubkey]
    result = subprocess.run(cmd, capture_output=True, text=True)
    return vote_pubkey, result

def check_kobe_api_mev_data(epoch):
    """Check if Kobe API has valid MEV data for the given epoch."""
    # turns out Jito1 validator is "experimental" and not reliable.
    # url = "https://kobe.mainnet.jito.network/api/v1/validators/J1to1yufRnoWn81KYg1XkTWzmKjnYSnmE2VY8DGUJ9Qv"
    # switching to use Trillium
    url = "https://kobe.mainnet.jito.network/api/v1/validators/tri1cHBy47fPyhCvrCf6FnR7Mz6XdSoSBah2FsZVQeT"
    try:
        response = requests.get(url, timeout=10)
        if response.status_code == 200:
            data = response.json()
            if data and isinstance(data, list) and len(data) > 0:
                # Search for the specific epoch in the data list
                for entry in data:
                    if entry.get('epoch') == epoch and entry.get('mev_rewards') is not None:
                        logger.info(f"Kobe API confirmed MEV data for epoch {epoch}")
                        return True
                logger.error(f"Kobe API missing MEV data for epoch {epoch} or epoch not found")
                return False
            else:
                logger.error(f"Kobe API returned empty or invalid data for epoch {epoch}")
                return False
        else:
            logger.error(f"Kobe API request failed with status {response.status_code}: {response.text}")
            return False
    except requests.RequestException as e:
        logger.error(f"Error checking Kobe API: {str(e)}")
        return False

def fetch_and_store_data(start_epoch, end_epoch, process_validator_icons, process_stakenet, process_leader_schedule, aggregate_epoch_info):
    print("Starting fetch_and_store_data")
    logger.info(
        f"Starting fetch_and_store_data with args: "
        f"start_epoch={start_epoch}, "
        f"end_epoch={end_epoch}, "
        f"process_validator_icons={process_validator_icons}, "
        f"process_stakenet={process_stakenet}, "
        f"process_leader_schedule={process_leader_schedule}, "
        f"aggregate_epoch_info={aggregate_epoch_info}"
    )

    conn = psycopg2.connect(**db_params)
    cur = conn.cursor()
    
    cur.execute("SELECT MAX(epoch) FROM validator_data")
    current_epoch = cur.fetchone()[0]

    # Batch insert validator_info
    print("Starting validator_info insertion")
    # Make the RPC request
    payload = {
        "jsonrpc": "2.0",
        "id": 1,
        "method": "getProgramAccounts",
        "params": [
            "Config1111111111111111111111111111111111111",
            {"encoding": "jsonParsed"}
        ]
    }

    response = requests.post(RPC_URL, json=payload, headers={'Content-Type': 'application/json'})
    response.raise_for_status()
    rpc_data = response.json()

    if 'result' not in rpc_data:
        raise Exception(f"RPC request failed: {rpc_data}")

    validator_accounts = rpc_data['result']
    logger.info(f"Fetched {len(validator_accounts)} validator accounts from RPC...")

    # Transform the RPC data to match the expected format
    validator_info_data = []
    for account_data in validator_accounts:
        try:
            # Extract the parsed data
            parsed_data = account_data['account']['data']['parsed']
            
            # Skip if not a validatorInfo type
            if parsed_data.get('type') != 'validatorInfo':
                continue
                
            # Get the info section  
            parsed_info = parsed_data['info']
            config_data = parsed_info.get('configData', {})
            keys = parsed_info.get('keys', [])
            
            # Find the identity pubkey (signer=true)
            identity_pubkey = None
            for key in keys:
                if key.get('signer', False):
                    identity_pubkey = key['pubkey']
                    break
            
            # Skip if no identity pubkey found
            if not identity_pubkey:
                continue
                
            # The info pubkey is the account's pubkey
            info_pubkey = account_data['pubkey']
            
            # Create the validator info record
            validator_record = {
                'identityPubkey': identity_pubkey,
                'infoPubkey': info_pubkey,
                'info': {
                    'name': config_data.get('name'),
                    'website': config_data.get('website'),
                    'details': config_data.get('details'),
                    'keybaseUsername': config_data.get('keybaseUsername'),
                    'iconUrl': config_data.get('iconUrl')
                }
            }
            
            validator_info_data.append(validator_record)
            
        except (KeyError, TypeError) as e:
            logger.info(f"Skipping malformed account data: {e}")
            continue

    logger.info(f"Processed {len(validator_info_data)} validator info records...")

    # Deduplicate by identity_pubkey (keep the last entry)
    unique_validators = {validator['identityPubkey']: validator for validator in validator_info_data}
    values = [
        (
            validator['identityPubkey'],
            validator['infoPubkey'],
            validator['info'].get('name', None),
            validator['info'].get('website', None),
            validator['info'].get('details', None),
            validator['info'].get('keybaseUsername', None),
            validator['info'].get('iconUrl', None)
        )
        for validator in unique_validators.values()
    ]
    logger.info(f"After deduplication, importing {len(values)} unique validator info records...")

    execute_values(cur, """
        INSERT INTO validator_info (identity_pubkey, info_pubkey, name, website, details, keybase_username, icon_url)
        VALUES %s
        ON CONFLICT (identity_pubkey) DO UPDATE SET
            info_pubkey = EXCLUDED.info_pubkey,
            name = EXCLUDED.name,
            website = EXCLUDED.website,
            details = EXCLUDED.details,
            keybase_username = EXCLUDED.keybase_username,
            icon_url = EXCLUDED.icon_url
    """, values)
    conn.commit()
    print("End of validator_info insertion")

    if process_validator_icons.lower() == 'y':
        fetch_and_store_icons(conn, cur)
    else:
        logger.info("Skipping retrieval and storage of validator icons.")

    if process_stakenet == 'y':
        print("Starting process_stakenet")
        print(f"Epoch {start_epoch}")

        # Fetch vote pubkeys
        cur.execute(
            "SELECT DISTINCT vote_account_pubkey FROM validator_stats WHERE vote_account_pubkey IS NOT NULL AND epoch = %s",
            (start_epoch,)
        )
        vote_account_pubkeys = [row[0] for row in cur.fetchall()]

        # Check if specific vote_account_pubkeys are in the list
        target_pubkeys = [
            #"DzQHN1oTdN85Sbku2bc9Fu9yEwrgRMiu2XbRcntZ31yb",
            #"9xEFsHZt2mbuGQjtVGABxpeQiNeDvj54pfsJrrrUUev6",
            "gaToR246dheK1DGAMEqxMdBJZwU4qFyt7DzhSwAHFWF"
        ]

        if DEBUG:
            for pubkey in target_pubkeys:
                if pubkey in vote_account_pubkeys:
                    print(f"Pubkey {pubkey} is in the vote_account_pubkeys list.")
                else:
                    print(f"Pubkey {pubkey} is NOT in the vote_account_pubkeys list.")

        # Initialize skip_vote_pubkeys to avoid UnboundLocalError
        skip_vote_pubkeys = set()

        ## Print statement 1: Confirm pubkeys before CLI calls
        if DEBUG:
            for pubkey in target_pubkeys:
                if pubkey in vote_account_pubkeys and pubkey not in skip_vote_pubkeys:
                    print(f"Pubkey {pubkey} will be processed by fetch_validator_history.")
                else:
                    print(f"Pubkey {pubkey} will NOT be processed (in skip list or not in vote_account_pubkeys).")

        # Load skip list
        stakenet_error_list_path = "./data/configs/92_stakenet_vote_id_errors.list"
        os.makedirs("./data/configs", exist_ok=True)
        try:
            # TODO: remove this file -- errors in one epoch cause future epochs to fail -- 2025-06-04 jrh
            with open('92_stakenet_vote_id_errors.list.do-not-use', 'r') as error_file:
                skip_vote_pubkeys = set(line.strip() for line in error_file if line.strip())
            logger.info(f"Loaded {len(skip_vote_pubkeys)} vote_pubkeys to skip")
        except FileNotFoundError:
            logger.info(f"{stakenet_error_list_path} not found. Creating a new file.")
            open(stakenet_error_list_path, 'w').close()

        # Parallel CLI calls
        logger.info("Fetching validator history in parallel")
        with ThreadPoolExecutor(max_workers=16) as executor:
            results = list(executor.map(fetch_validator_history, [pk for pk in vote_account_pubkeys if pk not in skip_vote_pubkeys]))

        # Process CLI results into in-memory buffers
        csv_buffers = {}
        for vote_pubkey, result in results:
            if result.stderr:
                logger.info(f"Error for {vote_pubkey}: {result.stderr}")
                with open(stakenet_error_list_path, 'a') as error_file:
                    error_file.write(f"{vote_pubkey}\n")
                continue
                
            # Parse JSON output
            try:
                json_data = json.loads(result.stdout)
            except json.JSONDecodeError as e:
                logger.error(f"Failed to parse JSON for {vote_pubkey}: {e}")
                with open(stakenet_error_list_path, 'a') as error_file:
                    error_file.write(f"{vote_pubkey}\n")
                continue
            
            # Handle both old format (list) and new format (dict with 'epochs' key)
            if isinstance(json_data, dict) and 'epochs' in json_data:
                # New format: extract the epochs list
                epochs_list = json_data['epochs']
            elif isinstance(json_data, list):
                # Old format: already a list
                epochs_list = json_data
            else:
                logger.error(f"Unexpected JSON format for {vote_pubkey}: expected list or dict with 'epochs' key, got {type(json_data).__name__}")
                logger.debug(f"JSON data: {json_data}")
                with open(stakenet_error_list_path, 'a') as error_file:
                    error_file.write(f"{vote_pubkey}\n")
                continue
            
            csv_buffer = StringIO()
            csv_buffer.write("vote_account_pubkey,epoch,commission,epoch_credits,mev_commission,mev_earned,stake,jito_rank,superminority,ip,client_type,client_version\n")
            
            # Find data for the target epoch
            epoch_found = False
            for epoch_data in epochs_list:
                # Ensure epoch_data is a dictionary
                if not isinstance(epoch_data, dict):
                    logger.warning(f"Skipping non-dict epoch_data for {vote_pubkey}: {type(epoch_data).__name__}")
                    continue
                
                # Handle both old and new format
                if 'validator_history' in epoch_data:
                    # New format with nested validator_history
                    epoch_num = epoch_data.get('epoch')
                    validator_history = epoch_data.get('validator_history', {})
                else:
                    # Old format - flat structure
                    epoch_num = epoch_data.get('epoch')
                    validator_history = epoch_data
                
                if epoch_num == start_epoch:
                    epoch_found = True
                    
                    # Extract values and handle None/null values
                    commission = validator_history.get('commission') or '[NULL]'
                    epoch_credits = validator_history.get('epoch_credits') or '[NULL]'
                    mev_commission = validator_history.get('mev_commission') or '[NULL]'
                    
                    # Handle mev_earned - convert from SOL to lamports if it's a float
                    mev_earned_raw = validator_history.get('mev_earned')
                    if mev_earned_raw is not None:
                        try:
                            mev_earned = str(int(float(mev_earned_raw) * 1_000_000_000))
                        except (ValueError, TypeError):
                            mev_earned = '[NULL]'
                    else:
                        mev_earned = '[NULL]'
                    
                    # Handle activated_stake (new format uses activated_stake_lamports)
                    activated_stake = validator_history.get('activated_stake') or validator_history.get('activated_stake_lamports') or '[NULL]'
                    
                    # Handle jito_rank (new format uses 'rank')
                    jito_rank = validator_history.get('jito_rank') or validator_history.get('rank') or '[NULL]'
                    
                    # Handle superminority (new format uses is_superminority)
                    if 'superminority' in validator_history:
                        superminority = 1 if validator_history.get('superminority') else 0
                    elif 'is_superminority' in validator_history:
                        # New format stores it as a string "0" or "1"
                        is_super = validator_history.get('is_superminority')
                        if is_super is not None:
                            superminority = int(is_super)
                        else:
                            superminority = 0
                    else:
                        superminority = 0
                    
                    ip = validator_history.get('ip') or '[NULL]'
                    client_type = validator_history.get('client_type') or '[NULL]'
                    
                    # Handle client_version (new format uses 'version')
                    client_version = validator_history.get('client_version') or validator_history.get('version') or '[NULL]'
                    
                    # Convert values to strings and handle None values properly
                    commission = str(commission) if commission != '[NULL]' else '[NULL]'
                    epoch_credits = str(epoch_credits) if epoch_credits != '[NULL]' else '[NULL]'
                    mev_commission = str(mev_commission) if mev_commission != '[NULL]' else '[NULL]'
                    activated_stake = str(activated_stake) if activated_stake != '[NULL]' else '[NULL]'
                    jito_rank = str(jito_rank) if jito_rank != '[NULL]' else '[NULL]'
                    client_type = str(client_type) if client_type != '[NULL]' else '[NULL]'
                    ip = str(ip) if ip != '[NULL]' else '[NULL]'
                    client_version = str(client_version) if client_version != '[NULL]' else '[NULL]'
                    
                    # Print statement 2: Log activated_stake after parsing CLI output
                    if DEBUG:
                        if vote_pubkey in target_pubkeys:
                            print(f"Pubkey {vote_pubkey}, Epoch {start_epoch}: activated_stake={activated_stake}, mev_earned={mev_earned}")
                    
                    csv_buffer.write(f"{vote_pubkey},{start_epoch},{commission},{epoch_credits},{mev_commission},{mev_earned},{activated_stake},{jito_rank},{superminority},{ip},{client_type},{client_version}\n")
                    break
            
            if not epoch_found:
                logger.info(f"No data found for {vote_pubkey} in epoch {start_epoch}")
                
            csv_buffer.seek(0)
            csv_buffers[vote_pubkey] = csv_buffer

        # Print statement 3: Check CSV buffer content for target pubkeys
        for pubkey in target_pubkeys:
            if pubkey in csv_buffers:
                csv_buffer = csv_buffers[pubkey]
                csv_buffer.seek(0)
                lines = csv_buffer.readlines()[1:]  # Skip header
                for line in lines:
                    fields = line.strip().split(',')
                    if len(fields) > 6:  # Ensure activated_stake field exists
                        if DEBUG:
                            print(f"CSV buffer for {pubkey}: activated_stake={fields[6]}")
                csv_buffer.seek(0)
            else:
                if DEBUG:
                    print(f"No CSV buffer created for {pubkey}.")

        # Check Kobe API for MEV data availability
        jito_data = {}
        if check_kobe_api_mev_data(start_epoch):
            logger.info(f"Fetching Jito data for epoch {start_epoch}")
            response = requests.post(
                "https://kobe.mainnet.jito.network/api/v1/validators",
                headers={"Content-Type": "application/json"},
                json={"epoch": start_epoch}
            )
            if response.status_code == 200:
                logger.info(f"Response: 200 -- Got Jito Kobe API data for epoch {start_epoch}")
                jito_data = {item['vote_account']: item for item in response.json()['validators']}
                logger.info(f"Jito data sample: {list(jito_data.values())[:5]}")
            else:
                logger.error(f"Failed to fetch Jito Kobe API data: {response.text}")
        else:
            logger.error(f"Using fetch_validator_history data for epoch {start_epoch} due to missing Kobe API MEV data")

        # Update buffers with Jito data if available
        for vote_pubkey, csv_buffer in csv_buffers.items():
            if vote_pubkey in jito_data:
                validator_data = jito_data[vote_pubkey]
                csv_buffer.seek(0)
                lines = csv_buffer.readlines()
                csv_buffer.seek(0)
                csv_buffer.truncate()
                csv_buffer.write(lines[0])  # Header
                for line in lines[1:]:
                    parts = line.strip().split(',')
                    parts[4] = str(validator_data['mev_commission_bps'])
                    parts[5] = str(validator_data['mev_rewards'])
                    csv_buffer.write(','.join(parts) + '\n')
                csv_buffer.seek(0)
                # Print statement 4: Check updated CSV buffer for target pubkeys
                if vote_pubkey in target_pubkeys:
                    csv_buffer.seek(0)
                    updated_lines = csv_buffer.readlines()[1:]  # Skip header
                    for updated_line in updated_lines:
                        fields = updated_line.strip().split(',')
                        if len(fields) > 6:
                            if DEBUG:
                                print(f"Updated CSV buffer for {vote_pubkey}: activated_stake={fields[6]}, mev_earned={fields[5]}")
                    csv_buffer.seek(0)

        # Load all data into one temp table
        cur.execute("DROP TABLE IF EXISTS temp_validator_stats;")
        cur.execute("""
            CREATE TEMPORARY TABLE temp_validator_stats (
                vote_account_pubkey TEXT, epoch INTEGER, commission TEXT, epoch_credits TEXT,
                mev_commission TEXT, mev_earned TEXT, activated_stake TEXT, jito_rank TEXT,
                superminority TEXT, ip TEXT, client_type TEXT, version TEXT
            );
        """)
        for vote_pubkey, csv_buffer in csv_buffers.items():
            cur.copy_expert("COPY temp_validator_stats FROM STDIN WITH CSV HEADER", csv_buffer)
            csv_buffer.close()

        # Print statement 5: Query temp_validator_stats for target pubkeys
        for pubkey in target_pubkeys:
            cur.execute(
                "SELECT vote_account_pubkey, activated_stake, mev_earned FROM temp_validator_stats WHERE vote_account_pubkey = %s AND epoch = %s",
                (pubkey, start_epoch)
            )
            row = cur.fetchone()
            if row:
                if DEBUG:
                    print(f"temp_validator_stats for {pubkey}: activated_stake={row[1]}, mev_earned={row[2]}")
            else:
                if DEBUG:
                    print(f"No data in temp_validator_stats for {pubkey} in epoch {start_epoch}.")
        cur.execute("SELECT * FROM temp_validator_stats WHERE commission = '[NULL]' OR epoch_credits = '[NULL]' OR mev_commission = '[NULL]' OR jito_rank = '[NULL]' OR superminority = '[NULL]' OR client_type = '[NULL]' LIMIT 1;")
        problematic_row = cur.fetchone()
        if problematic_row:
            logger.info(f"Found problematic row: {problematic_row}")

        # Update validator_stats
        cur.execute("""
            UPDATE validator_stats vs
            SET
                vote_account_pubkey = tvs.vote_account_pubkey,
                activated_stake = CAST(NULLIF(NULLIF(NULLIF(tvs.activated_stake, 'None'), '[NULL]'), '') AS BIGINT),
                commission = CAST(NULLIF(NULLIF(tvs.commission, '[NULL]'), '') AS INTEGER),
                epoch_credits = CAST(NULLIF(NULLIF(tvs.epoch_credits, '[NULL]'), '') AS BIGINT),
                mev_commission = CAST(NULLIF(NULLIF(tvs.mev_commission, '[NULL]'), '') AS INTEGER),
                mev_earned = CAST(NULLIF(NULLIF(tvs.mev_earned, '[NULL]'), '') AS DOUBLE PRECISION),
                jito_rank = CAST(NULLIF(NULLIF(tvs.jito_rank, '[NULL]'), '') AS INTEGER),
                ip = tvs.ip,
                client_type = CAST(NULLIF(NULLIF(tvs.client_type, '[NULL]'), '') AS INTEGER),
                version = tvs.version,
                superminority = CAST(NULLIF(NULLIF(tvs.superminority, '[NULL]'), '') AS INTEGER)
            FROM temp_validator_stats tvs
            WHERE vs.identity_pubkey = (SELECT identity_pubkey FROM validator_stats WHERE vote_account_pubkey = tvs.vote_account_pubkey AND epoch = %s LIMIT 1)
            AND vs.epoch = %s
        """, (start_epoch, start_epoch))

        # Print statement 6: Query validator_stats after update for target pubkeys
        for pubkey in target_pubkeys:
            cur.execute(
                "SELECT vote_account_pubkey, activated_stake, mev_earned FROM validator_stats WHERE vote_account_pubkey = %s AND epoch = %s",
                (pubkey, start_epoch)
            )
            row = cur.fetchone()
            if row:
                if DEBUG:
                    print(f"validator_stats for {pubkey}: activated_stake={row[1]}, mev_earned={row[2]}")
            else:
                if DEBUG:
                    print(f"No data in validator_stats for {pubkey} in epoch {start_epoch}.")

        # Calculate MEV splits
        cur.execute("""
            UPDATE validator_stats vs
            SET 
                mev_to_jito_block_engine = CASE 
                    WHEN epoch < 752 THEN COALESCE(vs.mev_earned, 0) * 0.05 / 0.95
                    WHEN epoch >= 752 THEN COALESCE(vs.mev_earned, 0) * 0.03 / 0.97
                END,
                mev_to_jito_tip_router = CASE
                    WHEN epoch >= 752 THEN COALESCE(vs.mev_earned, 0) * 0.03 / 0.97
                    ELSE NULL
                END,
                mev_to_validator = CASE
                    WHEN epoch < 752 THEN COALESCE(vs.mev_earned, 0) * COALESCE(vs.mev_commission, 0) / 10000
                    WHEN epoch >= 752 THEN (COALESCE(vs.mev_earned, 0) - (COALESCE(vs.mev_earned, 0) * 0.03 / 0.97)) * COALESCE(vs.mev_commission, 0) / 10000
                END,
                mev_to_stakers = CASE
                    WHEN epoch < 752 THEN COALESCE(vs.mev_earned, 0) - (COALESCE(vs.mev_earned, 0) * COALESCE(vs.mev_commission, 0) / 10000)
                    WHEN epoch >= 752 THEN (COALESCE(vs.mev_earned, 0) - (COALESCE(vs.mev_earned, 0) * 0.03 / 0.97)) - ((COALESCE(vs.mev_earned, 0) - (COALESCE(vs.mev_earned, 0) * 0.03 / 0.97)) * COALESCE(vs.mev_commission, 0) / 10000)
                END
            WHERE epoch = %s
        """, (start_epoch,))
        conn.commit()

        # Gossip update
        logger.info("Fetching and processing gossip data")
        fetch_and_store_gossip_data(RPC_URL, "92_gossip.json")
        update_validator_stats_with_gossip(db_params, start_epoch, start_epoch, "92_gossip.json")
    else:
        logger.info("Skipping retrieval and storage of stakenet.")

    print("End of process_stakenet")

    if aggregate_epoch_info == 'y':
        print(f"Aggregating data for epoch {start_epoch}")
        logger.info(f"Aggregating data for epoch {start_epoch}")
        if process_leader_schedule == 'y':
            update_validator_skip_rates(conn, start_epoch)
        
        # Direct SQL for validator_stats
        cur.execute("""
            WITH validator_rewards AS (
                SELECT
                    identity_pubkey, epoch,
                    SUM(COALESCE(rewards, 0)) AS rewards_total,
                    AVG(COALESCE(rewards, 0)) AS rewards_average,
                    COUNT(*) AS blocks_produced,
                    SUM(total_user_tx) AS user_tx_included_in_blocks,
                    SUM(total_vote_tx) AS vote_tx_included_in_blocks,
                    SUM(total_cu) AS cu,
                    SUM(total_tx) AS tx_included_in_blocks,
                    SUM(total_signatures) AS signatures,
                    SUM(total_fees) AS total_block_rewards_before_burn,
                    SUM(total_validator_signature_fees) AS validator_signature_fees,
                    SUM(total_validator_priority_fees) AS validator_priority_fees,
                    SUM(total_validator_fees) AS total_block_rewards_after_burn
                FROM validator_data
                WHERE epoch = %s AND reward_type = 'Fee'
                GROUP BY identity_pubkey, epoch
            ),
            epoch_total_stake AS (
                SELECT SUM(activated_stake) AS total_stake
                FROM validator_stats
                WHERE epoch = %s
            )
            INSERT INTO validator_stats (
                identity_pubkey, epoch, rewards, avg_rewards_per_block, blocks_produced,
                user_tx_included_in_blocks, vote_tx_included_in_blocks, cu, tx_included_in_blocks,
                signatures, total_block_rewards_before_burn, validator_signature_fees,
                validator_priority_fees, total_block_rewards_after_burn, avg_mev_per_block,
                mev_to_validator, mev_to_jito_block_engine, mev_to_jito_tip_router,
                mev_to_stakers, avg_cu_per_block, avg_user_tx_per_block, avg_vote_tx_per_block,
                avg_priority_fees_per_block, avg_signature_fees_per_block, avg_tx_per_block,
                vote_cost, stake_percentage
            )
            SELECT
                vr.identity_pubkey, vr.epoch, vr.rewards_total,
                CASE WHEN vr.blocks_produced > 0 THEN vr.rewards_total / vr.blocks_produced ELSE 0 END,
                vr.blocks_produced, vr.user_tx_included_in_blocks, vr.vote_tx_included_in_blocks,
                vr.cu, vr.tx_included_in_blocks, vr.signatures, vr.total_block_rewards_before_burn,
                vr.validator_signature_fees, vr.validator_priority_fees, vr.total_block_rewards_after_burn,
                CASE WHEN vr.blocks_produced > 0 THEN COALESCE(vs.mev_earned, 0) / vr.blocks_produced ELSE 0 END,
                CASE
                    WHEN vr.epoch < 752 THEN COALESCE(vs.mev_earned, 0) * COALESCE(vs.mev_commission, 0) / 10000
                    WHEN vr.epoch >= 752 THEN (COALESCE(vs.mev_earned, 0) - (COALESCE(vs.mev_earned, 0) * 0.03 / 0.97)) * COALESCE(vs.mev_commission, 0) / 10000
                END,  -- mev_to_validator
                CASE 
                    WHEN vr.epoch < 752 THEN COALESCE(vs.mev_earned, 0) * 0.05 / 0.95 
                    WHEN vr.epoch >= 752 THEN COALESCE(vs.mev_earned, 0) * 0.03 / 0.97 
                END,  -- mev_to_jito_block_engine
                CASE 
                    WHEN vr.epoch >= 752 THEN COALESCE(vs.mev_earned, 0) * 0.03 / 0.97 
                    ELSE NULL 
                END,  -- mev_to_jito_tip_router
                CASE
                    WHEN vr.epoch < 752 THEN COALESCE(vs.mev_earned, 0) - (COALESCE(vs.mev_earned, 0) * COALESCE(vs.mev_commission, 0) / 10000)
                    WHEN vr.epoch >= 752 THEN (COALESCE(vs.mev_earned, 0) - (COALESCE(vs.mev_earned, 0) * 0.03 / 0.97)) - ((COALESCE(vs.mev_earned, 0) - (COALESCE(vs.mev_earned, 0) * 0.03 / 0.97)) * COALESCE(vs.mev_commission, 0) / 10000)
                END,  -- mev_to_stakers
                CASE WHEN vr.blocks_produced > 0 THEN vr.cu / vr.blocks_produced ELSE 0 END,
                CASE WHEN vr.blocks_produced > 0 THEN vr.user_tx_included_in_blocks / vr.blocks_produced ELSE 0 END,
                CASE WHEN vr.blocks_produced > 0 THEN vr.vote_tx_included_in_blocks / vr.blocks_produced ELSE 0 END,
                CASE WHEN vr.blocks_produced > 0 THEN vr.validator_priority_fees / vr.blocks_produced ELSE 0 END,
                CASE WHEN vr.blocks_produced > 0 THEN vr.validator_signature_fees / vr.blocks_produced ELSE 0 END,
                vr.tx_included_in_blocks / NULLIF(vr.blocks_produced, 0),
                COALESCE(vs.votes_cast, 0) * 5000,
                (COALESCE(vs.activated_stake, 0)::float / NULLIF(ets.total_stake, 0)) * 100
            FROM validator_rewards AS vr
            CROSS JOIN epoch_total_stake AS ets
            LEFT JOIN validator_stats AS vs ON vr.identity_pubkey = vs.identity_pubkey AND vr.epoch = vs.epoch
            ON CONFLICT (identity_pubkey, epoch) DO UPDATE SET
                rewards = EXCLUDED.rewards, avg_rewards_per_block = EXCLUDED.avg_rewards_per_block,
                blocks_produced = EXCLUDED.blocks_produced, user_tx_included_in_blocks = EXCLUDED.user_tx_included_in_blocks,
                vote_tx_included_in_blocks = EXCLUDED.vote_tx_included_in_blocks, cu = EXCLUDED.cu,
                tx_included_in_blocks = EXCLUDED.tx_included_in_blocks, signatures = EXCLUDED.signatures,
                total_block_rewards_before_burn = EXCLUDED.total_block_rewards_before_burn,
                validator_signature_fees = EXCLUDED.validator_signature_fees,
                validator_priority_fees = EXCLUDED.validator_priority_fees,
                total_block_rewards_after_burn = EXCLUDED.total_block_rewards_after_burn,
                avg_mev_per_block = EXCLUDED.avg_mev_per_block, mev_to_validator = EXCLUDED.mev_to_validator,
                mev_to_jito_block_engine = EXCLUDED.mev_to_jito_block_engine,
                mev_to_jito_tip_router = EXCLUDED.mev_to_jito_tip_router,
                mev_to_stakers = EXCLUDED.mev_to_stakers,
                avg_cu_per_block = EXCLUDED.avg_cu_per_block, avg_user_tx_per_block = EXCLUDED.avg_user_tx_per_block,
                avg_vote_tx_per_block = EXCLUDED.avg_vote_tx_per_block,
                avg_priority_fees_per_block = EXCLUDED.avg_priority_fees_per_block,
                avg_signature_fees_per_block = EXCLUDED.avg_signature_fees_per_block,
                avg_tx_per_block = EXCLUDED.avg_tx_per_block, vote_cost = EXCLUDED.vote_cost,
                stake_percentage = EXCLUDED.stake_percentage;
        """, (start_epoch, start_epoch))

        # Direct SQL for epoch_aggregate_data
        cur.execute(f"""
            WITH epoch_data AS (
                SELECT epoch, (SUM(leader_slots::numeric * activated_stake) / NULLIF(SUM(activated_stake::numeric), 0))::bigint AS avg_stake_weighted_leader_slots
                FROM validator_stats WHERE epoch = {start_epoch} GROUP BY epoch
            ),
            main_aggregates AS (
                SELECT
                    AVG(commission) AS avg_commission, AVG(epoch_credits) AS avg_credits,
                    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY epoch_credits) AS median_credits,
                    SUM(cu)::numeric / NULLIF(SUM(blocks_produced), 0) AS avg_cu_per_block,
                    AVG(mev_commission) AS avg_mev_commission,
                    SUM(mev_earned)::numeric / NULLIF(SUM(blocks_produced), 0) AS avg_mev_per_block,
                    AVG(mev_to_validator) AS avg_mev_to_validator,
                    AVG(mev_to_jito_block_engine) AS avg_mev_to_jito_block_engine,
                    AVG(mev_to_jito_tip_router) AS avg_mev_to_jito_tip_router,
                    SUM(validator_priority_fees)::numeric / NULLIF(SUM(blocks_produced), 0) AS avg_priority_fees_per_block,
                    SUM(rewards)::numeric / NULLIF(SUM(blocks_produced), 0) AS avg_rewards_per_block,
                    SUM(validator_signature_fees)::numeric / NULLIF(SUM(blocks_produced), 0) AS avg_signature_fees_per_block,
                    SUM(user_tx_included_in_blocks)::numeric / NULLIF(SUM(blocks_produced), 0) AS avg_user_tx_per_block,
                    SUM(vote_cost)::numeric / NULLIF(SUM(blocks_produced), 0) AS avg_vote_cost_per_block,
                    SUM(vote_tx_included_in_blocks)::numeric / NULLIF(SUM(blocks_produced), 0) AS avg_vote_tx_per_block,
                    SUM(tx_included_in_blocks)::numeric / NULLIF(SUM(blocks_produced), 0) AS avg_tx_per_block,
                    AVG(votes_cast)::numeric AS avg_votes_cast,
                    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY CASE WHEN blocks_produced > 0 THEN rewards::numeric / blocks_produced ELSE NULL END) AS median_rewards_per_block,
                    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY CASE WHEN blocks_produced > 0 THEN mev_earned::numeric / blocks_produced ELSE NULL END) AS median_mev_per_block,
                    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY CASE WHEN blocks_produced > 0 THEN mev_to_jito_block_engine::numeric / blocks_produced ELSE NULL END) AS median_mev_to_jito_block_engine,
                    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY CASE WHEN blocks_produced > 0 THEN mev_to_jito_tip_router::numeric / blocks_produced ELSE NULL END) AS median_mev_to_jito_tip_router,
                    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY votes_cast) AS median_votes_cast,
                    SUM(activated_stake) AS total_active_stake, SUM(blocks_produced) AS total_blocks_produced,
                    SUM(epoch_credits) AS total_credits, SUM(cu) AS total_cu, SUM(mev_earned) AS total_mev_earned,
                    SUM(mev_to_validator) as total_mev_to_validator, 
                    SUM(mev_to_jito_block_engine) as total_mev_to_jito_block_engine, 
                    SUM(mev_to_jito_tip_router) as total_mev_to_jito_tip_router, 
                    SUM(mev_to_stakers) as total_mev_to_stakers, 
                    SUM(signatures) AS total_signatures, SUM(tx_included_in_blocks) AS total_tx,
                    SUM(user_tx_included_in_blocks) AS total_user_tx, SUM(total_block_rewards_after_burn) AS total_validator_fees,
                    SUM(validator_priority_fees) AS total_validator_priority_fees,
                    SUM(validator_signature_fees) AS total_validator_signature_fees,
                    SUM(vote_cost) AS total_vote_cost, SUM(vote_tx_included_in_blocks) AS total_vote_tx,
                    SUM(votes_cast) AS total_votes_cast,
                    SUM(skip_rate * activated_stake) / NULLIF(SUM(activated_stake), 0) AS avg_stake_weighted_skip_rate,
                    AVG(CASE WHEN activated_stake > 0 AND leader_slots > 0 THEN (activated_stake::float / 1000000000) / (leader_slots::float / 4) ELSE NULL END) AS average_sol_per_4_slots,
                    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY CASE WHEN activated_stake > 0 AND leader_slots > 0 THEN (activated_stake::float / 1000000000) / (leader_slots::float / 4) ELSE NULL END) AS median_sol_per_4_slots,
                    AVG(activated_stake)::numeric AS avg_active_stake,
                    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY activated_stake) AS median_active_stake,
                    COUNT(*) FILTER (WHERE leader_slots > 0) AS total_active_validators
                FROM validator_stats vs
                WHERE vs.epoch = {start_epoch}
            ),
            slot_time_stats AS (
                SELECT MIN(block_slot) AS min_slot, MAX(block_slot) AS max_slot,
                    MIN(block_time) AS min_block_time, MAX(block_time) AS max_block_time
                FROM validator_data WHERE epoch = {start_epoch}
            ),
            epoch_slot_info AS (
                SELECT {start_epoch} * 432000 AS epoch_start_slot,
                    ({start_epoch} + 1) * 432000 - 1 AS epoch_end_slot
            )
            INSERT INTO epoch_aggregate_data (
                avg_commission, avg_credits, median_credits, avg_cu_per_block, avg_mev_commission,
                avg_mev_per_block, avg_mev_to_validator, avg_mev_to_jito_block_engine, avg_mev_to_jito_tip_router,
                avg_priority_fees_per_block, avg_rewards_per_block, avg_signature_fees_per_block,
                avg_stake_weighted_skip_rate, avg_stake_weighted_leader_slots, avg_user_tx_per_block,
                avg_vote_cost_per_block, avg_vote_tx_per_block, avg_tx_per_block, avg_votes_cast,
                epoch, epoch_start_slot, epoch_end_slot, median_rewards_per_block, median_mev_per_block,
                median_mev_to_jito_block_engine, median_mev_to_jito_tip_router, median_votes_cast,
                total_active_stake, total_blocks_produced, total_credits, total_cu, total_mev_earned,
                total_mev_to_validator, total_mev_to_jito_block_engine, total_mev_to_jito_tip_router,total_mev_to_stakers,
                total_signatures, total_tx, total_user_tx, total_validator_fees, total_validator_priority_fees,
                total_validator_signature_fees, total_vote_cost, total_vote_tx, total_votes_cast,
                min_slot, max_slot, min_block_time, max_block_time, average_sol_per_4_slots,
                median_sol_per_4_slots, avg_active_stake, median_active_stake, total_active_validators
            )
            SELECT
                ma.avg_commission, ma.avg_credits, ma.median_credits, ma.avg_cu_per_block, ma.avg_mev_commission,
                ma.avg_mev_per_block, ma.avg_mev_to_validator, ma.avg_mev_to_jito_block_engine, ma.avg_mev_to_jito_tip_router,
                ma.avg_priority_fees_per_block, ma.avg_rewards_per_block, ma.avg_signature_fees_per_block,
                ma.avg_stake_weighted_skip_rate, ed.avg_stake_weighted_leader_slots, ma.avg_user_tx_per_block,
                ma.avg_vote_cost_per_block, ma.avg_vote_tx_per_block, ma.avg_tx_per_block, ma.avg_votes_cast,
                {start_epoch} AS epoch, esi.epoch_start_slot, esi.epoch_end_slot, ma.median_rewards_per_block,
                ma.median_mev_per_block, ma.median_mev_to_jito_block_engine, ma.median_mev_to_jito_tip_router,
                ma.median_votes_cast, ma.total_active_stake, ma.total_blocks_produced, ma.total_credits,
                ma.total_cu, ma.total_mev_earned, ma.total_mev_to_validator, ma.total_mev_to_jito_block_engine,
                ma.total_mev_to_jito_tip_router, ma.total_mev_to_stakers, ma.total_signatures, ma.total_tx, ma.total_user_tx,
                ma.total_validator_fees, ma.total_validator_priority_fees, ma.total_validator_signature_fees,
                ma.total_vote_cost, ma.total_vote_tx, ma.total_votes_cast, sts.min_slot, sts.max_slot,
                sts.min_block_time, sts.max_block_time, ma.average_sol_per_4_slots, ma.median_sol_per_4_slots,
                ma.avg_active_stake, ma.median_active_stake, ma.total_active_validators
            FROM main_aggregates ma, slot_time_stats sts, epoch_slot_info esi, epoch_data ed
            ON CONFLICT (epoch) DO UPDATE SET
                avg_commission = EXCLUDED.avg_commission, avg_credits = EXCLUDED.avg_credits,
                median_credits = EXCLUDED.median_credits, avg_cu_per_block = EXCLUDED.avg_cu_per_block,
                avg_mev_commission = EXCLUDED.avg_mev_commission, avg_mev_per_block = EXCLUDED.avg_mev_per_block,
                avg_mev_to_validator = EXCLUDED.avg_mev_to_validator,
                avg_mev_to_jito_block_engine = EXCLUDED.avg_mev_to_jito_block_engine,
                avg_mev_to_jito_tip_router = EXCLUDED.avg_mev_to_jito_tip_router,
                avg_priority_fees_per_block = EXCLUDED.avg_priority_fees_per_block,
                avg_rewards_per_block = EXCLUDED.avg_rewards_per_block,
                avg_signature_fees_per_block = EXCLUDED.avg_signature_fees_per_block,
                avg_stake_weighted_skip_rate = EXCLUDED.avg_stake_weighted_skip_rate,
                avg_stake_weighted_leader_slots = EXCLUDED.avg_stake_weighted_leader_slots,
                avg_user_tx_per_block = EXCLUDED.avg_user_tx_per_block,
                avg_vote_cost_per_block = EXCLUDED.avg_vote_cost_per_block,
                avg_vote_tx_per_block = EXCLUDED.avg_vote_tx_per_block,
                avg_votes_cast = EXCLUDED.avg_votes_cast, avg_tx_per_block = EXCLUDED.avg_tx_per_block,
                median_rewards_per_block = EXCLUDED.median_rewards_per_block,
                median_mev_per_block = EXCLUDED.median_mev_per_block,
                median_mev_to_jito_block_engine = EXCLUDED.median_mev_to_jito_block_engine,
                median_mev_to_jito_tip_router = EXCLUDED.median_mev_to_jito_tip_router,
                median_votes_cast = EXCLUDED.median_votes_cast,
                total_active_stake = EXCLUDED.total_active_stake, total_blocks_produced = EXCLUDED.total_blocks_produced,
                total_credits = EXCLUDED.total_credits, total_cu = EXCLUDED.total_cu,
                total_mev_earned = EXCLUDED.total_mev_earned, total_mev_to_validator = EXCLUDED.total_mev_to_validator,
                total_mev_to_jito_block_engine = EXCLUDED.total_mev_to_jito_block_engine,
                total_mev_to_jito_tip_router = EXCLUDED.total_mev_to_jito_tip_router, total_mev_to_stakers = EXCLUDED.total_mev_to_stakers,
                total_signatures = EXCLUDED.total_signatures, total_tx = EXCLUDED.total_tx,
                total_user_tx = EXCLUDED.total_user_tx, total_validator_fees = EXCLUDED.total_validator_fees,
                total_validator_priority_fees = EXCLUDED.total_validator_priority_fees,
                total_validator_signature_fees = EXCLUDED.total_validator_signature_fees,
                total_vote_cost = EXCLUDED.total_vote_cost, total_vote_tx = EXCLUDED.total_vote_tx,
                total_votes_cast = EXCLUDED.total_votes_cast, epoch_start_slot = EXCLUDED.epoch_start_slot,
                epoch_end_slot = EXCLUDED.epoch_end_slot, min_slot = EXCLUDED.min_slot,
                max_slot = EXCLUDED.max_slot, min_block_time = EXCLUDED.min_block_time,
                max_block_time = EXCLUDED.max_block_time, average_sol_per_4_slots = EXCLUDED.average_sol_per_4_slots,
                median_sol_per_4_slots = EXCLUDED.median_sol_per_4_slots,
                avg_active_stake = EXCLUDED.avg_active_stake, median_active_stake = EXCLUDED.median_active_stake,
                total_active_validators = EXCLUDED.total_active_validators
        """)
        conn.commit()
        
        # Remaining cleanup steps (optimized to remove subprocesses)
        logger.info("Recreating validator_data_to_inspect table")
        cur.execute("""
            DROP TABLE IF EXISTS validator_data_to_inspect;
            CREATE TABLE validator_data_to_inspect (LIKE validator_data INCLUDING ALL);
        """)
        
        logger.info("Saving and deleting rows from validator_data where identity_pubkey matches vote_account_pubkey")
        # First query - Insert into validator_data_to_inspect
        cur.execute("""
            WITH vote_account_list AS (SELECT DISTINCT vote_account_pubkey FROM validator_stats)
            INSERT INTO validator_data_to_inspect
            SELECT vd.* FROM validator_data vd 
            JOIN vote_account_list val ON vd.identity_pubkey = val.vote_account_pubkey
        """)

        # Second query - Delete from validator_data where we have vote pubkey in identity pubkey field
        cur.execute("""
            WITH vote_account_list AS (SELECT DISTINCT vote_account_pubkey FROM validator_stats)
            DELETE FROM validator_data 
            WHERE identity_pubkey IN (SELECT vote_account_pubkey FROM vote_account_list)
        """)
        
        logger.info("Recreating validator_stats_to_inspect table")
        cur.execute("""
            DROP TABLE IF EXISTS validator_stats_to_inspect;
            CREATE TABLE validator_stats_to_inspect (LIKE validator_stats INCLUDING ALL);
        """)
        
        logger.info("Saving and deleting rows from validator_stats where identity_pubkey matches vote_account_pubkey")
        # First query - Insert into validator_stats_to_inspect
        cur.execute("""
            WITH vote_account_list AS (SELECT DISTINCT vote_account_pubkey FROM validator_stats)
            INSERT INTO validator_stats_to_inspect
            SELECT vs.* FROM validator_stats vs JOIN vote_account_list val ON vs.identity_pubkey = val.vote_account_pubkey
        """)

        # Second query - Delete from validator_stats where we have identity pubkey in vote pubkey field
        cur.execute("""
            WITH vote_account_list AS (SELECT DISTINCT vote_account_pubkey FROM validator_stats)
            DELETE FROM validator_stats WHERE identity_pubkey IN (SELECT vote_account_pubkey FROM vote_account_list)
        """)
        
        logger.info("Deleting system program rows from validator_stats")
        cur.execute("""
            DELETE FROM validator_stats
            WHERE vote_account_pubkey LIKE '%1111111111%'
            OR identity_pubkey LIKE '%1111111111%';
        """)
        conn.commit()
    else:
        logger.info("Skipping epoch info aggregation.")

    # GeoIP processing is now handled separately by 92_ip_api.py
    logger.info("Skipping geoip info update - now handled by 92_ip_api.py")

    cur.close()
    conn.close()

def update_elapsed_time_per_epoch():
    try:
        # Establish connection
        conn = psycopg2.connect(**db_params)
        cursor = conn.cursor()
        
        # Update elapsed_time_per_epoch for all rows
        cursor.execute("""
            UPDATE epoch_aggregate_data
            SET elapsed_time_per_epoch = (
                ((max_block_time - min_block_time)::NUMERIC / NULLIF((max_slot - min_slot), 0)) * 432000
            );
        """)
        
        # Commit the transaction
        conn.commit()
        
        # Close the cursor and connection
        cursor.close()
        conn.close()
        
        logger.info("Updated elapsed_time_per_epoch for all rows successfully.")
    
    except Exception as error:
        logger.error(f"Error updating elapsed_time_per_epoch: {error}")
        if conn:
            conn.close()

def calculate_and_update_epochs_per_year():
    weights = [Decimal('0.2649'), Decimal('0.1987'), Decimal('0.1490'), Decimal('0.1118'), Decimal('0.0838'), 
               Decimal('0.0629'), Decimal('0.0471'), Decimal('0.0354'), Decimal('0.0265'), Decimal('0.0199')] 

    try:
        # Establish connection
        conn = psycopg2.connect(**db_params)
        cursor = conn.cursor()
        
        # Fetch all current_epoch values
        cursor.execute("SELECT epoch FROM epoch_aggregate_data;")
        epochs = cursor.fetchall()
        
        for epoch_row in epochs:
            current_epoch = epoch_row[0]
            epochs_per_year = Decimal('0')
            weight_sum = Decimal('0')            
            for i in range(10):
                if current_epoch - i > 0:
                    cursor.execute("""
                        SELECT (365 * 24 * 60 * 60) / NULLIF(elapsed_time_per_epoch, 0)
                        FROM epoch_aggregate_data
                        WHERE epoch = %s;
                    """, (current_epoch - i,))
                    
                    temp_epochs_per_year = cursor.fetchone()
                    
                    if temp_epochs_per_year and temp_epochs_per_year[0] is not None:
                        temp_epochs_per_year = Decimal(str(temp_epochs_per_year[0]))
                        epochs_per_year += temp_epochs_per_year * weights[i]
                        weight_sum += weights[i]
            
            # Normalize the result based on the sum of weights used
            if weight_sum > 0:
                epochs_per_year /= weight_sum
            
            # Update epochs_per_year for the current epoch
            cursor.execute("""
                UPDATE epoch_aggregate_data
                SET epochs_per_year = %s
                WHERE epoch = %s;
            """, (round(float(epochs_per_year), 2), current_epoch))
        
        # Commit the transaction
        conn.commit()
        
        # Close the cursor and connection
        cursor.close()
        conn.close()
        
        logger.info("Calculated and updated epochs_per_year for all rows successfully.")
    
    except Exception as error:
        logger.error(f"Error calculating and updating epochs_per_year: {error}")
        if conn:
            conn.close()


def get_country_region_map():
    global country_region_map
    try:
        logger.info("Fetching country-region mapping data...")
        response = requests.get("https://trillium.so/pages/country-region.json")
        response.raise_for_status()
        country_region_map = {item['country']: item['region'] for item in response.json()}
        logger.info("Country-region mapping data fetched and processed successfully.")
    except requests.RequestException as e:
        logger.error(f"Error retrieving country-region mapping: {str(e)}")
        country_region_map = {}

def get_db_connection(db_params):
    conn = psycopg2.connect(**db_params)
    logger.debug(f"Created new DB connection: {conn}")
    return conn

# Session with retries
def requests_retry_session(retries=3, backoff_factor=1, status_forcelist=(429, 500, 502, 503, 504)):
    session = requests.Session()
    retry = Retry(total=retries, read=retries, connect=retries, backoff_factor=backoff_factor, status_forcelist=status_forcelist)
    adapter = HTTPAdapter(max_retries=retry)
    session.mount('http://', adapter)
    session.mount('https://', adapter)
    return session


def run_full_script(process_validator_icons):
    print()

    conn = psycopg2.connect(**db_params)
    cur = conn.cursor()

    # Get the highest and lowest epoch numbers
    cur.execute("SELECT MAX(epoch), MIN(epoch) FROM validator_data")
    max_epoch, min_epoch = cur.fetchone()
    
    print(f"Lowest epoch: {min_epoch}")
    print(f"Highest epoch: {max_epoch}")
    
    start_epoch = max_epoch
    end_epoch = max_epoch
    
    print()
    user_input = input(f"Enter the starting epoch number (default: {start_epoch}): ")
    if user_input:
        start_epoch = int(user_input)
    
    print()
    user_input = input(f"Enter the ending epoch number (default: {end_epoch}): ")
    if user_input:
        end_epoch = int(user_input)

    # Ask the user if they want to aggregate epoch info
    print()
    aggregate_epoch_info = input("Do you want to AGGREGATE EPOCH info? (y/n) [y]: ").lower() or 'y'

    process_stakenet = "y"  # Default value
    # Ask the user if they want to process the "stakenet" section
    print()
    process_stakenet = input(f"Do you want to retrieve and store STAKENET info? (y/n) [{process_stakenet}]: ").lower() or process_stakenet

    print()
    process_leader_schedule = input("Do you want to update LEADER SCHEDULE and skip rate info? (y/n) [y]: ").lower() or 'y'

    # Get current date and time
    current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    # Get the filename of the current script
    script_filename = os.path.basename(__file__)

    # Print message in the logger-like format
    print()
    print(f"{current_time} - STARTING - {script_filename}")

    # jrh -- geoip processing is now handled separately by 92_ip_api.py

    if process_leader_schedule == 'y':
        # Process leader schedules before other data processing
        leader_schedule_directory = "/home/smilax/trillium_api/data/leader_schedules"
        process_leader_schedules(db_params, leader_schedule_directory, start_epoch, end_epoch)

    fetch_and_store_data(start_epoch, end_epoch, process_validator_icons, process_stakenet, process_leader_schedule, aggregate_epoch_info)

    # Update elapsed time per epoch
    update_elapsed_time_per_epoch()

    # Calculate and update epochs per year
    calculate_and_update_epochs_per_year()

    cur.close()
    conn.close()

if __name__ == '__main__':

    current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    logger.info(f"92_update_validator_aggregate_info.py - {current_time}")
    print()

    process_validator_icons = "n"
    # Ask the user if they want to process the "retrieve validator icons" section
    process_validator_icons = input("Do you want to retrieve and store validator ICONS? (y/n) [n]: ").lower()
    print()

    if process_validator_icons == 'y':
        icons_only = input("Do you want to ONLY process the validator ICONS? (y/n) [n]: ").lower()
        
        if icons_only == 'y':
            # Run only the validator icons processing
            conn = psycopg2.connect(**db_params)
            cur = conn.cursor()
            fetch_and_store_icons(conn, cur)
            cur.close()
            conn.close()
        else:
            # Proceed with the full script execution
            run_full_script(process_validator_icons='y')
    else:
        # Proceed with the full script execution without processing validator icons
        run_full_script(process_validator_icons='n')
