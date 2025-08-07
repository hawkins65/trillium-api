#!/usr/bin/env python3
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
import importlib.util
import logging
from datetime import datetime
import signal
import threading
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# Setup basic logging
import sys
from pathlib import Path

# Add parent directories to path for imports
sys.path.append(str(Path(__file__).parent.parent / "python"))
sys.path.append("/home/smilax/api")

# Import RPC_ENDPOINT
try:
    from rpc_config import RPC_ENDPOINT
except ImportError:
    RPC_ENDPOINT = "https://api.mainnet-beta.solana.com"
    print(f"Warning: Could not import RPC_ENDPOINT, using default: {RPC_ENDPOINT}")

# RPC Configuration
RPC_ENDPOINT_1 = RPC_ENDPOINT
RPC_ENDPOINT_2 = RPC_ENDPOINT
#RPC_ENDPOINT_2 = "https://silent-frequent-firefly.solana-mainnet.quiknode.pro/2059a05165e13886cb8226c6b87081ad579860e3/"
RPC_ENDPOINT_3 = RPC_ENDPOINT

headers = {'Content-Type': 'application/json'}
debug = True
error_log_file = "solana_rpc_errors.log"

# ENHANCED RATE LIMITING CONFIGURATION (more aggressive after observations)
MAX_CONCURRENT_REQUESTS = 4  # 
REQUEST_SPACING = 0.15       # Increased to 150ms between requests for better staggering
GLOBAL_REQUEST_SEMAPHORE = threading.Semaphore(MAX_CONCURRENT_REQUESTS)
LAST_REQUEST_TIME = 0
REQUEST_TIMING_LOCK = threading.Lock()

# Network timing constants
SOLANA_SLOT_TIME = 0.4      # 400ms per slot
SOLANA_BLOCKS_PER_SECOND = 2.5
MINUTES_PER_SLOT = SOLANA_SLOT_TIME / 60

# Updated performance targets for 30-minute acceptable lag
MAX_MINUTES_BEHIND_TIP = 30
URGENCY_THRESHOLDS = {
    'MAINTAINING': 30,    # < x min behind
    'CATCHING_UP': 40,    # x-y min behind  
    'URGENT': 60,         # y-z min behind
    'CRITICAL': float('inf')  # > 30 min behind
}

# More conservative thread allocation to prevent bandwidth spikes
THREAD_ALLOCATION = {
    'MAINTAINING': 2,     # Reduced from 3
    'CATCHING_UP': 4,     # Reduced from 6
    'URGENT': 6,          # Reduced from 8
    'CRITICAL': 8,        # Reduced from 12
    'EPOCH_TRANSITION_RUSH': 8,  # Reduced from 12
    'EMERGENCY_CATCHUP': 2
}

# Global state management
shutdown_requested = threading.Event()
current_thread_count = 2
last_evaluation_time = 0
evaluation_interval = 300  # Re-evaluate every 5 minutes

# Optimized session for bandwidth management
session = requests.Session()
adapter = HTTPAdapter(
    pool_connections=3,   # One pool per RPC endpoint
    pool_maxsize=6,       # Reduced to match realistic concurrent usage
    max_retries=0         # Handle retries manually
)
session.mount('https://', adapter)
session.mount('http://', adapter)

def setup_logging():
    """Setup enhanced logging with bandwidth monitoring focus"""
    # Create a completely clean logger
    logger = logging.getLogger('get_epoch_data_csv')
    
    # Clear any existing handlers to avoid duplication
    for handler in logger.handlers[:]:
        logger.removeHandler(handler)
    
    # Prevent propagation to the root logger to avoid duplication
    logger.propagate = False
    
    logger.setLevel(logging.INFO)
    
    now = datetime.now()
    formatted_time = now.strftime('%Y-%m-%d_%H-%M')
    script_name = os.path.basename(__file__).replace('.py', '')
    log_dir = '/home/smilax/log'
    os.makedirs(log_dir, exist_ok=True)
    
    # Console output only by default
    ch = logging.StreamHandler(sys.stdout)
    ch.setLevel(logging.INFO)
    ch_formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    ch.setFormatter(ch_formatter)
    logger.addHandler(ch)
    
    # Only add file handler if explicitly requested
    if os.environ.get('LOG_TO_FILE', 'false').lower() == 'true':
        filename = f'{log_dir}/{script_name}_log_{formatted_time}.log'
        fh = logging.FileHandler(filename)
        fh.setLevel(logging.INFO)
        fh_formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
        fh.setFormatter(fh_formatter)
        logger.addHandler(fh)
    
    # Bandwidth monitoring log (separate logger)
    bandwidth_logger = logging.getLogger('bandwidth')
    # Clear any existing handlers
    for handler in bandwidth_logger.handlers[:]:
        bandwidth_logger.removeHandler(handler)
    
    # Prevent propagation to the root logger
    bandwidth_logger.propagate = False
    
    bandwidth_handler = logging.FileHandler(f'{log_dir}/{script_name}_bandwidth_{formatted_time}.log')
    bandwidth_handler.setFormatter(logging.Formatter('%(asctime)s - %(message)s'))
    bandwidth_logger.addHandler(bandwidth_handler)
    bandwidth_logger.setLevel(logging.INFO)
    
    return logger, bandwidth_logger

def get_domain_name(url):
    """Extract just the domain name from the RPC URL for concise logging"""
    if not url:
        return "unknown"
    
    # Remove protocol
    if url.startswith(('http://', 'https://')):
        clean_url = url.split('://', 1)[1]
    else:
        clean_url = url
    
    # Extract domain
    domain = clean_url.split('/')[0]
    
    # Further extract base domain for common providers
    if 'quiknode.pro' in domain:
        return 'quiknode.pro'
    elif 'alchemy.com' in domain:
        return 'alchemy.com'
    elif 'ankr.com' in domain:
        return 'ankr.com'
    elif 'helius.xyz' in domain:
        return 'helius.xyz'
    elif 'runnode.com' in domain:
        return 'runnode.com'
    else:
        # Return just the domain without subdomains
        parts = domain.split('.')
        if len(parts) > 2:
            return '.'.join(parts[-2:])
        return domain

def rate_limited_request(url, payload, thread_id, logger, bandwidth_logger):
    """Enhanced rate-limited RPC request with better error reporting and URL prefix"""
    global LAST_REQUEST_TIME
    
    request_start_time = time.time()
    domain = get_domain_name(url)
    
    # More restrictive concurrent request limiting
    with GLOBAL_REQUEST_SEMAPHORE:
        # Longer spacing between requests to prevent burst arrivals
        with REQUEST_TIMING_LOCK:
            current_time = time.time()
            elapsed = current_time - LAST_REQUEST_TIME
            if elapsed < REQUEST_SPACING:
                sleep_time = REQUEST_SPACING - elapsed
                time.sleep(sleep_time)
            LAST_REQUEST_TIME = time.time()
        
        try:
            response = session.post(url, headers=headers, json=payload, timeout=(10, 30))
            
            # Calculate metrics
            request_duration = time.time() - request_start_time
            response_size = len(response.content) if response.content else 0
            response_size_mb = response_size / (1024 * 1024)
            
            # Still log very large responses to the bandwidth log for monitoring
            if response_size_mb > 10:
                logger.warning(f"LARGE_RESPONSE - Thread{thread_id}[{domain}] - {response_size_mb:.1f}MB response")
                bandwidth_logger.info(f"Thread{thread_id}[{domain}] - Size: {response_size_mb:.1f}MB, Duration: {request_duration:.2f}s")
            
            # Enhanced error logging for non-200 responses
            if response.status_code != 200:
                error_details = {
                    'status_code': response.status_code,
                    'reason': response.reason,
                    'response_text': response.text[:200] if response.text else '',
                    'domain': domain,
                    'request_duration': request_duration
                }
                bandwidth_logger.error(f"Thread{thread_id}[{domain}] - HTTP_ERROR - {json.dumps(error_details)}")
            
            # Return response and metrics together
            return response, response_size_mb, request_duration
            
        except requests.exceptions.RequestException as e:
            request_duration = time.time() - request_start_time
            error_details = {
                'error_type': type(e).__name__,
                'error_message': str(e),
                'domain': domain,
                'request_duration': request_duration
            }
            bandwidth_logger.error(f"Thread{thread_id}[{domain}] - REQUEST_EXCEPTION - {json.dumps(error_details)}")
            # Return None for the response and default values for metrics
            return None, 0.0, request_duration

def log_error(slot, error_code, error_details, logger):
    """Enhanced error logging with timestamp and better formatting"""
    timestamp = datetime.now().isoformat()
    with open(error_log_file, 'a') as f:
        # Write both old format for compatibility and new enhanced format
        if isinstance(error_details, str):
            try:
                # Try to parse as JSON to extract domain
                details_dict = json.loads(error_details)
                domain = details_dict.get('domain', 'unknown')
                f.write(f"{slot},{error_code},{error_details}\n")
                f.write(f"ENHANCED,{timestamp},{slot},{error_code},{domain},{error_details}\n")
            except:
                f.write(f"{slot},{error_code},{error_details}\n")
        else:
            f.write(f"{slot},{error_code},{json.dumps(error_details)}\n")

def signal_handler(signum, frame):
    """Handle graceful shutdown signals with forced exit"""
    print(f"Received signal {signum}. Aborting process...")
    shutdown_requested.set()
    # Force exit after a short delay to allow minimal cleanup
    threading.Timer(3.0, lambda: os._exit(130)).start()  # 130 is the exit code for SIGINT

# Register signal handlers for more forceful termination
signal.signal(signal.SIGINT, signal_handler)
signal.signal(signal.SIGTERM, signal_handler)
signal.signal(signal.SIGQUIT, signal_handler)  # Add SIGQUIT handler

def process_slot_data(thread_id, slots, file_index, epoch_number, rpc_endpoint, urgency_level, logger, bandwidth_logger):
    """Process slot data with simplified logging and better 401 error reporting"""
    slot_data_file = f"slot_data_thread_{thread_id}_file_{file_index}.csv"
    vote_data_file = f"epoch_votes_thread_{thread_id}_file_{file_index}.csv"
    
    # Get domain for concise logging
    domain = get_domain_name(rpc_endpoint)

    # Simplified startup logging
    logger.info(f"E{epoch_number} T{thread_id}[{domain}] START: {len(slots)} slots")
    
    # Thread performance tracking
    thread_start_time = time.time()
    processed_slots = 0
    total_slots = len(slots)
    last_progress_time = time.time()
    
    try:
        with open(slot_data_file, 'w', newline='') as slot_file, open(vote_data_file, 'w', newline='') as vote_file:
            fieldnames = ["identity_pubkey", "epoch", "block_slot", "block_hash", "block_time", "rewards", "post_balance", "reward_type", "commission",
                          "total_user_tx", "total_vote_tx", "total_cu", "total_signature_fees", "total_priority_fees", "total_fees",
                          "total_tx", "total_signatures", "total_validator_fees", "total_validator_signature_fees", "total_validator_priority_fees",
                          "block_height", "parent_slot", "previous_block_hash"]
            slot_writer = csv.DictWriter(slot_file, fieldnames=fieldnames)
            slot_writer.writeheader()

            vote_writer = csv.DictWriter(vote_file, fieldnames=["epoch", "block_slot", "block_hash", "identity_pubkey", "vote_account_pubkey"])
            vote_writer.writeheader()

            for slot in slots:
                # Check for shutdown request periodically
                if processed_slots % 10 == 0 and shutdown_requested.is_set():
                    logger.info(f"E{epoch_number} T{thread_id}[{domain}]: Shutdown requested - aborting")
                    break
                
                # Single line per slot processing - will be updated with success/fail status
                slot_status = f"E{epoch_number} T{thread_id}[{domain}] Slot {slot} ({processed_slots + 1}/{total_slots})"

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

                success = False
                max_retries = 2  # Conservative retry count
                last_error_details = None
                
                for attempt in range(max_retries):
                    if shutdown_requested.is_set():
                        break
                        
                    try:
                        response_block, size_mb, duration = rate_limited_request(rpc_endpoint, payload_block, thread_id, logger, bandwidth_logger)
                        
                        if response_block and response_block.status_code == 200:
                            response_json = response_block.json()
                            if "result" in response_json and response_json["result"] is not None:
                                block_info = response_json["result"]
                                
                                slot_data_entry = extract_slot_data(slot, block_info, epoch_number)
                                if slot_data_entry:
                                    slot_writer.writerow(slot_data_entry)
                                    slot_file.flush()
                                
                                vote_data = extract_vote_data(slot, block_info, epoch_number)
                                for vote_entry in vote_data:
                                    vote_writer.writerow(vote_entry)
                                vote_file.flush()
                                
                                success = True
                                logger.info(f"{slot_status} ✓ [{size_mb:.1f}MB/{duration:.2f}s]")
                                break
                            else:
                                error_info = response_json.get("error", {})
                                error_code = error_info.get("code", -999)
                                
                                # Enhanced error logging with more details
                                error_details = {
                                    'slot': slot,
                                    'error_code': error_code,
                                    'error_message': error_info.get("message", ""),
                                    'domain': domain,
                                    'attempt': attempt + 1
                                }
                                log_error(slot, error_code, json.dumps(error_details), logger)
                                
                                # Special handling for certain error codes
                                if error_code in [-32009, -32007]:
                                    logger.info(f"{slot_status} - SKIPPED")
                                    success = True
                                    break
                                
                                last_error_details = error_details
                        elif response_block:
                            # Enhanced HTTP error logging with special 401 handling
                            http_error_details = {
                                'slot': slot,
                                'status_code': response_block.status_code,
                                'reason': response_block.reason,
                                'response_text': response_block.text[:100] if response_block.text else '',
                                'domain': domain,
                                'attempt': attempt + 1
                            }
                            
                            log_error(slot, response_block.status_code, json.dumps(http_error_details), logger)
                            
                            # Special handling for 401 errors with detailed diagnostics
                            if response_block.status_code == 401:
                                logger.error(f"{slot_status} - HTTP 401 (attempt {attempt + 1}/{max_retries})")
                            else:
                                if attempt == max_retries - 1: # Only log on last attempt
                                    logger.warning(f"{slot_status} - HTTP {response_block.status_code}")
                            
                            last_error_details = http_error_details
                        else:
                            # Response is None, error already logged in rate_limited_request
                            last_error_details = {'error_type': 'RequestError', 'duration': duration}
                            if attempt == max_retries - 1:
                                logger.error(f"{slot_status} - Request failed [{duration:.2f}s]")
                                
                    except Exception as e:
                        error_type = type(e).__name__
                        unexpected_details = {
                            'slot': slot,
                            'error_type': error_type,
                            'error_message': str(e),
                            'domain': domain,
                            'attempt': attempt + 1
                        }
                        if attempt == max_retries - 1: # Only log on last attempt
                            logger.error(f"{slot_status} - {error_type}: {str(e)}")
                        log_error(slot, -996, json.dumps(unexpected_details), logger)
                        last_error_details = unexpected_details
                    
                    if shutdown_requested.is_set():
                        break
                    
                    # Enhanced retry delay with special handling for 401s
                    if attempt < max_retries - 1:
                        if last_error_details and last_error_details.get('status_code') == 401:
                            # Don't retry 401s as quickly - likely auth issue that won't resolve quickly
                            retry_delay = 15  # Longer delay for auth issues
                        else:
                            retry_delay = min(3 + attempt * 2, 10)  # 3s, 5s, then cap at 10s
                        time.sleep(retry_delay)

                if not success and not shutdown_requested.is_set():
                    # Simplified failure logging
                    logger.error(f"{slot_status} - Failed after {max_retries} attempts")

                processed_slots += 1

                # Progress reporting every 50 slots
                if processed_slots % 50 == 0:
                    elapsed = time.time() - thread_start_time
                    rate = processed_slots / elapsed if elapsed > 0 else 0
                    progress_pct = (processed_slots / total_slots) * 100
                    logger.info(f"E{epoch_number} T{thread_id}[{domain}] Progress: {processed_slots}/{total_slots} ({progress_pct:.1f}%) at {rate:.1f} s/s")

    except Exception as e:
        logger.error(f"E{epoch_number} T{thread_id}[{domain}] - Fatal error: {str(e)}")
        raise
    finally:
        thread_duration = time.time() - thread_start_time
        final_rate = processed_slots / thread_duration if thread_duration > 0 else 0
        logger.info(f"E{epoch_number} T{thread_id}[{domain}] DONE: {processed_slots}/{total_slots} slots in {thread_duration/60:.1f}m ({final_rate:.1f} s/s)")

    return slot_data_file, vote_data_file

def extract_slot_data(slot, block_data, epoch_number):
    """Extract slot data from block response (unchanged)"""
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
    return {
        "identity_pubkey": reward['pubkey'],
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

def extract_vote_data(slot, block_data, epoch_number):
    """Extract vote data from block response (unchanged)"""
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

def calculate_true_catchup_time(slots_behind, processing_rate):
    """Calculate actual catch-up time accounting for ongoing network advancement"""
    if processing_rate <= SOLANA_BLOCKS_PER_SECOND:
        return None  # Can't catch up if processing slower than network
    
    # Net progress rate = processing rate - network advancement rate
    net_progress_rate = processing_rate - SOLANA_BLOCKS_PER_SECOND
    catchup_time_seconds = slots_behind / net_progress_rate
    
    return catchup_time_seconds / 60  # Return in minutes

def calculate_urgency_level(target_epoch, network_current_epoch, network_current_slot, latest_processed_slot):
    """Calculate urgency level with corrected catch-up time estimation"""
    
    if target_epoch == network_current_epoch:
        # Processing current epoch - calculate time behind tip
        slots_behind = network_current_slot - latest_processed_slot
        minutes_behind = slots_behind * MINUTES_PER_SLOT
        
        for urgency, threshold in URGENCY_THRESHOLDS.items():
            if minutes_behind < threshold:
                return urgency, minutes_behind
                
        return 'CRITICAL', minutes_behind
        
    elif target_epoch == network_current_epoch - 1:
        # One epoch behind - transition period, finish quickly
        return 'EPOCH_TRANSITION_RUSH', None
        
    else:
        # Multiple epochs behind - emergency
        return 'EMERGENCY_CATCHUP', None

def get_epoch_info(epoch_number=None, logger=None, bandwidth_logger=None):
    """Get epoch information using rate-limited request"""
    payload = {
        "jsonrpc": "2.0",
        "id": 1,
        "method": "getEpochInfo", 
        "params": [None]
    }
    
    response, _, _ = rate_limited_request(RPC_ENDPOINT_1, payload, "epoch_info", logger, bandwidth_logger)
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
        "end_slot": last_slot_of_epoch,
        "current_slot": epoch_info["absoluteSlot"],
        "slotIndex": epoch_info["slotIndex"],
        "slotsInEpoch": epoch_info["slotsInEpoch"]
    }

def find_missing_slots(epoch_start_slot, epoch_end_slot, logger):
    """Find missing slots from previous runs"""
    processed_slots = set()

    logger.info("Scanning for previously processed slots...")
    run_dirs = glob.glob('run*')
    if not run_dirs:
        logger.info("No run directories found - processing all slots in epoch")
        return list(range(epoch_start_slot, epoch_end_slot + 1))

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
                            continue

    epoch_slots = set(range(epoch_start_slot, epoch_end_slot + 1))
    missing_slots = list(epoch_slots - processed_slots)
    missing_slots.sort()

    logger.info(f"Found {len(processed_slots):,} previously processed slots")
    logger.info(f"Missing slots to process: {len(missing_slots):,}")
    if missing_slots:
        logger.info(f"Slot range: {missing_slots[0]:,} to {missing_slots[-1]:,}")

    return missing_slots

def evaluate_processing_situation(epoch_number, slots_to_process, logger, bandwidth_logger):
    """Enhanced processing situation evaluation with corrected catch-up estimation"""
    try:
        current_epoch_info = get_epoch_info(logger=logger, bandwidth_logger=bandwidth_logger)
        network_current_epoch = current_epoch_info["epoch_number"]
        network_current_slot = current_epoch_info["current_slot"]
        
        if slots_to_process:
            latest_processed_slot = min(slots_to_process) - 1
        else:
            latest_processed_slot = network_current_slot
        
        urgency_level, minutes_behind = calculate_urgency_level(
            epoch_number, network_current_epoch, network_current_slot, latest_processed_slot
        )
        
        # Simplified processing assessment
        logger.info(f"=== Processing Assessment ===")
        logger.info(f"Target: E{epoch_number}, Current: E{network_current_epoch}")
        logger.info(f"Slots: {len(slots_to_process):,}, Urgency: {urgency_level}")
        
        if minutes_behind is not None:
            logger.info(f"Minutes behind: {minutes_behind:.1f}")
        
        # Simplified RPC endpoint logging
        logger.info(f"RPC Endpoints: {get_domain_name(RPC_ENDPOINT_1)}, {get_domain_name(RPC_ENDPOINT_2)}, {get_domain_name(RPC_ENDPOINT_3)}")
        logger.info(f"===========================")
        
        return urgency_level, network_current_epoch, network_current_slot
        
    except Exception as e:
        logger.error(f"Error evaluating processing situation: {e}")
        return 'MAINTAINING', None, None

def get_optimal_thread_count(urgency_level, slots_remaining):
    """Get optimal thread count with conservative bandwidth management"""
    base_threads = THREAD_ALLOCATION.get(urgency_level, 4)
    
    # Workload-based adjustments
    if slots_remaining < 500:
        base_threads = min(base_threads, 2)
    elif slots_remaining < 2000:
        base_threads = min(base_threads, 4)
    elif slots_remaining > 50000:
        base_threads = max(base_threads, 6)
    
    return base_threads

def verify_epoch_completion(epoch_number, epoch_info, logger):
    """Verify that an epoch has been adequately processed"""
    start_slot = epoch_info["start_slot"]
    end_slot = epoch_info["end_slot"]
    total_epoch_slots = end_slot - start_slot + 1
    
    # Count processed slots from all run directories
    processed_slots = set()
    
    logger.info(f"Verifying epoch {epoch_number} completion...")
    run_dirs = glob.glob('run*')
    
    for run_dir in run_dirs:
        csv_files = glob.glob(os.path.join(run_dir, "slot_data_thread_*.csv"))
        for file in csv_files:
            try:
                with open(file, "r") as f:
                    csv_reader = csv.DictReader(f)
                    for row in csv_reader:
                        if 'block_slot' in row:
                            slot = int(row['block_slot'])
                            if start_slot <= slot <= end_slot:
                                processed_slots.add(slot)
            except Exception as e:
                logger.warning(f"Error reading {file}: {e}")
        
        # Also count error slots that are legitimately skipped
        log_files = glob.glob(os.path.join(run_dir, "solana*rpc*errors.log"))
        for file in log_files:
            try:
                with open(file, "r") as f:
                    for line in f:
                        if "-32007" in line or "-32009" in line:  # Non-existent slots
                            slot = int(line.split(',')[0])
                            if start_slot <= slot <= end_slot:
                                processed_slots.add(slot)
            except Exception as e:
                logger.warning(f"Error reading error log {file}: {e}")
    
    processed_count = len(processed_slots)
    completion_percentage = (processed_count / total_epoch_slots) * 100
    
    # Simplified completion status
    logger.info(f"Epoch {epoch_number} completion: {processed_count:,}/{total_epoch_slots:,} slots ({completion_percentage:.2f}%)")
    
    # Consider epoch complete if we have at least 99.5% or at least 431,900 slots
    is_complete = (completion_percentage >= 99.5) or (processed_count >= 431900)
    
    if is_complete:
        logger.info(f"✓ Epoch {epoch_number} is complete")
    else:
        missing_slots = total_epoch_slots - processed_count
        logger.info(f"✗ Epoch {epoch_number} incomplete - missing {missing_slots:,} slots")
    
    return is_complete, processed_count, total_epoch_slots

def analyze_error_patterns(logger):
    """Analyze error patterns from the error log to identify issues"""
    if not os.path.exists(error_log_file):
        logger.info("No error log file found")
        return
    
    error_counts = {}
    domain_error_counts = {}
    recent_errors = []
    auth_error_details = []
    
    try:
        with open(error_log_file, 'r') as f:
            for line in f:
                if line.startswith('ENHANCED,'):
                    # Parse enhanced error format
                    parts = line.strip().split(',', 5)
                    if len(parts) >= 6:
                        _, timestamp, slot, error_code, domain, details = parts
                        error_counts[error_code] = error_counts.get(error_code, 0) + 1
                        domain_error_counts[domain] = domain_error_counts.get(domain, 0) + 1
                        recent_errors.append({
                            'timestamp': timestamp,
                            'slot': slot,
                            'error_code': error_code,
                            'domain': domain,
                            'details': details
                        })
                        
                        # Collect 401 error details for analysis
                        if error_code == '401':
                            try:
                                detail_dict = json.loads(details)
                                auth_error_details.append(detail_dict)
                            except:
                                pass
                                
                elif ',' in line:
                    # Parse old error format
                    parts = line.strip().split(',', 2)
                    if len(parts) >= 2:
                        error_code = parts[1]
                        error_counts[error_code] = error_counts.get(error_code, 0) + 1
    
        # Simplified error analysis
        logger.info("=== Error Analysis ===")
        for code, count in sorted(error_counts.items(), key=lambda x: x[1], reverse=True)[:5]:
            logger.info(f"  {code}: {count} errors")
        
        if domain_error_counts:
            logger.info("Errors by domain:")
            for domain, count in sorted(domain_error_counts.items(), key=lambda x: x[1], reverse=True)[:3]:
                logger.info(f"  {domain}: {count} errors")
        
        # Only report 401 errors if they exist
        recent_401s = [e for e in recent_errors[-100:] if e['error_code'] == '401']
        if recent_401s:
            logger.warning(f"WARNING: {len(recent_401s)} recent 401 auth errors")
            
            # Group by domain
            domain_401_counts = {}
            for error in recent_401s:
                domain = error['domain']
                domain_401_counts[domain] = domain_401_counts.get(domain, 0) + 1
            
            for domain, count in sorted(domain_401_counts.items(), key=lambda x: x[1], reverse=True):
                logger.warning(f"  {domain}: {count} auth errors")
        
        logger.info("=====================")
                
    except Exception as e:
        logger.error(f"Error analyzing error patterns: {e}")

def main(logger, bandwidth_logger):
    """Main function that processes epoch data with clean logger instances"""
    parser = argparse.ArgumentParser()
    parser.add_argument('epoch_number', type=int, help='Epoch number to fetch')
    parser.add_argument('--max-threads', type=int, default=None, help='Maximum number of threads (override)')
    parser.add_argument('--timeout', type=int, default=3600, help='Timeout in seconds (default: 1 hour)')
    args = parser.parse_args()

    epoch_number = args.epoch_number
    timeout_seconds = args.timeout

    try:
        # Simplified startup logging
        logger.info(f"=== Starting Epoch {epoch_number} Processing ===")
        logger.info(f"RPC Endpoints: {get_domain_name(RPC_ENDPOINT_1)}, {get_domain_name(RPC_ENDPOINT_2)}, {get_domain_name(RPC_ENDPOINT_3)}")
        
        # Quick RPC endpoint health check
        logger.info("Performing RPC health check...")
        for i, endpoint in enumerate([RPC_ENDPOINT_1, RPC_ENDPOINT_2, RPC_ENDPOINT_3], 1):
            domain = get_domain_name(endpoint)
            try:
                health_payload = {"jsonrpc": "2.0", "id": 1, "method": "getHealth"}
                health_response, size_mb, duration = rate_limited_request(endpoint, health_payload, f"health_{i}", logger, bandwidth_logger)
                if health_response.status_code == 200:
                    logger.info(f"  ✓ {domain}: Healthy [{size_mb:.1f}MB/{duration:.2f}s]")
                else:
                    logger.warning(f"  ! {domain}: HTTP {health_response.status_code}")
                    if health_response.status_code == 401:
                        logger.error(f"  ! {domain}: Authentication error")
            except Exception as e:
                logger.error(f"  ✗ {domain}: {str(e)}")
        
        # Get epoch and network information
        epoch_info = get_epoch_info(epoch_number, logger, bandwidth_logger)
        current_epoch_info = get_epoch_info(logger=logger, bandwidth_logger=bandwidth_logger)
        
        logger.info(f"Network: E{current_epoch_info['epoch_number']}")
        logger.info(f"Epoch slot range: {epoch_info['start_slot']:,} to {epoch_info['end_slot']:,}")

        # Determine processing end point
        start_slot = epoch_info["start_slot"]
        end_slot = epoch_info["end_slot"]
        current_epoch = current_epoch_info["epoch_number"]

        if epoch_number == current_epoch:
            # For current epoch, only process up to current network position
            if epoch_info["slotIndex"] / epoch_info["slotsInEpoch"] < 0.9:
                end_slot = start_slot + epoch_info["slotIndex"]
                logger.info(f"Current epoch - limiting end slot to {end_slot:,}")

        logger.info(f"Processing range: {start_slot:,} to {end_slot:,} ({end_slot - start_slot + 1:,} slots)")

        # Find slots to process with concise logging
        logger.info("Scanning for missing slots...")
        slots_to_process = find_missing_slots(start_slot, end_slot, logger)
        logger.info(f"Found {len(slots_to_process):,} slots to process")

        if not slots_to_process:
            logger.info(f"No slots to process for Epoch {epoch_number}")
            
            # Perform immediate verification to determine which case
            is_complete, processed_count, total_slots = verify_epoch_completion(epoch_number, epoch_info, logger)
            
            if is_complete:
                logger.info(f"✓ Epoch {epoch_number} is verified COMPLETE")
                # Update the last slots file to record completion
                last_slots_file = "last_slots_to_process.txt"
                with open(last_slots_file, 'w') as f:
                    f.write("0")
                exit(99)
            else:
                logger.warning(f"! Epoch {epoch_number} appears INCOMPLETE but no work found")
                logger.info("Waiting 5 minutes and re-scanning...")
                
                # Wait and re-check for new slots
                time.sleep(300)  # 5 minutes
                
                slots_to_process_recheck = find_missing_slots(start_slot, end_slot, logger)
                logger.info(f"After wait: Found {len(slots_to_process_recheck):,} slots to process")
                
                if slots_to_process_recheck:
                    logger.info(f"✓ Found {len(slots_to_process_recheck)} new slots")
                    slots_to_process = slots_to_process_recheck
                else:
                    logger.error(f"✗ Still no missing slots found")
                    exit(1)
        else:
            logger.info(f"Processing range: {min(slots_to_process):,} to {max(slots_to_process):,}")

        # Continue with normal processing if we have slots
        num_slots_to_process = len(slots_to_process)
        
        # Update the last slots file
        last_slots_file = "last_slots_to_process.txt"
        with open(last_slots_file, 'w') as f:
            f.write(str(num_slots_to_process))

        # Evaluate situation and determine thread count
        urgency_level, network_current_epoch, network_current_slot = evaluate_processing_situation(
            epoch_number, slots_to_process, logger, bandwidth_logger
        )
        
        # For past epochs, use max threads and no timeout
        is_past_epoch = epoch_number < current_epoch
        if is_past_epoch:
            logger.info(f"Past epoch detected (E{epoch_number} < E{current_epoch})")
            optimal_threads = 12  # Maximum threads for past epochs
            timeout_seconds = None  # No timeout for past epochs
        else:
            optimal_threads = get_optimal_thread_count(urgency_level, num_slots_to_process)
            if args.max_threads:
                optimal_threads = min(optimal_threads, args.max_threads)
        
        # Calculate catch-up estimation
        expected_processing_rate = optimal_threads * 0.8  # Assume 0.8 slots/sec per thread
        true_catchup_minutes = calculate_true_catchup_time(num_slots_to_process, expected_processing_rate)
        
        slots_per_file = min(500, max(100, num_slots_to_process // (optimal_threads * 2)))
        
        # Simplified configuration summary
        logger.info(f"Threads: {optimal_threads}, Slots per batch: {slots_per_file}")
        
        if true_catchup_minutes:
            logger.info(f"Estimated catch-up time: {true_catchup_minutes:.1f} minutes")
        else:
            logger.info(f"Processing rate too slow to catch up")

        # Confirmation with different timeouts based on urgency
        if urgency_level in ['URGENT', 'CRITICAL', 'EPOCH_TRANSITION_RUSH']:
            confirmation_timeout = 5
        else:
            confirmation_timeout = 15
            
        logger.info(f"Starting in {confirmation_timeout}s (Press Enter to start immediately)...")
        start_time = time.time()
        while True:
            if time.time() - start_time >= confirmation_timeout:
                break
            if os.sys.stdin in select.select([os.sys.stdin], [], [], 0)[0]:
                break

        # Initialize processing
        futures = []
        file_indices = [0] * optimal_threads
        completed_tasks = 0
        failed_tasks = 0
        processing_start_time = time.time()

        logger.info(f"Starting processing with {optimal_threads} threads")

        with ThreadPoolExecutor(max_workers=optimal_threads) as executor:
            # Submit all tasks
            for i in range(0, len(slots_to_process), slots_per_file):
                thread_id = (i // slots_per_file) % optimal_threads + 1
                thread_slots = slots_to_process[i:i + slots_per_file]
                
                # Rotate RPC endpoints
                rpc_endpoint = RPC_ENDPOINT_1 if thread_id % 3 == 1 else (RPC_ENDPOINT_2 if thread_id % 3 == 2 else RPC_ENDPOINT_3)
                domain = get_domain_name(rpc_endpoint)
                
                future = executor.submit(
                    process_slot_data, 
                    thread_id, 
                    thread_slots, 
                    file_indices[thread_id - 1], 
                    epoch_number, 
                    rpc_endpoint,
                    urgency_level,
                    logger,
                    bandwidth_logger
                )
                futures.append((future, thread_id, len(thread_slots), domain))
                file_indices[thread_id - 1] += 1

            total_futures = len(futures)
            logger.info(f"Submitted {total_futures} tasks to {optimal_threads} threads")
            
            # Process completed futures with enhanced monitoring
            try:
                # Handle no timeout for past epochs
                if timeout_seconds is None:
                    future_iterator = as_completed([f[0] for f in futures])
                    logger.info("Processing with NO TIMEOUT (past epoch)")
                else:
                    future_iterator = as_completed([f[0] for f in futures], timeout=timeout_seconds)
                    logger.info(f"Processing with {timeout_seconds/60:.1f} minute timeout")
                
                # Add keyboard interrupt handling during future processing
                for future in future_iterator:
                    try:
                        # Check for Ctrl+C during processing
                        if shutdown_requested.is_set():
                            logger.info("Shutdown requested - aborting remaining tasks")
                            for f, _, _, _ in futures:
                                if not f.done():
                                    f.cancel()
                            # Force exit after cancellation
                            os._exit(130)
                            
                        slot_data_file, vote_data_file = future.result(timeout=60)
                        completed_tasks += 1
                        completion_rate = completed_tasks / total_futures
                        
                        # Find which thread completed
                        thread_info = next((f for f in futures if f[0] == future), ("unknown", 0, "unknown"))
                        thread_id, slot_count, domain = thread_info[1], thread_info[2], thread_info[3]
                        
                        current_time = time.time()
                        elapsed_time = current_time - processing_start_time
                        
                        # Only log completion status every 5 tasks or 10% to reduce verbosity
                        if completed_tasks % 5 == 0 or completed_tasks == total_futures:
                            if completion_rate > 0:
                                estimated_total_time = elapsed_time / completion_rate
                                remaining_time = estimated_total_time - elapsed_time
                                
                                # Calculate current processing rate
                                estimated_slots_processed = completion_rate * num_slots_to_process
                                current_processing_rate = estimated_slots_processed / elapsed_time if elapsed_time > 0 else 0
                                
                                logger.info(f"Progress: {completed_tasks}/{total_futures} ({completion_rate:.1%}) - "
                                          f"ETA: {remaining_time/60:.1f}min, "
                                          f"Rate: {current_processing_rate:.1f} s/s")
                        
                    except TimeoutError:
                        failed_tasks += 1
                        thread_info = next((f for f in futures if f[0] == future), ("unknown", 0, "unknown"))
                        thread_id, domain = thread_info[1], thread_info[3]
                        logger.error(f"Task timeout - T{thread_id}[{domain}]")
                        
                    except Exception as e:
                        failed_tasks += 1
                        thread_info = next((f for f in futures if f[0] == future), ("unknown", 0, "unknown"))
                        thread_id, domain = thread_info[1], thread_info[3]
                        logger.error(f"Task failed - T{thread_id}[{domain}]: {str(e)}")

            except TimeoutError:
                unfinished_count = total_futures - completed_tasks - failed_tasks
                if timeout_seconds is None:
                    logger.error(f"Unexpected timeout. {unfinished_count} tasks still running")
                else:
                    logger.error(f"Overall timeout after {timeout_seconds/60:.1f}m. {unfinished_count} tasks still running")
                
                for future, thread_id, _, domain in futures:
                    if not future.done():
                        logger.info(f"Cancelling thread {thread_id}[{domain}]")
                        future.cancel()
                
                shutdown_requested.set()
                # Force exit after a short delay
                logger.info("Forcing exit in 5 seconds...")
                time.sleep(5)
                os._exit(1)

        # Final summary with simplified metrics
        total_processing_time = time.time() - processing_start_time
        actual_processing_rate = (completed_tasks * slots_per_file) / total_processing_time if total_processing_time > 0 else 0
        
        logger.info(f"=== Final Summary ===")
        logger.info(f"Epoch {epoch_number}: {completed_tasks}/{total_futures} tasks complete")
        logger.info(f"Time: {total_processing_time/60:.1f}m, Rate: {actual_processing_rate:.1f} s/s")
        
        if actual_processing_rate > SOLANA_BLOCKS_PER_SECOND:
            net_progress = actual_processing_rate - SOLANA_BLOCKS_PER_SECOND
            logger.info(f"Net progress: {net_progress:.1f} s/s (gaining)")
        else:
            logger.warning(f"Net progress: NEGATIVE (falling behind)")

        # Analyze error patterns at the end to show 401 issues
        analyze_error_patterns(logger)

        failure_rate = failed_tasks / total_futures if total_futures > 0 else 0

        if failure_rate > 0.5:
            logger.error(f"High failure rate ({failure_rate:.1%}) - exiting with error")
            exit(1)
        elif failed_tasks > 0:
            logger.warning(f"Some tasks failed ({failure_rate:.1%})")
            exit(0)
        else:
            logger.info(f"All tasks completed successfully!")
            exit(0)

    except KeyboardInterrupt:
        logger.info("Keyboard interrupt received - aborting process immediately...")
        os._exit(130)  # Exit with code 130 (SIGINT)
    except Exception as e:
        logger.error(f"Fatal error in main: {str(e)}")
        raise

if __name__ == "__main__":
    # Set up proper logging without any inherited handlers
    logger, bandwidth_logger = setup_logging()
    
    # Pass the loggers to main
    main(logger, bandwidth_logger)