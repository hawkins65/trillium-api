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
import logging
import sys
from datetime import datetime
import signal
import threading
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# Add the directory containing rpc_config.py to sys.path
sys.path.append("/home/smilax/api")
from rpc_config import RPC_ENDPOINT

# RPC Configuration
RPC_ENDPOINT_1 = RPC_ENDPOINT
RPC_ENDPOINT_2 = "https://silent-frequent-firefly.solana-mainnet.quiknode.pro/2059a05165e13886cb8226c6b87081ad579860e3/"
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

def get_rpc_url_prefix(url):
    """Extract first 15 characters of RPC URL for logging (FIXED)"""
    if not url:
        return "unknown"
    
    # Remove protocol
    if url.startswith(('http://', 'https://')):
        clean_url = url.split('://', 1)[1]
    else:
        clean_url = url
    
    # For better readability, let's show domain + first part of path
    parts = clean_url.split('/')
    if len(parts) >= 2:
        # Show domain + first path segment (which often contains the API key)
        domain = parts[0]
        if len(parts) > 1 and parts[1]:
            # Show first 8 chars of domain + first 8 chars of path
            return f"{domain[:8]}..{parts[1][:8]}"
        else:
            return domain[:15]
    else:
        return clean_url[:15]

def setup_logging():
    """Setup enhanced logging with bandwidth monitoring focus"""
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)
    
    now = datetime.now()
    formatted_time = now.strftime('%Y-%m-%d_%H-%M')
    script_name = os.path.basename(__file__).replace('.py', '')
    log_dir = '/home/smilax/log'
    os.makedirs(log_dir, exist_ok=True)
    
    # Main log file
    filename = f'{log_dir}/{script_name}_log_{formatted_time}.log'
    fh = logging.FileHandler(filename)
    fh.setLevel(logging.INFO)  # Ensure slot logging appears
    fh_formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    fh.setFormatter(fh_formatter)
    
    # Console output
    ch = logging.StreamHandler(sys.stdout)
    ch.setLevel(logging.INFO)
    ch_formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    ch.setFormatter(ch_formatter)
    
    logger.addHandler(fh)
    logger.addHandler(ch)
    
    # Bandwidth monitoring log
    bandwidth_logger = logging.getLogger('bandwidth')
    bandwidth_handler = logging.FileHandler(f'{log_dir}/{script_name}_bandwidth_{formatted_time}.log')
    bandwidth_handler.setFormatter(logging.Formatter('%(asctime)s - %(message)s'))
    bandwidth_logger.addHandler(bandwidth_handler)
    bandwidth_logger.setLevel(logging.INFO)
    
    return logger

logger = setup_logging()
bandwidth_logger = logging.getLogger('bandwidth')

def rate_limited_request(url, payload, thread_id):
    """Enhanced rate-limited RPC request with better error reporting and URL prefix"""
    global LAST_REQUEST_TIME
    
    request_start_time = time.time()
    url_prefix = get_rpc_url_prefix(url)
    
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
            
            # Enhanced bandwidth monitoring with URL prefix
            request_duration = time.time() - request_start_time
            response_size = len(response.content) if response.content else 0
            response_size_mb = response_size / (1024 * 1024)
            
            # Log all responses for bandwidth awareness with URL prefix
            bandwidth_logger.info(f"Thread{thread_id}[{url_prefix}] - Size: {response_size_mb:.1f}MB, Duration: {request_duration:.2f}s, Status: {response.status_code}, Concurrent: {MAX_CONCURRENT_REQUESTS - GLOBAL_REQUEST_SEMAPHORE._value}/{MAX_CONCURRENT_REQUESTS}")
            
            # Alert on large responses that could cause spikes
            if response_size_mb > 10:
                logger.warning(f"LARGE_RESPONSE - Thread{thread_id}[{url_prefix}] - {response_size_mb:.1f}MB response")
            
            # Enhanced error logging for non-200 responses
            if response.status_code != 200:
                error_details = {
                    'status_code': response.status_code,
                    'reason': response.reason,
                    'headers': dict(response.headers),
                    'response_text': response.text[:500] if response.text else '',  # First 500 chars
                    'url_prefix': url_prefix,
                    'request_duration': request_duration
                }
                bandwidth_logger.error(f"Thread{thread_id}[{url_prefix}] - HTTP_ERROR - {json.dumps(error_details)}")
            
            return response
            
        except requests.exceptions.RequestException as e:
            request_duration = time.time() - request_start_time
            error_details = {
                'error_type': type(e).__name__,
                'error_message': str(e),
                'url_prefix': url_prefix,
                'request_duration': request_duration
            }
            bandwidth_logger.error(f"Thread{thread_id}[{url_prefix}] - REQUEST_EXCEPTION - {json.dumps(error_details)}")
            raise

def process_slot_data(thread_id, slots, file_index, epoch_number, rpc_endpoint, urgency_level):
    """Process slot data with enhanced logging and better 401 error reporting"""
    slot_data_file = f"slot_data_thread_{thread_id}_file_{file_index}.csv"
    vote_data_file = f"epoch_votes_thread_{thread_id}_file_{file_index}.csv"
    
    # Get URL prefix for consistent logging
    url_prefix = get_rpc_url_prefix(rpc_endpoint)

    # Enhanced startup logging with URL prefix
    logger.info(f"Epoch {epoch_number} Thread {thread_id}[{url_prefix}] STARTING - {len(slots)} slots, Urgency: {urgency_level}")
    
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
                # Check for shutdown request
                if shutdown_requested.is_set():
                    logger.info(f"Epoch {epoch_number} Thread {thread_id}[{url_prefix}] - Shutdown requested, stopping at slot {slot}")
                    break
                
                # ALWAYS log slot processing with URL prefix
                logger.info(f"Epoch {epoch_number} Thread {thread_id}[{url_prefix}] - Processing slot {slot} ({processed_slots + 1}/{total_slots})")

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
                        logger.info(f"Epoch {epoch_number} Thread {thread_id}[{url_prefix}] - Shutdown requested during retry")
                        break
                        
                    try:
                        response_block = rate_limited_request(rpc_endpoint, payload_block, thread_id)
                        
                        if response_block.status_code == 200:
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
                                logger.info(f"Epoch {epoch_number} Thread {thread_id}[{url_prefix}] - ✅ Completed slot {slot}")
                                break
                            else:
                                error_info = response_json.get("error", {})
                                error_code = error_info.get("code", -999)
                                
                                # Enhanced error logging with more details
                                error_details = {
                                    'slot': slot,
                                    'error_code': error_code,
                                    'error_message': error_info.get("message", ""),
                                    'response': response_json,
                                    'url_prefix': url_prefix,
                                    'attempt': attempt + 1
                                }
                                log_error(slot, error_code, json.dumps(error_details))
                                
                                logger.warning(f"Epoch {epoch_number} Thread {thread_id}[{url_prefix}] - Slot {slot}: RPC Error {error_code} - {error_info.get('message', 'No message')} (attempt {attempt + 1}/{max_retries})")
                                
                                # Skip slot for certain error codes
                                if error_code in [-32009, -32007]:
                                    logger.info(f"Epoch {epoch_number} Thread {thread_id}[{url_prefix}] - Slot {slot}: Non-existent slot (error {error_code})")
                                    success = True
                                    break
                                
                                last_error_details = error_details
                        else:
                            # Enhanced HTTP error logging with special 401 handling
                            http_error_details = {
                                'slot': slot,
                                'status_code': response_block.status_code,
                                'reason': response_block.reason,
                                'response_text': response_block.text[:200] if response_block.text else '',
                                'headers': dict(response_block.headers),
                                'url_prefix': url_prefix,
                                'attempt': attempt + 1,
                                'full_url': rpc_endpoint  # Include full URL for 401 debugging
                            }
                            
                            log_error(slot, response_block.status_code, json.dumps(http_error_details))
                            
                            # Special handling for 401 errors with detailed diagnostics
                            if response_block.status_code == 401:
                                auth_header = response_block.headers.get('WWW-Authenticate', 'Not provided')
                                logger.error(f"Epoch {epoch_number} Thread {thread_id}[{url_prefix}] - Slot {slot}: 🚨 HTTP 401 UNAUTHORIZED (attempt {attempt + 1}/{max_retries})")
                                logger.error(f"  ↳ Full URL: {rpc_endpoint}")
                                logger.error(f"  ↳ WWW-Authenticate header: {auth_header}")
                                logger.error(f"  ↳ Response body: {response_block.text[:200] if response_block.text else 'Empty response'}")
                                logger.error(f"  ↳ Content-Type: {response_block.headers.get('Content-Type', 'Not provided')}")
                                
                                # Analyze the response for common 401 patterns
                                response_text = response_block.text.lower() if response_block.text else ""
                                if "unauthorized" in response_text:
                                    logger.error(f"  ↳ 🔍 Server explicitly returned UNAUTHORIZED")
                                if "api key" in response_text or "apikey" in response_text:
                                    logger.error(f"  ↳ 🔍 Response mentions API key issues")
                                if "quota" in response_text or "limit" in response_text:
                                    logger.error(f"  ↳ 🔍 Possible quota/rate limit issue")
                                if "expired" in response_text:
                                    logger.error(f"  ↳ 🔍 Possible expired credentials")
                                
                                # For QuickNode URLs, check if the API key format looks correct
                                if "quiknode.pro" in rpc_endpoint:
                                    if rpc_endpoint.count('/') >= 4:  # Should have API key as last path segment
                                        api_key_part = rpc_endpoint.split('/')[-2]  # Second to last part
                                        if len(api_key_part) > 30:  # QuickNode keys are long
                                            logger.error(f"  ↳ 🔍 QuickNode API key format appears correct (length: {len(api_key_part)})")
                                        else:
                                            logger.error(f"  ↳ ⚠️  QuickNode API key seems too short (length: {len(api_key_part)})")
                                    else:
                                        logger.error(f"  ↳ ⚠️  QuickNode URL format appears incorrect")
                                
                                # Don't retry 401s as aggressively - likely auth issue that won't resolve
                                if attempt == 0:
                                    logger.error(f"  ↳ 💡 Suggestion: Check API key validity and rate limits for {url_prefix}")
                            else:
                                logger.warning(f"Epoch {epoch_number} Thread {thread_id}[{url_prefix}] - Slot {slot}: HTTP {response_block.status_code} {response_block.reason} (attempt {attempt + 1}/{max_retries})")
                            
                            last_error_details = http_error_details
                            
                    except requests.exceptions.Timeout as e:
                        timeout_details = {
                            'slot': slot,
                            'error_type': 'Timeout',
                            'error_message': str(e),
                            'url_prefix': url_prefix,
                            'attempt': attempt + 1
                        }
                        logger.error(f"Epoch {epoch_number} Thread {thread_id}[{url_prefix}] - Slot {slot}: Timeout on attempt {attempt + 1}/{max_retries}")
                        log_error(slot, -998, json.dumps(timeout_details))
                        last_error_details = timeout_details
                        
                    except requests.exceptions.ConnectionError as e:
                        connection_details = {
                            'slot': slot,
                            'error_type': 'ConnectionError',
                            'error_message': str(e),
                            'url_prefix': url_prefix,
                            'attempt': attempt + 1
                        }
                        logger.error(f"Epoch {epoch_number} Thread {thread_id}[{url_prefix}] - Slot {slot}: Connection error on attempt {attempt + 1}/{max_retries}")
                        log_error(slot, -997, json.dumps(connection_details))
                        last_error_details = connection_details
                        
                    except Exception as e:
                        unexpected_details = {
                            'slot': slot,
                            'error_type': type(e).__name__,
                            'error_message': str(e),
                            'url_prefix': url_prefix,
                            'attempt': attempt + 1
                        }
                        logger.error(f"Epoch {epoch_number} Thread {thread_id}[{url_prefix}] - Slot {slot}: Unexpected error on attempt {attempt + 1}/{max_retries}")
                        log_error(slot, -996, json.dumps(unexpected_details))
                        last_error_details = unexpected_details
                    
                    if shutdown_requested.is_set():
                        break
                    
                    # Enhanced retry delay with special handling for 401s
                    if attempt < max_retries - 1:
                        if last_error_details and last_error_details.get('status_code') == 401:
                            # Don't retry 401s as quickly - likely auth issue that won't resolve quickly
                            retry_delay = 15  # Longer delay for auth issues
                            logger.info(f"Epoch {epoch_number} Thread {thread_id}[{url_prefix}] - Slot {slot}: Auth error, waiting {retry_delay}s before retry (auth issues rarely resolve quickly)")
                        else:
                            retry_delay = min(3 + attempt * 2, 10)  # 3s, 5s, then cap at 10s
                            logger.info(f"Epoch {epoch_number} Thread {thread_id}[{url_prefix}] - Slot {slot}: Retrying in {retry_delay}s")
                        time.sleep(retry_delay)

                if not success:
                    # Enhanced failure logging with last error details and URL prefix
                    failure_summary = f"Epoch {epoch_number} Thread {thread_id}[{url_prefix}] - ❌ Failed slot {slot} after {max_retries} attempts"
                    if last_error_details:
                        if 'status_code' in last_error_details:
                            failure_summary += f" (Last: HTTP {last_error_details['status_code']} {last_error_details.get('reason', '')})"
                        elif 'error_code' in last_error_details:
                            failure_summary += f" (Last: RPC {last_error_details['error_code']})"
                        elif 'error_type' in last_error_details:
                            failure_summary += f" (Last: {last_error_details['error_type']})"
                    logger.error(failure_summary)

                processed_slots += 1

                # Progress reporting every 25 slots with URL prefix
                if processed_slots % 25 == 0:
                    elapsed = time.time() - thread_start_time
                    rate = processed_slots / elapsed if elapsed > 0 else 0
                    progress_pct = (processed_slots / total_slots) * 100
                    logger.info(f"Epoch {epoch_number} Thread {thread_id}[{url_prefix}] - Progress: {processed_slots}/{total_slots} ({progress_pct:.1f}%) at {rate:.1f} slots/sec")

    except Exception as e:
        logger.error(f"Epoch {epoch_number} Thread {thread_id}[{url_prefix}] - Fatal error: {str(e)}")
        raise
    finally:
        thread_duration = time.time() - thread_start_time
        final_rate = processed_slots / thread_duration if thread_duration > 0 else 0
        logger.info(f"Epoch {epoch_number} Thread {thread_id}[{url_prefix}] COMPLETED - {processed_slots}/{total_slots} slots in {thread_duration/60:.1f} min ({final_rate:.1f} slots/sec)")

    return slot_data_file, vote_data_file

def log_error(slot, error_code, error_details):
    """Enhanced error logging with timestamp and better formatting"""
    timestamp = datetime.now().isoformat()
    with open(error_log_file, 'a') as f:
        # Write both old format for compatibility and new enhanced format
        if isinstance(error_details, str):
            try:
                # Try to parse as JSON to extract URL prefix
                details_dict = json.loads(error_details)
                url_prefix = details_dict.get('url_prefix', 'unknown')
                f.write(f"{slot},{error_code},{error_details}\n")
                f.write(f"ENHANCED,{timestamp},{slot},{error_code},{url_prefix},{error_details}\n")
            except:
                f.write(f"{slot},{error_code},{error_details}\n")
        else:
            f.write(f"{slot},{error_code},{json.dumps(error_details)}\n")

# [Keep all the existing helper functions unchanged - extract_slot_data, extract_vote_data, etc.]
# [I'll just show the ones that need URL prefix updates]

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

def signal_handler(signum, frame):
    """Handle graceful shutdown signals"""
    logger.info(f"Received signal {signum}. Requesting graceful shutdown...")
    shutdown_requested.set()

# Register signal handlers
signal.signal(signal.SIGINT, signal_handler)
signal.signal(signal.SIGTERM, signal_handler)

def get_epoch_info(epoch_number=None):
    """Get epoch information using rate-limited request"""
    payload = {
        "jsonrpc": "2.0",
        "id": 1,
        "method": "getEpochInfo", 
        "params": [None]
    }
    
    response = rate_limited_request(RPC_ENDPOINT_1, payload, "epoch_info")
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

def find_missing_slots(epoch_start_slot, epoch_end_slot):
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
                            logger.warning(f"Skipping non-numeric slot value in {file}: '{line.strip()}'")
                            continue

    epoch_slots = set(range(epoch_start_slot, epoch_end_slot + 1))
    missing_slots = list(epoch_slots - processed_slots)
    missing_slots.sort()

    logger.info(f"Found {len(processed_slots):,} previously processed slots")
    logger.info(f"Missing slots to process: {len(missing_slots):,}")
    if missing_slots:
        logger.info(f"Slot range: {missing_slots[0]:,} to {missing_slots[-1]:,}")

    return missing_slots

def evaluate_processing_situation(epoch_number, slots_to_process):
    """Enhanced processing situation evaluation with corrected catch-up estimation"""
    try:
        current_epoch_info = get_epoch_info()
        network_current_epoch = current_epoch_info["epoch_number"]
        network_current_slot = current_epoch_info["current_slot"]
        
        if slots_to_process:
            latest_processed_slot = min(slots_to_process) - 1
        else:
            latest_processed_slot = network_current_slot
        
        urgency_level, minutes_behind = calculate_urgency_level(
            epoch_number, network_current_epoch, network_current_slot, latest_processed_slot
        )
        
        logger.info(f"=== Processing Situation Assessment ===")
        logger.info(f"Target epoch: {epoch_number}")
        logger.info(f"Network current epoch: {network_current_epoch}")
        logger.info(f"Network current slot: {network_current_slot:,}")
        logger.info(f"Slots to process: {len(slots_to_process):,}")
        logger.info(f"Urgency level: {urgency_level}")
        
        if minutes_behind is not None:
            logger.info(f"Minutes behind tip: {minutes_behind:.1f} (target: < {MAX_MINUTES_BEHIND_TIP})")
        
        # Enhanced bandwidth configuration logging with RPC endpoint info
        logger.info(f"Rate limiting: {MAX_CONCURRENT_REQUESTS} concurrent, {REQUEST_SPACING*1000:.0f}ms spacing")
        logger.info(f"Max theoretical bandwidth: ~{MAX_CONCURRENT_REQUESTS * 50}MB/s")
        logger.info(f"RPC Endpoints:")
        logger.info(f"  Primary: {get_rpc_url_prefix(RPC_ENDPOINT_1)}")
        logger.info(f"  Secondary: {get_rpc_url_prefix(RPC_ENDPOINT_2)}")
        logger.info(f"  Tertiary: {get_rpc_url_prefix(RPC_ENDPOINT_3)}")
        logger.info(f"==========================================")
        
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

def verify_epoch_completion(epoch_number, epoch_info):
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
    
    logger.info(f"Epoch {epoch_number} completion status:")
    logger.info(f"  Total slots in epoch: {total_epoch_slots:,}")
    logger.info(f"  Processed slots: {processed_count:,}")
    logger.info(f"  Completion: {completion_percentage:.2f}%")
    
    # Consider epoch complete if we have at least 99.5% or at least 431,900 slots
    is_complete = (completion_percentage >= 99.5) or (processed_count >= 431900)
    
    if is_complete:
        logger.info(f"✅ Epoch {epoch_number} is considered complete")
    else:
        missing_slots = total_epoch_slots - processed_count
        logger.info(f"❌ Epoch {epoch_number} incomplete - missing {missing_slots:,} slots")
    
    return is_complete, processed_count, total_epoch_slots

def analyze_error_patterns():
    """Analyze error patterns from the error log to identify issues"""
    if not os.path.exists(error_log_file):
        logger.info("No error log file found")
        return
    
    error_counts = {}
    url_error_counts = {}
    recent_errors = []
    auth_error_details = []
    
    try:
        with open(error_log_file, 'r') as f:
            for line in f:
                if line.startswith('ENHANCED,'):
                    # Parse enhanced error format
                    parts = line.strip().split(',', 5)
                    if len(parts) >= 6:
                        _, timestamp, slot, error_code, url_prefix, details = parts
                        error_counts[error_code] = error_counts.get(error_code, 0) + 1
                        url_error_counts[url_prefix] = url_error_counts.get(url_prefix, 0) + 1
                        recent_errors.append({
                            'timestamp': timestamp,
                            'slot': slot,
                            'error_code': error_code,
                            'url_prefix': url_prefix,
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
    
        logger.info("=== Error Pattern Analysis ===")
        logger.info("Error code frequencies:")
        for code, count in sorted(error_counts.items(), key=lambda x: x[1], reverse=True):
            logger.info(f"  {code}: {count} occurrences")
        
        if url_error_counts:
            logger.info("Error frequencies by RPC endpoint:")
            for url_prefix, count in sorted(url_error_counts.items(), key=lambda x: x[1], reverse=True):
                logger.info(f"  {url_prefix}: {count} errors")
        
        # Enhanced 401 error analysis
        recent_401s = [e for e in recent_errors[-100:] if e['error_code'] == '401']
        if recent_401s:
            logger.warning(f"=== 401 Authentication Error Analysis ===")
            logger.warning(f"Recent 401 errors: {len(recent_401s)} in last 100 errors")
            
            # Group by URL prefix
            url_401_counts = {}
            for error in recent_401s:
                url_prefix = error['url_prefix']
                url_401_counts[url_prefix] = url_401_counts.get(url_prefix, 0) + 1
            
            logger.warning("401 errors by endpoint:")
            for url_prefix, count in sorted(url_401_counts.items(), key=lambda x: x[1], reverse=True):
                logger.warning(f"  {url_prefix}: {count} auth errors")
            
            # Show recent 401 error details
            logger.warning("Recent 401 error samples:")
            for error in recent_401s[-3:]:  # Last 3
                logger.warning(f"  {error['timestamp']} - Slot {error['slot']} on {error['url_prefix']}")
                
            # Analyze 401 response patterns
            if auth_error_details:
                logger.warning("401 Response Analysis:")
                common_responses = {}
                for detail in auth_error_details[-10:]:  # Last 10
                    response_text = detail.get('response_text', '').lower()
                    if response_text:
                        # Look for key phrases
                        if 'unauthorized' in response_text:
                            common_responses['unauthorized'] = common_responses.get('unauthorized', 0) + 1
                        if 'api key' in response_text or 'apikey' in response_text:
                            common_responses['api_key_issue'] = common_responses.get('api_key_issue', 0) + 1
                        if 'quota' in response_text or 'limit' in response_text:
                            common_responses['quota_limit'] = common_responses.get('quota_limit', 0) + 1
                        if 'expired' in response_text:
                            common_responses['expired'] = common_responses.get('expired', 0) + 1
                
                for pattern, count in common_responses.items():
                    logger.warning(f"  {pattern}: {count} occurrences")
        
        logger.info("===============================")
                
    except Exception as e:
        logger.error(f"Error analyzing error patterns: {e}")

def main():
    global current_thread_count
    
    parser = argparse.ArgumentParser()
    parser.add_argument('epoch_number', type=int, help='Epoch number to fetch')
    parser.add_argument('--max-threads', type=int, default=None, help='Maximum number of threads (override)')
    parser.add_argument('--timeout', type=int, default=3600, help='Timeout in seconds (default: 1 hour)')
    args = parser.parse_args()

    epoch_number = args.epoch_number
    timeout_seconds = args.timeout

    try:
        # Enhanced startup logging with RPC endpoint diagnostics
        logger.info(f"=== Epoch Processing Startup ===")
        logger.info(f"Target Epoch: {epoch_number}")
        logger.info(f"RPC Endpoint Configuration:")
        logger.info(f"  Primary [1]: {get_rpc_url_prefix(RPC_ENDPOINT_1)} (Full: {RPC_ENDPOINT_1})")
        logger.info(f"  Secondary [2]: {get_rpc_url_prefix(RPC_ENDPOINT_2)} (Full: {RPC_ENDPOINT_2})")
        logger.info(f"  Tertiary [3]: {get_rpc_url_prefix(RPC_ENDPOINT_3)} (Full: {RPC_ENDPOINT_3})")
        
        # Quick RPC endpoint health check
        logger.info("🔍 Performing quick RPC endpoint health check...")
        for i, (name, endpoint) in enumerate([("Primary", RPC_ENDPOINT_1), ("Secondary", RPC_ENDPOINT_2), ("Tertiary", RPC_ENDPOINT_3)], 1):
            try:
                health_payload = {"jsonrpc": "2.0", "id": 1, "method": "getHealth"}
                health_response = rate_limited_request(endpoint, health_payload, f"health_{i}")
                if health_response.status_code == 200:
                    logger.info(f"  ✅ {name} [{get_rpc_url_prefix(endpoint)}]: Healthy")
                else:
                    logger.warning(f"  ⚠️  {name} [{get_rpc_url_prefix(endpoint)}]: HTTP {health_response.status_code}")
                    if health_response.status_code == 401:
                        logger.error(f"      🚨 AUTHENTICATION ISSUE DETECTED on {name}")
                        logger.error(f"      Response: {health_response.text[:100] if health_response.text else 'Empty'}")
            except Exception as e:
                logger.error(f"  ❌ {name} [{get_rpc_url_prefix(endpoint)}]: {str(e)}")
        
        # Get epoch and network information
        epoch_info = get_epoch_info(epoch_number)
        current_epoch_info = get_epoch_info()
        
        logger.info(f"Network Current Epoch: {current_epoch_info['epoch_number']}")
        logger.info(f"Epoch slot range: {epoch_info['start_slot']:,} to {epoch_info['end_slot']:,}")

        # Determine processing end point
        start_slot = epoch_info["start_slot"]
        end_slot = epoch_info["end_slot"]
        current_epoch = current_epoch_info["epoch_number"]

        logger.info(f"Initial slot range: {start_slot:,} to {end_slot:,}")

        if epoch_number == current_epoch:
            # For current epoch, only process up to current network position
            if epoch_info["slotIndex"] / epoch_info["slotsInEpoch"] < 0.9:
                end_slot = start_slot + epoch_info["slotIndex"]
                logger.info(f"Current epoch processing - limiting end slot to {end_slot:,}")

        logger.info(f"Final processing range: {start_slot:,} to {end_slot:,}")
        logger.info(f"Total slots in range: {end_slot - start_slot + 1:,}")

        # Find slots to process with verbose logging
        logger.info("🔍 Scanning for missing slots...")
        slots_to_process = find_missing_slots(start_slot, end_slot)
        logger.info(f"📊 Found {len(slots_to_process):,} slots that need processing")

        if not slots_to_process:
            logger.info(f"❗ Epoch {epoch_number} - No slots to process found")
            logger.info("🔍 This could mean:")
            logger.info("   1. Epoch is complete")
            logger.info("   2. All slots have been processed")
            logger.info("   3. There's an issue with slot detection")
            
            # Perform immediate verification to determine which case
            logger.info("🔍 Performing completion verification to determine status...")
            is_complete, processed_count, total_slots = verify_epoch_completion(epoch_number, epoch_info)
            
            logger.info(f"📈 Verification results:")
            logger.info(f"   Processed: {processed_count:,} slots")
            logger.info(f"   Total: {total_slots:,} slots")
            logger.info(f"   Completion: {(processed_count/total_slots)*100:.2f}%")
            logger.info(f"   Completion threshold: 99.5% OR 431,900+ slots")
            
            if is_complete:
                logger.info(f"✅ Epoch {epoch_number} is verified COMPLETE")
                logger.info(f"   Meeting completion criteria - exiting with code 99")
                # Update the last slots file to record completion
                last_slots_file = "last_slots_to_process.txt"
                with open(last_slots_file, 'w') as f:
                    f.write("0")
                exit(99)
            else:
                logger.warning(f"⚠️ Epoch {epoch_number} appears INCOMPLETE")
                logger.warning(f"   But find_missing_slots() found no work to do")
                logger.warning(f"   This suggests an issue with slot detection logic")
                logger.info("⏳ Waiting 5 minutes and re-scanning for missing slots...")
                logger.info("   (Sometimes slots become available after network delays)")
                
                # Wait and re-check for new slots
                time.sleep(300)  # 5 minutes
                
                logger.info("🔍 Re-scanning for missing slots after wait...")
                slots_to_process_recheck = find_missing_slots(start_slot, end_slot)
                logger.info(f"📊 After wait: Found {len(slots_to_process_recheck):,} slots to process")
                
                if slots_to_process_recheck:
                    logger.info(f"✅ Found {len(slots_to_process_recheck)} new slots after waiting")
                    logger.info("🚀 Proceeding with processing these newly discovered slots")
                    slots_to_process = slots_to_process_recheck
                else:
                    logger.error(f"❌ Still no missing slots found after 5-minute wait")
                    logger.error(f"   Epoch appears incomplete but no work detected")
                    logger.error(f"   This indicates a bug in slot detection logic")
                    exit(1)
        else:
            logger.info(f"✅ Found work to do: {len(slots_to_process):,} slots need processing")
            if len(slots_to_process) > 0:
                logger.info(f"   Slot range: {min(slots_to_process):,} to {max(slots_to_process):,}")

        # Continue with normal processing if we have slots
        num_slots_to_process = len(slots_to_process)
        logger.info(f"📝 Recording {num_slots_to_process} slots to process in tracking file")

        # Update the last slots file
        last_slots_file = "last_slots_to_process.txt"
        with open(last_slots_file, 'w') as f:
            f.write(str(num_slots_to_process))

        # Evaluate situation and determine thread count
        urgency_level, network_current_epoch, network_current_slot = evaluate_processing_situation(
            epoch_number, slots_to_process
        )
        
        optimal_threads = get_optimal_thread_count(urgency_level, num_slots_to_process)
        
        if args.max_threads:
            optimal_threads = min(optimal_threads, args.max_threads)
        
        # Calculate CORRECTED catch-up estimation
        expected_processing_rate = optimal_threads * 0.8  # Assume 0.8 slots/sec per thread
        true_catchup_minutes = calculate_true_catchup_time(num_slots_to_process, expected_processing_rate)
        
        slots_per_file = min(500, max(100, num_slots_to_process // (optimal_threads * 2)))
        
        logger.info(f"=== Processing Configuration ===")
        logger.info(f"Slots to process: {num_slots_to_process:,}")
        logger.info(f"Urgency level: {urgency_level}")
        logger.info(f"Optimal threads: {optimal_threads}")
        logger.info(f"Slots per file: {slots_per_file}")
        logger.info(f"Expected processing rate: {expected_processing_rate:.1f} slots/sec")
        logger.info(f"Network advancement rate: {SOLANA_BLOCKS_PER_SECOND:.1f} slots/sec")
        
        if true_catchup_minutes:
            logger.info(f"⏱️  CORRECTED catch-up time: {true_catchup_minutes:.1f} minutes")
            logger.info(f"   (This accounts for network advancing during processing)")
        else:
            logger.info(f"⚠️  Processing rate too slow to catch up - will fall further behind")
            
        net_progress = expected_processing_rate - SOLANA_BLOCKS_PER_SECOND
        logger.info(f"Net progress rate: {net_progress:.1f} slots/sec (processing - network advancement)")
        logger.info(f"====================================")

        # Confirmation with different timeouts based on urgency
        if urgency_level in ['URGENT', 'CRITICAL', 'EPOCH_TRANSITION_RUSH']:
            confirmation_timeout = 10
        else:
            confirmation_timeout = 30
            
        logger.info(f"Starting in {confirmation_timeout} seconds (Press Enter to start immediately)...")
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

        logger.info(f"🚀 Starting processing with {optimal_threads} threads")

        with ThreadPoolExecutor(max_workers=optimal_threads) as executor:
            # Submit all tasks
            for i in range(0, len(slots_to_process), slots_per_file):
                thread_id = (i // slots_per_file) % optimal_threads + 1
                thread_slots = slots_to_process[i:i + slots_per_file]
                
                # Rotate RPC endpoints with enhanced logging
                rpc_endpoint = RPC_ENDPOINT_1 if thread_id % 3 == 1 else (RPC_ENDPOINT_2 if thread_id % 3 == 2 else RPC_ENDPOINT_3)
                url_prefix = get_rpc_url_prefix(rpc_endpoint)
                
                logger.info(f"📋 Submitting task: Thread {thread_id}[{url_prefix}] - {len(thread_slots)} slots")
                
                future = executor.submit(
                    process_slot_data, 
                    thread_id, 
                    thread_slots, 
                    file_indices[thread_id - 1], 
                    epoch_number, 
                    rpc_endpoint,
                    urgency_level
                )
                futures.append((future, thread_id, len(thread_slots), url_prefix))
                file_indices[thread_id - 1] += 1

            total_futures = len(futures)
            logger.info(f"Submitted {total_futures} tasks to {optimal_threads} threads")
            
            # Process completed futures with enhanced monitoring
            try:
                for future in as_completed([f[0] for f in futures], timeout=timeout_seconds):
                    try:
                        slot_data_file, vote_data_file = future.result(timeout=60)
                        completed_tasks += 1
                        completion_rate = completed_tasks / total_futures
                        
                        # Find which thread completed
                        thread_info = next((f for f in futures if f[0] == future), ("unknown", 0, "unknown"))
                        thread_id, slot_count, url_prefix = thread_info[1], thread_info[2], thread_info[3]
                        
                        current_time = time.time()
                        elapsed_time = current_time - processing_start_time
                        
                        if completion_rate > 0:
                            estimated_total_time = elapsed_time / completion_rate
                            remaining_time = estimated_total_time - elapsed_time
                            
                            # Calculate current processing rate
                            estimated_slots_processed = completion_rate * num_slots_to_process
                            current_processing_rate = estimated_slots_processed / elapsed_time if elapsed_time > 0 else 0
                            
                            # Update true catch-up estimation based on actual performance
                            if current_processing_rate > SOLANA_BLOCKS_PER_SECOND:
                                remaining_slots = num_slots_to_process * (1 - completion_rate)
                                net_rate = current_processing_rate - SOLANA_BLOCKS_PER_SECOND
                                updated_catchup_time = remaining_slots / net_rate / 60
                                
                                logger.info(f"✅ Completed Thread {thread_id}[{url_prefix}] - Progress: {completed_tasks}/{total_futures} ({completion_rate:.1%}) - "
                                          f"ETA: {remaining_time/60:.1f}min, "
                                          f"Catch-up: {updated_catchup_time:.1f}min, "
                                          f"Rate: {current_processing_rate:.1f} slots/sec")
                            else:
                                logger.warning(f"⚠️  Completed Thread {thread_id}[{url_prefix}] - Progress: {completed_tasks}/{total_futures} ({completion_rate:.1%}) - "
                                             f"ETA: {remaining_time/60:.1f}min - "
                                             f"FALLING BEHIND (rate: {current_processing_rate:.1f} < {SOLANA_BLOCKS_PER_SECOND:.1f})")
                        
                    except TimeoutError:
                        failed_tasks += 1
                        thread_info = next((f for f in futures if f[0] == future), ("unknown", 0, "unknown"))
                        thread_id, url_prefix = thread_info[1], thread_info[3]
                        logger.error(f"⏰ Task timeout - Thread {thread_id}[{url_prefix}]")
                        
                    except Exception as e:
                        failed_tasks += 1
                        thread_info = next((f for f in futures if f[0] == future), ("unknown", 0, "unknown"))
                        thread_id, url_prefix = thread_info[1], thread_info[3]
                        logger.error(f"❌ Task failed - Thread {thread_id}[{url_prefix}]: {str(e)}")

            except TimeoutError:
                unfinished_count = total_futures - completed_tasks - failed_tasks
                logger.error(f"⏰ Overall timeout after {timeout_seconds/60:.1f} minutes. {unfinished_count} tasks still running")
                
                for future, thread_id, _, url_prefix in futures:
                    if not future.done():
                        logger.info(f"Cancelling thread {thread_id}[{url_prefix}]")
                        future.cancel()
                
                shutdown_requested.set()
                time.sleep(30)

        # Final summary with corrected metrics and RPC endpoint analysis
        total_processing_time = time.time() - processing_start_time
        actual_processing_rate = (completed_tasks * slots_per_file) / total_processing_time if total_processing_time > 0 else 0
        
        logger.info(f"=== Final Processing Summary ===")
        logger.info(f"Epoch {epoch_number} - Completed: {completed_tasks}/{total_futures} tasks")
        logger.info(f"Processing time: {total_processing_time/60:.1f} minutes")
        logger.info(f"Actual rate: {actual_processing_rate:.1f} slots/sec")
        logger.info(f"Network rate: {SOLANA_BLOCKS_PER_SECOND:.1f} slots/sec")
        
        if actual_processing_rate > SOLANA_BLOCKS_PER_SECOND:
            net_progress = actual_processing_rate - SOLANA_BLOCKS_PER_SECOND
            logger.info(f"✅ Net progress: {net_progress:.1f} slots/sec (gaining on network)")
        else:
            logger.warning(f"⚠️  Net progress: NEGATIVE (falling behind network)")
            
        logger.info(f"Threads used: {optimal_threads}")
        logger.info(f"Rate limiting: {MAX_CONCURRENT_REQUESTS} concurrent, {REQUEST_SPACING*1000:.0f}ms spacing")
        
        # RPC endpoint performance summary
        logger.info("RPC Endpoint Usage:")
        logger.info(f"  Primary: {get_rpc_url_prefix(RPC_ENDPOINT_1)}")
        logger.info(f"  Secondary: {get_rpc_url_prefix(RPC_ENDPOINT_2)}")
        logger.info(f"  Tertiary: {get_rpc_url_prefix(RPC_ENDPOINT_3)}")
        logger.info(f"================================")

        # Performance summary for bandwidth log with enhanced details
        bandwidth_logger.info(f"FINAL_SUMMARY - Epoch: {epoch_number}, Tasks: {total_futures}, "
                             f"Completed: {completed_tasks}, Failed: {failed_tasks}, "
                             f"Duration: {total_processing_time:.1f}s, Rate: {actual_processing_rate:.2f} slots/sec, "
                             f"Urgency: {urgency_level}, Threads: {optimal_threads}, "
                             f"RPC1: {get_rpc_url_prefix(RPC_ENDPOINT_1)}, "
                             f"RPC2: {get_rpc_url_prefix(RPC_ENDPOINT_2)}, "
                             f"RPC3: {get_rpc_url_prefix(RPC_ENDPOINT_3)}")

        # Analyze error patterns at the end to show 401 issues
        logger.info("🔍 Analyzing error patterns from this run...")
        analyze_error_patterns()

        failure_rate = failed_tasks / total_futures if total_futures > 0 else 0

        if failure_rate > 0.5:
            logger.error(f"High failure rate ({failure_rate:.1%}) - exiting with error")
            logger.error("💡 Check the error analysis above for 401 authentication issues")
            exit(1)
        elif failed_tasks > 0:
            logger.warning(f"Some tasks failed ({failure_rate:.1%}) - remaining slots will be picked up next run")
            if failure_rate > 0.1:  # More than 10% failure rate
                logger.warning("💡 High failure rate detected - check error analysis above")
            exit(0)
        else:
            logger.info(f"All tasks completed successfully!")
            exit(0)

    except KeyboardInterrupt:
        logger.info("Keyboard interrupt - shutting down gracefully...")
        shutdown_requested.set()
        raise
    except Exception as e:
        logger.error(f"Fatal error in main: {str(e)}")
        logger.error("💡 Run error pattern analysis to check for authentication issues")
        raise

if __name__ == "__main__":
    main()