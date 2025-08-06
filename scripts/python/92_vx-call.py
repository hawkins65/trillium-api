import sys
import os
import psycopg2
import importlib.util

# Setup unified logging
script_dir = os.path.dirname(os.path.abspath(__file__))
logging_config_path = os.path.join(script_dir, "999_logging_config.py")
spec = importlib.util.spec_from_file_location("logging_config", logging_config_path)
logging_config = importlib.util.module_from_spec(spec)
spec.loader.exec_module(logging_config)
logger = logging_config.setup_logging(os.path.basename(__file__).replace('.py', ''))
import requests
import base64
from db_config import db_params
from statistics import mean, median
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Semaphore
import math

# Define variables at the top level of your script
success_count = 0
fail_count = 0
total_vote_credits_calculated = 0
total_voted_slots_calculated = 0

# Rate limiting parameters
MAX_REQUESTS_PER_SECOND = 10
MAX_WORKERS_PER_BATCH = 20
BATCH_SIZE = 50
MAX_CONCURRENT_BATCHES = 3

# Semaphore for rate limiting
rate_limiter = Semaphore(MAX_REQUESTS_PER_SECOND)

def increment_success():
    global success_count
    success_count += 1

def increment_fail():
    global fail_count
    fail_count += 1

# ---------------------------
# Logging Setup
# ---------------------------
# Logger setup moved to unified configuration
logger.setLevel(logging.INFO)
formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")

console_handler = logging.StreamHandler(sys.stdout)
console_handler.setFormatter(formatter)
logger.addHandler(console_handler)

script_name = os.path.basename(sys.argv[0]).replace('.py', '')
log_dir = os.path.expanduser('~/log')
os.makedirs(log_dir, exist_ok=True)
log_file = os.path.join(log_dir, f"{script_name}.log")
file_handler = logging.FileHandler(log_file)
file_handler.setFormatter(formatter)
logger.addHandler(file_handler)

def fetch_vote_accounts_for_epoch(epoch):
    """Fetch all vote_account_pubkey values for a specific epoch."""
    try:
        conn = psycopg2.connect(**db_params)
        cur = conn.cursor()
        cur.execute("""
            SELECT DISTINCT vote_account_pubkey 
            FROM validator_stats 
            WHERE epoch = %s AND vote_account_pubkey IS NOT NULL
        """, (epoch,))
        data = [row[0] for row in cur.fetchall()]
        cur.close()
        conn.close()
        logger.info(f"Fetched {len(data)} vote accounts for epoch {epoch}")
        return data
    except Exception as e:
        logger.error(f"Failed to fetch vote accounts for epoch {epoch}: {e}")
        return []

def calculate_vote_credits(votes_bytes):
    """Calculate vote credits ensuring minimum of 1 for non-zero votes."""
    score = 0
    for vote in votes_bytes:
        if vote == 0:
            score += 0
        elif vote == 1 or vote == 2:
            score += 16
        else:
            score += max(1, 16 - (vote - 2))
    return score

def calculate_voted_slots(votes_bytes):
    """Calculate the number of non-zero values in the votes array."""
    return sum(1 for vote in votes_bytes if vote != 0)

def calculate_statistics(votes_bytes):
    """Calculate max, mean, and median for the votes array, ignoring zeros."""
    non_zero_votes = [vote for vote in votes_bytes if vote != 0]
    return {
        "max": max(non_zero_votes) if non_zero_votes else None,
        "mean": mean(non_zero_votes) if non_zero_votes else None,
        "median": median(non_zero_votes) if non_zero_votes else None,
    }

def insert_votes_into_db(epoch, vote_account_pubkey, votes_bytes):
    """Insert votes and metrics into the PostgreSQL database."""
    global total_vote_credits_calculated, total_voted_slots_calculated
    try:
        vote_credits = calculate_vote_credits(votes_bytes)
        voted_slots = calculate_voted_slots(votes_bytes)
        stats = calculate_statistics(votes_bytes)

        total_vote_credits_calculated += vote_credits
        total_voted_slots_calculated += voted_slots

        avg_credit_per_slot = vote_credits / voted_slots if voted_slots > 0 else 0

        conn = psycopg2.connect(**db_params)
        cur = conn.cursor()

        cur.execute(
            """
            INSERT INTO votes_table (
                epoch, vote_account_pubkey, votes, vote_credits, voted_slots,
                avg_credit_per_voted_slot, max_vote_latency, mean_vote_latency, median_vote_latency
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (epoch, vote_account_pubkey) DO UPDATE
            SET votes = EXCLUDED.votes,
                vote_credits = EXCLUDED.vote_credits,
                voted_slots = EXCLUDED.voted_slots,
                avg_credit_per_voted_slot = EXCLUDED.avg_credit_per_voted_slot,
                max_vote_latency = EXCLUDED.max_vote_latency,
                mean_vote_latency = EXCLUDED.mean_vote_latency,
                median_vote_latency = EXCLUDED.median_vote_latency
            """,
            (
                epoch, vote_account_pubkey, psycopg2.Binary(votes_bytes), vote_credits, voted_slots,
                avg_credit_per_slot, stats["max"], stats["mean"], stats["median"]
            )
        )

        conn.commit()
        cur.close()
        conn.close()
        # logger.info(f"Inserted metrics for epoch {epoch}, vote account {vote_account_pubkey}")
    except Exception as e:
        logger.error(f"Failed to insert data into the database: {e}")
        raise

def process_vote_account(epoch, vote_account_pubkey):
    """Fetch votes data for a single vote account and epoch with rate limiting."""
    url = "https://api.vx.tools/epochs/votes"
    max_attempts = 2
    
    for attempt in range(max_attempts):
        with rate_limiter:
            try:
                response = requests.post(url, json={"identity": vote_account_pubkey, "epoch": epoch}, timeout=10)
                response.raise_for_status()
                increment_success()
                # logger.info(f"Success {success_count} for vote_account_pubkey {vote_account_pubkey}")
                
                data = response.json()
                votes_base64 = data.get("votesBase64")
                if votes_base64 is None:
                    logger.error("Response JSON does not contain 'votesBase64' field.")
                    raise ValueError("Invalid API response.")

                votes_bytes = base64.b64decode(votes_base64)
                insert_votes_into_db(epoch, vote_account_pubkey, votes_bytes)
                return True
                
            except (requests.exceptions.RequestException, ValueError) as e:
                if attempt < max_attempts - 1:
                    logger.warning(f"Attempt {attempt + 1} failed for epoch {epoch}, vote_account_pubkey {vote_account_pubkey}: {e}. Retrying in 5 seconds...")
                    time.sleep(5)
                    continue
                else:
                    logger.error(f"All attempts failed for epoch {epoch}, vote_account_pubkey {vote_account_pubkey}: {e}")
                    increment_fail()
                    logger.info(f"Fail {fail_count} for vote_account_pubkey {vote_account_pubkey}")
                    return False
    time.sleep(1.0 / MAX_REQUESTS_PER_SECOND)

def process_batch(epoch, vote_accounts, batch_num, total_batches):
    """Process a batch of vote accounts in parallel."""
    batch_start_time = time.time()
    logger.info(f"Starting batch {batch_num}/{total_batches} with {len(vote_accounts)} accounts")
    with ThreadPoolExecutor(max_workers=MAX_WORKERS_PER_BATCH) as executor:
        futures = {executor.submit(process_vote_account, epoch, account): account for account in vote_accounts}
        completed = 0
        for future in as_completed(futures):
            account = futures[future]
            try:
                future.result()
                completed += 1
                if completed % 10 == 0:
                    logger.info(f"Batch {batch_num}/{total_batches}: Processed {completed}/{len(vote_accounts)} accounts")
            except Exception as e:
                logger.error(f"Error processing vote_account_pubkey {account}: {e}")
    batch_time = time.time() - batch_start_time
    logger.info(f"Completed batch {batch_num}/{total_batches}: {completed}/{len(vote_accounts)} accounts processed in {batch_time:.2f} seconds")
    return batch_time

def process_all_batches(epoch, vote_accounts):
    """Process all batches in parallel with a limit on concurrent batches."""
    total_batches = math.ceil(len(vote_accounts) / BATCH_SIZE)
    batches = [vote_accounts[i:i + BATCH_SIZE] for i in range(0, len(vote_accounts), BATCH_SIZE)]
    
    batch_times = []
    with ThreadPoolExecutor(max_workers=MAX_CONCURRENT_BATCHES) as executor:
        futures = {
            executor.submit(process_batch, epoch, batch, i + 1, total_batches): i
            for i, batch in enumerate(batches)
        }
        for future in as_completed(futures):
            try:
                batch_time = future.result()
                batch_times.append(batch_time)
            except Exception as e:
                logger.error(f"Error processing batch {futures[future] + 1}: {e}")
    
    return batch_times

def update_vote_credits_rank(epoch):
    """Update vote_credits_rank based on vote_credits in descending order."""
    try:
        conn = psycopg2.connect(**db_params)
        cur = conn.cursor()

        cur.execute(
            """
            UPDATE votes_table
            SET vote_credits_rank = subquery.rank
            FROM (
                SELECT vote_account_pubkey, 
                    RANK() OVER (ORDER BY vote_credits DESC, mean_vote_latency DESC) AS rank
                FROM votes_table
                WHERE epoch = %s
            ) AS subquery
            WHERE votes_table.vote_account_pubkey = subquery.vote_account_pubkey
            AND votes_table.epoch = %s
            """,
            (epoch, epoch)
        )

        conn.commit()
        cur.close()
        conn.close()
        logger.info(f"Updated vote_credits_rank for epoch {epoch}")
    except Exception as e:
        logger.error(f"Failed to update vote_credits_rank for epoch {epoch}: {e}")
        raise

def get_db_totals(epoch, vote_account_pubkey=None):
    """Query the database for total vote credits and voted slots."""
    try:
        conn = psycopg2.connect(**db_params)
        cur = conn.cursor()
        
        if vote_account_pubkey:
            cur.execute("""
                SELECT SUM(vote_credits), SUM(voted_slots)
                FROM votes_table
                WHERE epoch = %s AND vote_account_pubkey = %s
            """, (epoch, vote_account_pubkey))
        else:
            cur.execute("""
                SELECT SUM(vote_credits), SUM(voted_slots)
                FROM votes_table
                WHERE epoch = %s
            """, (epoch,))
        
        result = cur.fetchone()
        total_credits_db = result[0] if result[0] is not None else 0
        total_slots_db = result[1] if result[1] is not None else 0
        
        cur.close()
        conn.close()
        logger.info(f"Queried database totals for epoch {epoch}: Vote Credits={total_credits_db}, Voted Slots={total_slots_db}")
        return total_credits_db, total_slots_db
    
    except Exception as e:
        logger.error(f"Failed to query database totals for epoch {epoch}: {e}")
        return 0, 0

if __name__ == "__main__":
    if len(sys.argv) < 2 or len(sys.argv) > 3:
        logger.error("Usage: vx-call.py <epoch> [vote_account_pubkey]")
        sys.exit(1)

    epoch = int(sys.argv[1])
    vote_account_pubkey = sys.argv[2] if len(sys.argv) == 3 else None
    program_start_time = time.time()

    try:
        if vote_account_pubkey:
            logger.info(f"Starting single account processing for epoch {epoch}, account {vote_account_pubkey}")
            single_start_time = time.time()
            process_vote_account(epoch, vote_account_pubkey)
            single_time = time.time() - single_start_time
            logger.info(f"Completed single account processing for epoch {epoch}, account {vote_account_pubkey} in {single_time:.2f} seconds")
        else:
            logger.info(f"vx-call.py -- Starting batch processing for epoch {epoch}")
            vote_accounts = fetch_vote_accounts_for_epoch(epoch)
            if not vote_accounts:
                logger.info(f"No vote accounts found for epoch {epoch}. Exiting.")
                sys.exit(0)
            batch_times = process_all_batches(epoch, vote_accounts)
            update_vote_credits_rank(epoch)
            logger.info(f"Completed batch processing for epoch {epoch}. Average batch time: {sum(batch_times)/len(batch_times):.2f} seconds")

    finally:
        total_credits_db, total_slots_db = get_db_totals(epoch, vote_account_pubkey)
        program_time = time.time() - program_start_time
        comparison_message = (
            f"Run Totals Comparison for epoch {epoch}:\n"
            f"Calculated - Vote Credits: {total_vote_credits_calculated}, Voted Slots: {total_voted_slots_calculated}\n"
            f"Database   - Vote Credits: {total_credits_db}, Voted Slots: {total_slots_db}\n"
            f"Difference - Vote Credits: {total_vote_credits_calculated - total_credits_db}, "
            f"Voted Slots: {total_voted_slots_calculated - total_slots_db}\n"
            f"Total Program Runtime: {program_time:.2f} seconds"
        )
        logger.info(comparison_message)
        print(comparison_message)

    logger.info("Processing completed.")