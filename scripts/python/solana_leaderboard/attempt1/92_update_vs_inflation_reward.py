import psycopg2
import requests
import sys
import os
import time
from typing import List, Dict, Tuple, Set
from concurrent.futures import ThreadPoolExecutor, as_completed
from db_config import db_params
from rpc_config import RPC_ENDPOINT  # Import the centralized RPC endpoint
from logging_config import setup_logging

# Use the imported RPC endpoint
RPC_URL = RPC_ENDPOINT

# Constants
MAX_ACCOUNTS_PER_REQUEST = 32 
RATE_LIMIT_SLEEP = 1  

conn = psycopg2.connect(**db_params)

# Setup logging
script_name = os.path.basename(__file__).replace('.py', '')
logger = setup_logging(script_name)

def get_epoch(provided_epoch: int = None) -> int:
    if provided_epoch is not None:
        return provided_epoch
    max_epoch = get_max_epoch(conn)
    while True:
        try:
            user_input = input(f"Enter epoch number (default: {max_epoch}): ") or str(max_epoch)
            return int(user_input)
        except ValueError:
            logger.info("Please enter a valid integer for epoch")

def get_available_stake_epochs() -> Set[int]:
    """Fetch all distinct epochs present in stake_accounts."""
    try:
        cur = conn.cursor()
        cur.execute("SELECT DISTINCT epoch FROM stake_accounts ORDER BY epoch")
        epochs = {row[0] for row in cur.fetchall()}
        cur.close()
        return epochs
    except Exception as e:
        logging.error(f"Failed to fetch available stake epochs: {e}")
        return set()

def get_vote_accounts_and_data(epoch: int) -> Dict[str, int]:
    try:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT DISTINCT vote_account_pubkey, activated_stake
            FROM validator_stats 
            WHERE epoch = %s AND vote_account_pubkey IS NOT NULL
            ORDER BY vote_account_pubkey
            """, 
            (epoch,)
        )
        vote_data = {row[0]: row[1] if row[1] is not None else 0 for row in cur.fetchall()}
        cur.close()
        logger.info(f"Fetched {len(vote_data)} vote accounts with data for epoch {epoch}")
        return vote_data
    except Exception as e:
        logger.error(f"Failed to fetch vote accounts and data: {e}")
        return {}

def get_all_top_stake_accounts(epoch: int, vote_pubkeys: List[str]) -> Dict[str, List[Tuple[str, int]]]:
    """Fetch up to top 1 stake accounts for all validators in one query, if epoch exists."""
    try:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT 
                vote_account_pubkey,
                stake_pubkey,
                active_stake
            FROM (
                SELECT 
                    vote_account_pubkey,
                    stake_pubkey,
                    active_stake,
                    ROW_NUMBER() OVER (
                        PARTITION BY vote_account_pubkey 
                        ORDER BY active_stake DESC
                    ) AS rn
                FROM stake_accounts
                WHERE epoch = %s
                AND vote_account_pubkey = ANY(%s)
                AND active_stake > 0
                AND activation_epoch < %s
            ) t
            WHERE rn <= 1
            ORDER BY vote_account_pubkey, active_stake DESC;
            """,
            (epoch, vote_pubkeys, epoch)
        )
        rows = cur.fetchall()
        cur.close()

        stake_accounts_dict = {}
        for vote_pubkey, stake_pubkey, active_stake in rows:
            if vote_pubkey not in stake_accounts_dict:
                stake_accounts_dict[vote_pubkey] = []
            stake_accounts_dict[vote_pubkey].append((stake_pubkey, active_stake if active_stake is not None else 0))

        logger.info(f"Fetched top stake accounts for {len(stake_accounts_dict)} vote accounts in epoch {epoch}")
        return stake_accounts_dict
    except Exception as e:
        logger.error(f"Failed to fetch all top stake accounts: {e}")
        return {}

def get_all_inflation_rewards(pubkeys: List[str], epoch: int) -> Dict[str, int]:
    """Fetch inflation rewards for all public keys in one optimized batch process."""
    headers = {"Content-Type": "application/json"}
    rewards_by_pubkey = {}
    total_batches = (len(pubkeys) + MAX_ACCOUNTS_PER_REQUEST - 1) // MAX_ACCOUNTS_PER_REQUEST

    for i in range(0, len(pubkeys), MAX_ACCOUNTS_PER_REQUEST):
        batch_num = i // MAX_ACCOUNTS_PER_REQUEST + 1
        pubkey_group = pubkeys[i:i + MAX_ACCOUNTS_PER_REQUEST]
        payload = {
            "jsonrpc": "2.0",
            "id": 1,
            "method": "getInflationReward",
            "params": [pubkey_group, {"epoch": epoch}]
        }

        retry_count = 0
        success = False

        while retry_count < 5 and not success:
            try:
                response = requests.post(RPC_URL, headers=headers, json=payload)
                if response.status_code == 200:
                    results = response.json().get('result', [])
                    for pubkey, result in zip(pubkey_group, results):
                        rewards_by_pubkey[pubkey] = result.get('amount', 0) if result else 0
                    success = True
                else:
                    retry_count += 1
                    logger.warning(f"Batch {batch_num}/{total_batches} - Invalid response code {response.status_code}. Retrying {retry_count}/5...")
                    time.sleep(1)
            except Exception as e:
                retry_count += 1
                logger.error(f"Batch {batch_num}/{total_batches} - Error retrieving rewards (attempt {retry_count}/5): {e}")
                time.sleep(1)

        if not success:
            logger.error(f"Batch {batch_num}/{total_batches} - Failed to retrieve rewards after 5 attempts")
            for pubkey in pubkey_group:
                rewards_by_pubkey[pubkey] = 0

        logger.info(f"Completed batch {batch_num}/{total_batches} - Processed {len(pubkey_group)} pubkeys")
        time.sleep(RATE_LIMIT_SLEEP)

    return rewards_by_pubkey

def process_vote_account_chunk(
    vote_chunk: List[str],
    vote_account_data: Dict[str, int],
    stake_accounts_dict: Dict[str, List[Tuple[str, int]]],
    all_rewards: Dict[str, int],
    epoch: int
) -> Tuple[Dict[str, int], Dict[str, int]]:
    """Process a chunk of vote accounts in parallel."""
    validator_rewards = {}
    delegator_rewards = {}

    for vote_pubkey in vote_chunk:
        validator_rewards[vote_pubkey] = all_rewards.get(vote_pubkey, 0)

        stake_accounts = stake_accounts_dict.get(vote_pubkey, [])
        stake_pubkeys = [account[0] for account in stake_accounts]
        if not stake_pubkeys:
            delegator_rewards[vote_pubkey] = 0
            logger.info(f"Vote account {vote_pubkey}: No stake accounts found for epoch {epoch}")
            continue

        total_sampled_stake = sum(account[1] or 0 for account in stake_accounts)
        total_activated_stake = vote_account_data.get(vote_pubkey, 0) or 0

        if total_sampled_stake is None or total_activated_stake is None:
            logging.warning(f"Vote account {vote_pubkey}: Unexpected None in stake data (sampled: {total_sampled_stake}, activated: {total_activated_stake})")

        if total_sampled_stake > 0 and total_activated_stake > 0:
            total_sampled_reward = sum(all_rewards.get(pubkey, 0) for pubkey in stake_pubkeys)
            reward_rate = total_sampled_reward / total_sampled_stake
            delegator_reward = int(reward_rate * total_activated_stake)
            delegator_rewards[vote_pubkey] = delegator_reward
            logging.info(
                f"Vote account {vote_pubkey}: Validator reward {validator_rewards[vote_pubkey]}, "
                f"Delegator reward {delegator_reward} (extrapolated from {len(stake_accounts)} stake accounts)"
            )
        else:
            delegator_rewards[vote_pubkey] = 0
            logging.info(f"Vote account {vote_pubkey}: Insufficient stake data for extrapolation (sampled: {total_sampled_stake}, activated: {total_activated_stake})")

    return validator_rewards, delegator_rewards

def calculate_inflation_rewards(vote_account_data: Dict[str, int], epoch: int, available_stake_epochs: Set[int]):
    """Calculate validator and delegator inflation rewards with optimized API calls and parallelism."""
    validator_rewards_by_pubkey = {}
    delegator_rewards_by_pubkey = {}
    
    vote_account_pubkeys = list(vote_account_data.keys())

    if epoch not in available_stake_epochs:
        logging.warning(f"No stake account data available for epoch {epoch}. Processing validator rewards only.")
        stake_accounts_dict = {}
    else:
        stake_accounts_dict = get_all_top_stake_accounts(epoch, vote_account_pubkeys)

    all_pubkeys = vote_account_pubkeys.copy()
    for vote_pubkey in vote_account_pubkeys:
        stake_accounts = stake_accounts_dict.get(vote_pubkey, [])
        all_pubkeys.extend([account[0] for account in stake_accounts])

    all_rewards = get_all_inflation_rewards(all_pubkeys, epoch)

    chunk_size = max(1, len(vote_account_pubkeys) // 32)
    vote_chunks = [vote_account_pubkeys[i:i + chunk_size] for i in range(0, len(vote_account_pubkeys), chunk_size)]

    with ThreadPoolExecutor(max_workers=32) as executor:
        futures = [
            executor.submit(
                process_vote_account_chunk,
                chunk,
                vote_account_data,
                stake_accounts_dict,
                all_rewards,
                epoch
            )
            for chunk in vote_chunks
        ]
        for future in as_completed(futures):
            val_rewards, del_rewards = future.result()
            validator_rewards_by_pubkey.update(val_rewards)
            delegator_rewards_by_pubkey.update(del_rewards)

    return validator_rewards_by_pubkey, delegator_rewards_by_pubkey

def update_inflation_rewards(epoch: int, validator_rewards: Dict[str, int], delegator_rewards: Dict[str, int]):
    """Update validator_stats with both validator and delegator inflation rewards in bulk for all rows."""
    try:
        conn = psycopg2.connect(**db_params)
        cur = conn.cursor()

        reward_data = [
            (vote_pubkey, validator_rewards[vote_pubkey], delegator_rewards[vote_pubkey], epoch)
            for vote_pubkey in validator_rewards.keys()
        ]

        query = """
        UPDATE validator_stats AS vs
        SET 
            validator_inflation_reward = temp.validator_reward,
            delegator_inflation_reward = temp.delegator_reward,
            total_inflation_reward = temp.validator_reward + temp.delegator_reward
        FROM (VALUES %s) AS temp (vote_pubkey, validator_reward, delegator_reward, epoch)
        WHERE vs.epoch = temp.epoch AND vs.vote_account_pubkey = temp.vote_pubkey
        """

        from psycopg2.extras import execute_values
        execute_values(
            cur,
            query,
            reward_data,
            template="(%s, %s, %s, %s)",
            page_size=1000
        )
        conn.commit()

        cur.close()
        conn.close()
        logging.info(f"Successfully updated inflation rewards for epoch {epoch} in bulk")
    except Exception as e:
        logging.error(f"Failed to update inflation rewards: {e}")
        if conn:
            conn.rollback()
            cur.close()
            conn.close()

def get_max_epoch(conn) -> int:
    query = "SELECT MAX(epoch) FROM validator_stats"
    with conn.cursor() as cur:
        cur.execute(query)
        max_epoch = cur.fetchone()[0]
        return max_epoch if max_epoch is not None else 0

def main():
    provided_epoch = int(sys.argv[1]) if len(sys.argv) > 1 else None
    epoch = get_epoch(provided_epoch)
    
    configure_logging(epoch)
    logging.info(f"Processing epoch {epoch}")

    available_stake_epochs = get_available_stake_epochs()
    logging.info(f"Available stake epochs: {sorted(available_stake_epochs)}")

    vote_account_data = get_vote_accounts_and_data(epoch)
    if not vote_account_data:
        logging.error(f"No vote accounts found for epoch {epoch}")
        sys.exit(1)

    validator_rewards, delegator_rewards = calculate_inflation_rewards(vote_account_data, epoch, available_stake_epochs)
    update_inflation_rewards(epoch, validator_rewards, delegator_rewards)

if __name__ == "__main__":
    main()