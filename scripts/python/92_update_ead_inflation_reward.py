import psycopg2
import logging
import sys
import os
import subprocess
import json
from typing import Tuple
from db_config import db_params

# Logging configuration
def configure_logging(epoch: int):
    script_name = os.path.basename(sys.argv[0]).replace('.py', '')
    log_dir = os.path.expanduser('~/log')
    log_file = os.path.join(log_dir, f"{script_name}.log")    
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
        handlers=[
            logging.FileHandler(log_file),
            logging.StreamHandler()
        ]
    )

def get_epoch(provided_epoch: int = None) -> int:
    if provided_epoch is not None:
        return provided_epoch
    while True:
        try:
            return int(input("Enter epoch number: "))
        except ValueError:
            print("Please enter a valid integer for epoch")

def get_rewards_sums(epoch: int) -> Tuple[float, float, float]:
    """
    Calculate the sum of validator inflation, delegator inflation, and block rewards for the given epoch.
    """
    try:
        conn = psycopg2.connect(**db_params)
        cur = conn.cursor()
        cur.execute(
            """
            SELECT 
                COALESCE(SUM(validator_inflation_reward), 0) AS validator_sum,
                COALESCE(SUM(delegator_inflation_reward), 0) AS delegator_sum,
                COALESCE(SUM(rewards), 0) AS block_sum
            FROM validator_stats 
            WHERE epoch = %s 
            AND (validator_inflation_reward IS NOT NULL 
                 OR delegator_inflation_reward IS NOT NULL 
                 OR rewards IS NOT NULL)
            """, 
            (epoch,)
        )
        validator_sum, delegator_sum, block_sum = cur.fetchone()
        cur.close()
        conn.close()
        logging.info(f"Epoch {epoch}: Validator inflation sum = {validator_sum}, "
                     f"Delegator inflation sum = {delegator_sum}, Block rewards sum = {block_sum}")
        return validator_sum, delegator_sum, block_sum
    except Exception as e:
        logging.error(f"Failed to fetch rewards sums: {e}")
        return 0, 0, 0

def fetch_and_parse_inflation_data() -> Tuple[int, float, float]:
    """
    Execute solana inflation command and parse the JSON output to extract epoch, taper, and validator rate.
    """
    try:
        result = subprocess.run(
            ['solana', 'inflation', '--output', 'json'],
            capture_output=True,
            text=True,
            check=True
        )
        inflation_data = json.loads(result.stdout)
        epoch = inflation_data['currentRate']['epoch']
        taper = inflation_data['governor']['taper']
        validator_rate = inflation_data['currentRate']['validator']
        logging.info(f"Fetched inflation data for epoch {epoch}: taper = {taper}, validator_rate = {validator_rate}")
        return epoch, taper, validator_rate
    except subprocess.CalledProcessError as e:
        logging.error(f"Failed to execute solana inflation command: {e}")
        return 0, 0.0, 0.0
    except (json.JSONDecodeError, KeyError) as e:
        logging.error(f"Failed to parse inflation data: {e}")
        return 0, 0.0, 0.0

def update_rewards_data(epoch: int, validator_sum: float, delegator_sum: float, block_sum: float):
    """
    Update total_validator_inflation_rewards, total_delegator_inflation_rewards, and total_block_rewards
    in epoch_aggregate_data for the specified epoch.
    """
    try:
        conn = psycopg2.connect(**db_params)
        cur = conn.cursor()
        cur.execute(
            """
            UPDATE epoch_aggregate_data
            SET 
                total_validator_inflation_rewards = %s,
                total_delegator_inflation_rewards = %s,
                total_block_rewards = %s
            WHERE epoch = %s
            """,
            (validator_sum, delegator_sum, block_sum, epoch)
        )
        if cur.rowcount == 0:
            cur.execute(
                """
                INSERT INTO epoch_aggregate_data (epoch, total_validator_inflation_rewards, 
                                                 total_delegator_inflation_rewards, total_block_rewards)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (epoch) DO UPDATE 
                SET 
                    total_validator_inflation_rewards = EXCLUDED.total_validator_inflation_rewards,
                    total_delegator_inflation_rewards = EXCLUDED.total_delegator_inflation_rewards,
                    total_block_rewards = EXCLUDED.total_block_rewards
                """,
                (epoch, validator_sum, delegator_sum, block_sum)
            )
        conn.commit()
        cur.close()
        conn.close()
        logging.info(f"Updated rewards data for epoch {epoch}: "
                     f"total_validator_inflation_rewards = {validator_sum}, "
                     f"total_delegator_inflation_rewards = {delegator_sum}, "
                     f"total_block_rewards = {block_sum}")
    except Exception as e:
        logging.error(f"Failed to update rewards data: {e}")

def update_inflation_data(inflation_epoch: int, taper: float, validator_rate: float):
    """
    Update inflation_decay_rate and inflation_rate in epoch_aggregate_data for the specified inflation epoch.
    """
    try:
        conn = psycopg2.connect(**db_params)
        cur = conn.cursor()
        cur.execute(
            """
            UPDATE epoch_aggregate_data
            SET 
                inflation_decay_rate = %s,
                inflation_rate = %s
            WHERE epoch = %s
            """,
            (taper, validator_rate, inflation_epoch)
        )
        if cur.rowcount == 0:
            cur.execute(
                """
                INSERT INTO epoch_aggregate_data (epoch, inflation_decay_rate, inflation_rate)
                VALUES (%s, %s, %s)
                ON CONFLICT (epoch) DO UPDATE 
                SET 
                    inflation_decay_rate = EXCLUDED.inflation_decay_rate,
                    inflation_rate = EXCLUDED.inflation_rate
                """,
                (inflation_epoch, taper, validator_rate)
            )
        conn.commit()
        cur.close()
        conn.close()
        logging.info(f"Updated inflation data for epoch {inflation_epoch}: "
                     f"inflation_decay_rate = {taper}, inflation_rate = {validator_rate}")
    except Exception as e:
        logging.error(f"Failed to update inflation data: {e}")

def main():
    provided_epoch = int(sys.argv[1]) if len(sys.argv) > 1 else None
    epoch = get_epoch(provided_epoch)
    configure_logging(epoch)

    logging.info(f"Processing epoch {epoch}")

    # Calculate sums of rewards
    validator_sum, delegator_sum, block_sum = get_rewards_sums(epoch)
    if validator_sum == 0 and delegator_sum == 0 and block_sum == 0:
        logging.error(f"No valid rewards data found for epoch {epoch}")
        sys.exit(1)

    # Update rewards data for the current epoch
    update_rewards_data(epoch, validator_sum, delegator_sum, block_sum)

    # Fetch inflation data
    inflation_epoch, taper, validator_rate = fetch_and_parse_inflation_data()
    if inflation_epoch == 0:
        logging.error("No valid inflation data retrieved")
        sys.exit(1)

    # Update inflation data for the inflation epoch
    update_inflation_data(inflation_epoch, taper, validator_rate)

if __name__ == "__main__":
    main()