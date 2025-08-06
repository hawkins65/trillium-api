import psycopg2
from decimal import Decimal
from typing import Dict, List
from db_config import db_params
import sys
import os
import statistics
from math import isinf, isnan
from logging_config import setup_logging

LAMPORTS_PER_SOL = 1_000_000_000

# Setup logging
script_name = os.path.basename(__file__).replace('.py', '')
logger = setup_logging(script_name)

def validate_result(result):
    for key, value in result.items():
        if key in ['epoch', 'vote_account_pubkey']:
            continue
        if value is not None and (isnan(value) or isinf(value) or abs(value) >= 1e20):
            return False, key
    return True, None

def update_validator_stats(epoch: int, results: List[Dict]):
    logger.info("Updating validator_stats with calculated APYs")
    try:
        conn = psycopg2.connect(**db_params)
        cur = conn.cursor()

        # Set all APY columns to NULL for the epoch
        cur.execute(
            """
            UPDATE validator_stats
            SET 
                delegator_mev_apy = NULL,
                delegator_compound_mev_apy = NULL,
                delegator_total_apy = NULL,
                delegator_compound_total_apy = NULL,
                validator_block_rewards_apy = NULL,
                validator_compound_block_rewards_apy = NULL,
                validator_inflation_apy = NULL,
                validator_compound_inflation_apy = NULL,
                validator_mev_apy = NULL,
                validator_compound_mev_apy = NULL,
                validator_total_apy = NULL,
                validator_compound_total_apy = NULL,
                delegator_inflation_apy = NULL,
                delegator_compound_inflation_apy = NULL,
                total_overall_apy = NULL,
                compound_overall_apy = NULL
            WHERE epoch = %s
            """,
            (epoch,)
        )

        # Update only validators with valid APYs
        for result in results:
            is_valid, invalid_key = validate_result(result)
            if not is_valid:
                logger.error(f"Skipping update for {result['vote_account_pubkey']} in epoch {epoch}: invalid {invalid_key} = {result[invalid_key]}")
                continue
            if 'validator_block_rewards_apy' not in result or result['validator_block_rewards_apy'] is None:
                logger.warning(f"Skipping update for {result['vote_account_pubkey']} in epoch {epoch}: validator_block_rewards_apy is missing or None")
                continue
            #logging.debug(f"Updating validator {result['vote_account_pubkey']} with validator_block_rewards_apy = {result['validator_block_rewards_apy']}")
            cur.execute(
                """
                UPDATE validator_stats
                SET 
                    delegator_mev_apy = %s,
                    delegator_compound_mev_apy = %s,
                    delegator_total_apy = %s,
                    delegator_compound_total_apy = %s,
                    validator_block_rewards_apy = %s,
                    validator_compound_block_rewards_apy = %s,
                    validator_inflation_apy = %s,
                    validator_compound_inflation_apy = %s,
                    validator_mev_apy = %s,
                    validator_compound_mev_apy = %s,
                    validator_total_apy = %s,
                    validator_compound_total_apy = %s,
                    delegator_inflation_apy = %s,
                    delegator_compound_inflation_apy = %s,
                    total_overall_apy = %s,
                    compound_overall_apy = %s
                WHERE epoch = %s AND vote_account_pubkey = %s
                """,
                (
                    result['delegator_mev_apy'],
                    result['delegator_compound_mev_apy'],
                    result['delegator_total_apy'],
                    result['delegator_compound_total_apy'],
                    result['validator_block_rewards_apy'],
                    result['validator_compound_block_rewards_apy'],
                    result['validator_inflation_apy'],
                    result['validator_compound_inflation_apy'],
                    result['validator_mev_apy'],
                    result['validator_compound_mev_apy'],
                    result['validator_total_apy'],
                    result['validator_compound_total_apy'],
                    result['delegator_inflation_apy'],
                    result['delegator_compound_inflation_apy'],
                    result['total_overall_apy'],
                    result['compound_overall_apy'],
                    epoch,
                    result['vote_account_pubkey']
                )
            )

        conn.commit()
        cur.close()
        conn.close()
        logger.info(f"Successfully updated validator_stats for epoch {epoch}.")

    except Exception as e:
        logger.error(f"Failed to update validator_stats: {e}")
        if 'conn' in locals():
            conn.rollback()

def query_data_for_epoch(epoch: int):
    """
    Queries validator statistics for a given epoch, using the previous epoch's activated_stake
    for APY calculations.
    
    Args:
        epoch (int): The epoch to query.
    
    Returns:
        list: A list of dictionaries containing APY data for each vote account.
    
    Raises:
        Exception: If the query fails, logs the error and returns an empty list.
    """
    try:
        conn = psycopg2.connect(**db_params)
        cur = conn.cursor()

        cur.execute(
            """
            WITH previous_stake AS (
                SELECT 
                    vs_curr.vote_account_pubkey,
                    vs_curr.epoch,
                    COALESCE((
                        SELECT MAX(vs_prev.activated_stake) 
                        FROM validator_stats vs_prev 
                        WHERE vs_prev.epoch = vs_curr.epoch - 1 
                        AND vs_prev.vote_account_pubkey = vs_curr.vote_account_pubkey
                    ), 0) AS previous_activated_stake
                FROM 
                    validator_stats vs_curr
                WHERE 
                    vs_curr.epoch = %s
            )
            SELECT 
                vs.epoch, 
                vs.vote_account_pubkey, 
                COALESCE(vs.activated_stake, 0) AS current_activated_stake,
                ps.previous_activated_stake AS activated_stake,
                COALESCE(ead.epochs_per_year, 0) AS epochs_per_year,
                -- Delegator APYs
                CASE 
                    WHEN COALESCE(ps.previous_activated_stake, 0) >= 1000000000000
                    THEN ROUND((COALESCE(vs.delegator_inflation_reward, 0) / 1000000000.0 / 
                            (ps.previous_activated_stake / 1000000000.0)) * ead.epochs_per_year * 100, 2)
                    ELSE NULL
                END AS delegator_inflation_apy,
                CASE 
                    WHEN COALESCE(ps.previous_activated_stake, 0) >= 1000000000000
                    THEN ROUND(((1 + COALESCE(vs.delegator_inflation_reward, 0) / 1000000000.0 / 
                            (ps.previous_activated_stake / 1000000000.0)) ^ ead.epochs_per_year - 1) * 100, 2)
                    ELSE NULL
                END AS delegator_compound_inflation_apy,
                CASE 
                    WHEN COALESCE(ps.previous_activated_stake, 0) >= 1000000000000
                    THEN ROUND(((COALESCE(vs.mev_earned, 0) - COALESCE(vs.mev_to_validator, 0) - 
                            COALESCE(vs.mev_to_jito_tip_router, 0)) / 1000000000.0 / 
                            (ps.previous_activated_stake / 1000000000.0)) * ead.epochs_per_year * 100, 2)
                    ELSE NULL
                END AS delegator_mev_apy,
                CASE 
                    WHEN COALESCE(ps.previous_activated_stake, 0) >= 1000000000000
                    THEN ROUND(((COALESCE(vs.mev_earned, 0) - COALESCE(vs.mev_to_validator, 0) - 
                            COALESCE(vs.mev_to_jito_tip_router, 0)) / 1000000000.0 / 
                            (ps.previous_activated_stake / 1000000000.0)) * ead.epochs_per_year * 100, 2)
                    ELSE NULL
                END AS delegator_compound_mev_apy,
                CASE 
                    WHEN COALESCE(ps.previous_activated_stake, 0) >= 1000000000000
                    THEN ROUND(((COALESCE(vs.delegator_inflation_reward, 0) + 
                            (COALESCE(vs.mev_earned, 0) - COALESCE(vs.mev_to_validator, 0) - 
                            COALESCE(vs.mev_to_jito_tip_router, 0))) / 1000000000.0 / 
                            (ps.previous_activated_stake / 1000000000.0)) * ead.epochs_per_year * 100, 2)
                    ELSE NULL
                END AS delegator_total_apy,
                CASE 
                    WHEN COALESCE(ps.previous_activated_stake, 0) >= 1000000000000
                    THEN ROUND((
                        ((1 + COALESCE(vs.delegator_inflation_reward, 0) / 1000000000.0 / 
                             (ps.previous_activated_stake / 1000000000.0)) ^ ead.epochs_per_year - 1) +
                        ((COALESCE(vs.mev_earned, 0) - COALESCE(vs.mev_to_validator, 0) - 
                          COALESCE(vs.mev_to_jito_tip_router, 0)) / 1000000000.0 / 
                         (ps.previous_activated_stake / 1000000000.0)) * ead.epochs_per_year
                    ) * 100, 2)
                    ELSE NULL
                END AS delegator_compound_total_apy,
                -- Validator APYs
                CASE 
                    WHEN COALESCE(ps.previous_activated_stake, 0) >= 1000000000000
                    THEN ROUND((COALESCE(vs.validator_inflation_reward, 0) / 1000000000.0 / 
                            (ps.previous_activated_stake / 1000000000.0)) * ead.epochs_per_year * 100, 2)
                    ELSE NULL
                END AS validator_inflation_apy,
                CASE 
                    WHEN COALESCE(ps.previous_activated_stake, 0) >= 1000000000000
                    THEN ROUND((COALESCE(vs.mev_to_validator, 0) / 1000000000.0 / 
                            (ps.previous_activated_stake / 1000000000.0)) * ead.epochs_per_year * 100, 2)
                    ELSE NULL
                END AS validator_mev_apy,
                CASE 
                    WHEN COALESCE(ps.previous_activated_stake, 0) >= 1000000000000 
                    AND COALESCE(vs.rewards, 0) > 0 
                    AND ps.previous_activated_stake IS NOT NULL 
                    AND vs.rewards IS NOT NULL
                    THEN ROUND((COALESCE(vs.rewards, 0) / 1000000000.0 / 
                            (ps.previous_activated_stake / 1000000000.0)) * ead.epochs_per_year * 100, 2)
                    ELSE NULL
                END AS validator_block_rewards_apy,
                CASE 
                    WHEN COALESCE(ps.previous_activated_stake, 0) >= 1000000000000
                    THEN ROUND((COALESCE(vs.validator_inflation_reward, 0) / 1000000000.0 / 
                            (ps.previous_activated_stake / 1000000000.0)) * ead.epochs_per_year * 100, 2)
                    ELSE NULL
                END AS validator_compound_inflation_apy,
                CASE 
                    WHEN COALESCE(ps.previous_activated_stake, 0) >= 1000000000000
                    THEN ROUND((COALESCE(vs.mev_to_validator, 0) / 1000000000.0 / 
                            (ps.previous_activated_stake / 1000000000.0)) * ead.epochs_per_year * 100, 2)
                    ELSE NULL
                END AS validator_compound_mev_apy,
                CASE 
                    WHEN COALESCE(ps.previous_activated_stake, 0) >= 1000000000000 
                    AND COALESCE(vs.rewards, 0) > 0 
                    AND ps.previous_activated_stake IS NOT NULL 
                    AND vs.rewards IS NOT NULL
                    THEN ROUND((COALESCE(vs.rewards, 0) / 1000000000.0 / 
                            (ps.previous_activated_stake / 1000000000.0)) * ead.epochs_per_year * 100, 2)
                    ELSE NULL
                END AS validator_compound_block_rewards_apy,
                CASE 
                    WHEN COALESCE(ps.previous_activated_stake, 0) >= 1000000000000
                    THEN ROUND((
                        (COALESCE(vs.validator_inflation_reward, 0) + 
                         COALESCE(vs.mev_to_validator, 0) + 
                         COALESCE(vs.rewards, 0)) / 1000000000.0 / 
                        (ps.previous_activated_stake / 1000000000.0)
                    ) * ead.epochs_per_year * 100, 2)
                    ELSE NULL
                END AS validator_total_apy,
                CASE 
                    WHEN COALESCE(ps.previous_activated_stake, 0) >= 1000000000000
                    THEN ROUND((
                        ((1 + COALESCE(vs.validator_inflation_reward, 0) / 1000000000.0 / 
                             (ps.previous_activated_stake / 1000000000.0)) ^ ead.epochs_per_year - 1) +
                        ((COALESCE(vs.mev_to_validator, 0) + COALESCE(vs.rewards, 0)) / 1000000000.0 / 
                         (ps.previous_activated_stake / 1000000000.0)) * ead.epochs_per_year
                    ) * 100, 2)
                    ELSE NULL
                END AS validator_compound_total_apy,
                -- Overall APYs
                CASE 
                    WHEN COALESCE(ps.previous_activated_stake, 0) >= 1000000000000
                    THEN ROUND((
                        (COALESCE(vs.delegator_inflation_reward, 0) + 
                         COALESCE(vs.validator_inflation_reward, 0) + 
                         COALESCE(vs.mev_earned, 0) - 
                         COALESCE(vs.mev_to_jito_tip_router, 0) + 
                         COALESCE(vs.rewards, 0)) / 1000000000.0 / 
                        (ps.previous_activated_stake / 1000000000.0)
                    ) * ead.epochs_per_year * 100, 2)
                    ELSE NULL
                END AS total_overall_apy,
                CASE 
                    WHEN COALESCE(ps.previous_activated_stake, 0) >= 1000000000000
                    THEN ROUND((
                        ((1 + (COALESCE(vs.delegator_inflation_reward, 0) + 
                               COALESCE(vs.validator_inflation_reward, 0)) / 1000000000.0 / 
                              (ps.previous_activated_stake / 1000000000.0)) ^ ead.epochs_per_year - 1) +
                        ((COALESCE(vs.mev_earned, 0) - COALESCE(vs.mev_to_jito_tip_router, 0) + 
                          COALESCE(vs.rewards, 0)) / 1000000000.0 / 
                         (ps.previous_activated_stake / 1000000000.0)) * ead.epochs_per_year
                    ) * 100, 2)
                    ELSE NULL
                END AS compound_overall_apy
            FROM 
                validator_stats vs
            JOIN 
                epoch_aggregate_data ead
                ON vs.epoch = ead.epoch
            JOIN 
                previous_stake ps
                ON vs.vote_account_pubkey = ps.vote_account_pubkey
                AND vs.epoch = ps.epoch
            WHERE 
                vs.epoch = %s
                AND vs.activated_stake IS NOT NULL
                AND vs.activated_stake > 0
            ORDER BY 
                vs.vote_account_pubkey;
            """,
            (epoch, epoch)
        )

        rows = cur.fetchall()
        logger.info(f"Fetched {len(rows)} rows for epoch {epoch}")
        results = []
        for row in rows:
            if row[2] is None or row[2] == 0 or row[4] is None or row[4] == 0:
                logger.warning(f"Invalid data for {row[1]} in epoch {row[0]}: stake={row[2]}, epochs_per_year={row[4]}")
                continue
            apys = {
                'epoch': row[0],
                'vote_account_pubkey': row[1],
                'delegator_inflation_apy': row[5],
                'delegator_compound_inflation_apy': row[6],
                'delegator_mev_apy': row[7],
                'delegator_compound_mev_apy': row[8],
                'delegator_total_apy': row[9],
                'delegator_compound_total_apy': row[10],
                'validator_inflation_apy': row[11],
                'validator_mev_apy': row[12],
                'validator_block_rewards_apy': row[13],
                'validator_compound_inflation_apy': row[14],
                'validator_compound_mev_apy': row[15],
                'validator_compound_block_rewards_apy': row[16],
                'validator_total_apy': row[17],
                'validator_compound_total_apy': row[18],
                'total_overall_apy': row[19],
                'compound_overall_apy': row[20],
            }
            activated_stake = row[2] / LAMPORTS_PER_SOL
            epochs_per_year = Decimal(row[4])

            results.append(apys)
            logger.debug(f"APYs for {row[1]} in epoch {row[0]}: {apys}")

        logger.info(f"Processed {len(results)} valid results for epoch {epoch}")
        cur.close()
        conn.close()
        return results

    except Exception as e:
        logger.error(f"Error querying data for epoch {epoch}: {e}")
        return []
    
def validate_epoch(epoch: int):
    try:
        conn = psycopg2.connect(**db_params)
        cur = conn.cursor()
        cur.execute("SELECT COUNT(*) FROM epoch_aggregate_data WHERE epoch = %s;", (epoch,))
        count = cur.fetchone()[0]
        cur.close()
        conn.close()
        return count > 0
    except Exception as e:
        logging.error(f"Error validating epoch {epoch}: {e}")
        sys.exit(1)

def calculate_summary_statistics(results: List[Dict]):
    keys = [
        'delegator_mev_apy', 'delegator_compound_mev_apy', 
        'delegator_total_apy', 'delegator_compound_total_apy',
        'validator_block_rewards_apy', 'validator_compound_block_rewards_apy',
        'validator_inflation_apy', 'validator_compound_inflation_apy',
        'validator_mev_apy', 'validator_compound_mev_apy',
        'validator_total_apy', 'validator_compound_total_apy',
        'delegator_inflation_apy', 'delegator_compound_inflation_apy',
        'total_overall_apy', 'compound_overall_apy'
    ]

    for key in keys:
        values = [float(result.get(key, 0)) for result in results if result.get(key) is not None]
        if values:
            mean = statistics.mean(values)
            median = statistics.median(values)
            minimum = min(values)
            maximum = max(values)
            logger.debug(f"{key}: Mean = {mean:.2f}%, Median = {median:.2f}%, Min = {minimum:.2f}%, Max = {maximum:.2f}%")
        else:
            logger.debug(f"{key}: No valid values found.")

def get_epoch_from_user():
    try:
        conn = psycopg2.connect(**db_params)
        cur = conn.cursor()
        cur.execute("SELECT MIN(epoch), MAX(epoch) FROM epoch_aggregate_data;")
        min_epoch, max_epoch = cur.fetchone()
        logger.info(f"Please enter an epoch number between {min_epoch} and {max_epoch}: ")
        while True:
            try:
                epoch = int(input("Epoch: "))
                if min_epoch <= epoch <= max_epoch:
                    return epoch
                logger.info(f"Epoch must be between {min_epoch} and {max_epoch}. Try again.")
            except ValueError:
                logger.info("Invalid input. Please enter a valid integer epoch.")
    except Exception as e:
        logger.error(f"Error retrieving epoch range: {e}")
        sys.exit(1)

def main():
    if len(sys.argv) > 1:
        try:
            epoch = int(sys.argv[1])
            if not validate_epoch(epoch):
                logger.error(f"Epoch {epoch} does not exist in the database.")
                sys.exit(1)
        except ValueError:
            logger.error("Invalid epoch number passed. Please enter a valid integer.")
            epoch = get_epoch_from_user()
    else:
        epoch = get_epoch_from_user()

    logger.info(f"Calculating APYs for all vote accounts in epoch {epoch}...")

    results = query_data_for_epoch(epoch)
    if results:
        update_validator_stats(epoch, results)
        calculate_summary_statistics(results)
    else:
        logger.warning(f"No data found for epoch {epoch}.")

if __name__ == "__main__":
    main()