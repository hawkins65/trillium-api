# Standard Library Imports
import logging
import sys
from datetime import datetime

# Third-Party Library Imports
import psycopg2
from psycopg2 import sql

# PostgreSQL database connection parameters
from db_config import db_params

# Logger setup
def setup_logging():
    logger = logging.getLogger()
    logger.setLevel(logging.DEBUG)

    now = datetime.now()
    formatted_time = now.strftime('%Y-%m-%d_%H-%M')
    script_name = 'update_leader_schedule_block_produced'
    filename = f'/home/smilax/log/{script_name}_log_{formatted_time}.log'

    # File handler
    fh = logging.FileHandler(filename)
    fh.setLevel(logging.DEBUG)
    fh_formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    fh.setFormatter(fh_formatter)

    # Console handler
    ch = logging.StreamHandler(sys.stdout)
    ch.setLevel(logging.INFO)
    ch_formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    ch.setFormatter(ch_formatter)

    logger.addHandler(fh)
    logger.addHandler(ch)
    return logger

logger = setup_logging()

def get_db_connection(db_params):
    conn = psycopg2.connect(**db_params)
    logger.debug(f"Created new DB connection: {conn}")
    return conn

def update_leader_schedule_block_produced(db_params, epoch):
    logger.info(f"Updating leader_schedule.block_produced for epoch: {epoch}")

    conn = get_db_connection(db_params)
    cur = conn.cursor()

    try:
        # Step 1: Identify and log mismatches where validator_data.identity_pubkey != leader_schedule.identity_pubkey
        cur.execute("""
            SELECT ls.block_slot, ls.identity_pubkey, vd.identity_pubkey
            FROM leader_schedule ls
            INNER JOIN validator_data vd
            ON ls.block_slot = vd.block_slot
            WHERE ls.epoch = %s
            AND ls.identity_pubkey != vd.identity_pubkey
        """, (epoch,))
        mismatches = cur.fetchall()

        if mismatches:
            logger.info(f"{len(mismatches)} Identity pubkey mismatches found in epoch {epoch}")
            for mismatch in mismatches:
                slot, ls_pubkey, vd_pubkey = mismatch
                logger.error(f"Mismatch in epoch {epoch} - Slot: {slot}, leader_schedule.identity_pubkey: {ls_pubkey}, validator_data.identity_pubkey: {vd_pubkey}")
        else:
            logger.info(f"No identity pubkey mismatches found in epoch {epoch}")

        # Step 2: Update block_produced to TRUE where validator_data and leader_schedule match on block_slot and identity_pubkey
        cur.execute("""
            UPDATE leader_schedule ls
            SET block_produced = TRUE
            FROM validator_data vd
            WHERE ls.epoch = %s
            AND ls.block_slot = vd.block_slot
            AND ls.identity_pubkey = vd.identity_pubkey
        """, (epoch,))
        matched_updates = cur.rowcount
        logger.info(f"Set block_produced = TRUE for {matched_updates} matching slots in epoch {epoch}")

        # Step 3: Set block_produced to FALSE for all other slots in leader_schedule for this epoch
        cur.execute("""
            UPDATE leader_schedule ls
            SET block_produced = FALSE
            WHERE ls.epoch = %s
            AND NOT EXISTS (
                SELECT 1
                FROM validator_data vd
                WHERE vd.block_slot = ls.block_slot
                AND vd.identity_pubkey = ls.identity_pubkey
                AND vd.epoch = ls.epoch
            )
        """, (epoch,))
        unmatched_updates = cur.rowcount
        logger.info(f"Set block_produced = FALSE for {unmatched_updates} unmatched slots in epoch {epoch}")

        conn.commit()

        # Step 4: Verify the update
        cur.execute("""
            SELECT COUNT(*) 
            FROM leader_schedule 
            WHERE epoch = %s AND block_produced = TRUE
        """, (epoch,))
        produced_count = cur.fetchone()[0]
        logger.info(f"Number of block_produced = TRUE entries in leader_schedule for epoch {epoch}: {produced_count}")

        cur.execute("""
            SELECT COUNT(*) 
            FROM leader_schedule 
            WHERE epoch = %s
        """, (epoch,))
        total_count = cur.fetchone()[0]
        logger.info(f"Total number of leader_schedule entries for epoch {epoch}: {total_count}")

    except Exception as e:
        logger.error(f"An error occurred while updating leader_schedule for epoch {epoch}: {e}")
        conn.rollback()
        raise
    finally:
        cur.close()
        conn.close()

def main():
    # Check if epoch number is provided as a command-line argument
    if len(sys.argv) != 2:
        print("Error: Please provide a single epoch number as an argument.")
        print("Usage: python update_leader_schedule_block_produced.py <epoch>")
        sys.exit(1)

    try:
        epoch = int(sys.argv[1])
    except ValueError:
        print("Error: Epoch must be a valid integer.")
        sys.exit(1)

    conn = get_db_connection(db_params)
    cur = conn.cursor()

    try:
        # Get the highest and lowest epoch numbers from validator_data
        cur.execute("SELECT MAX(epoch), MIN(epoch) FROM validator_data")
        max_epoch, min_epoch = cur.fetchone()

        if max_epoch is None or min_epoch is None:
            print("Error: No epochs found in validator_data.")
            logger.error("No epochs found in validator_data.")
            sys.exit(1)

        # Verify the provided epoch is within range
        if epoch < min_epoch or epoch > max_epoch:
            print(f"Error: Epoch {epoch} is out of range. Must be between {min_epoch} and {max_epoch}.")
            logger.error(f"Epoch {epoch} is out of range. Must be between {min_epoch} and {max_epoch}.")
            sys.exit(1)

        print(f"Updating leader_schedule.block_produced for epoch {epoch}")
        #logger.info(f"Updating leader_schedule.block_produced for epoch {epoch}")
        update_leader_schedule_block_produced(db_params, epoch)

    except Exception as e:
        logger.error(f"Error in main execution: {e}")
        sys.exit(1)
    finally:
        cur.close()
        conn.close()

if __name__ == '__main__':
    main()