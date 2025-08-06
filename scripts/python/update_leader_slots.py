# Standard Library Imports
import importlib.util
import os

# Setup unified logging
script_dir = os.path.dirname(os.path.abspath(__file__))
logging_config_path = os.path.join(script_dir, "999_logging_config.py")
spec = importlib.util.spec_from_file_location("logging_config", logging_config_path)
logging_config = importlib.util.module_from_spec(spec)
spec.loader.exec_module(logging_config)
logger = logging_config.setup_logging(os.path.basename(__file__).replace('.py', ''))
import sys
from datetime import datetime

# Third-Party Library Imports
import psycopg2
from psycopg2 import sql
from psycopg2.extras import execute_batch

# PostgreSQL database connection parameters
from db_config import db_params

# Logger setup
def setup_logging():
    # Logger setup moved to unified configuration
    logger.setLevel(logging.DEBUG)

    now = datetime.now()
    formatted_time = now.strftime('%Y-%m-%d_%H-%M')
    script_name = 'update_leader_slots'
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

def verify_epoch_exists(conn, epoch):
    cur = conn.cursor()
    try:
        cur.execute("SELECT EXISTS(SELECT 1 FROM validator_stats WHERE epoch = %s)", (epoch,))
        exists = cur.fetchone()[0]
        return exists
    finally:
        cur.close()

def get_epoch_range(conn):
    cur = conn.cursor()
    try:
        cur.execute("SELECT MIN(epoch), MAX(epoch) FROM validator_stats")
        min_epoch, max_epoch = cur.fetchone()
        return min_epoch, max_epoch
    finally:
        cur.close()

def main():
    conn = get_db_connection(db_params)
    try:
        # Check if epoch parameter is provided
        if len(sys.argv) < 2:
            min_epoch, max_epoch = get_epoch_range(conn)
            print(f"Error: No epoch parameter provided.")
            print(f"Please provide an epoch number between {min_epoch} and {max_epoch}")
            print(f"Usage: python update_leader_slots.py <epoch>")
            sys.exit(1)

        # Get and validate epoch parameter
        try:
            epoch = int(sys.argv[1])
        except ValueError:
            min_epoch, max_epoch = get_epoch_range(conn)
            print(f"Error: Invalid epoch number. Please provide a valid integer.")
            print(f"Available epoch range: {min_epoch} to {max_epoch}")
            sys.exit(1)

        # Verify epoch exists
        if not verify_epoch_exists(conn, epoch):
            min_epoch, max_epoch = get_epoch_range(conn)
            print(f"Error: Epoch {epoch} does not exist in validator_stats.")
            print(f"Available epoch range: {min_epoch} to {max_epoch}")
            sys.exit(1)

        print(f"Updating leader_slots for epoch {epoch}")
        logger.info(f"Updating leader_slots for epoch {epoch}")
        update_validator_skip_rates(conn, epoch)

    except Exception as e:
        logger.error(f"Error in main execution: {e}")
        sys.exit(1)
    finally:
        conn.close()

if __name__ == '__main__':
    main()