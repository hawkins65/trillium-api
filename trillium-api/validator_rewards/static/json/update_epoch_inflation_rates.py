import psycopg2
import logging
import sys
import os
import json
from typing import Tuple
from db_config import db_params

# Logging configuration
def configure_logging():
    script_name = os.path.basename(sys.argv[0]).replace('.py', '')
    log_dir = os.path.expanduser('~/log')
    os.makedirs(log_dir, exist_ok=True)
    log_file = os.path.join(log_dir, f"{script_name}.log")    
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
        handlers=[
            logging.FileHandler(log_file),
            logging.StreamHandler()
        ]
    )

def get_epoch_range() -> Tuple[int, int]:
    """
    Get the start and end epoch from command-line arguments or database query, then prompt with defaults.
    If two arguments are passed, use them as defaults without querying the database.
    If no arguments, query min/max epochs from epoch_aggregate_data.
    Prompts with defaults (e.g., 'Enter start epoch number (600):').
    Pressing Enter accepts the defaults. Fails if database query returns no epochs or fails.
    """
    min_epoch = None
    max_epoch = None

    # Check for two command-line arguments (script name + start + end = 3)
    if len(sys.argv) == 3:
        try:
            min_epoch = int(sys.argv[1])
            max_epoch = int(sys.argv[2])
            if min_epoch > max_epoch:
                logging.error("Start epoch must be less than or equal to end epoch")
                sys.exit(1)
            logging.info(f"Using command-line arguments as defaults: min_epoch = {min_epoch}, max_epoch = {max_epoch}")
        except ValueError:
            logging.error("Invalid epoch numbers provided as arguments")
            sys.exit(1)
    elif len(sys.argv) == 1:
        # No arguments; query database for min and max epochs
        try:
            conn = psycopg2.connect(**db_params)
            cur = conn.cursor()
            cur.execute(
                """
                SELECT MIN(epoch), MAX(epoch)
                FROM epoch_aggregate_data
                """
            )
            result = cur.fetchone()
            cur.close()
            conn.close()
            
            if result[0] is None or result[1] is None:
                logging.error("No epochs found in epoch_aggregate_data. Cannot proceed without valid epoch range.")
                sys.exit(1)
            
            min_epoch, max_epoch = result
            logging.info(f"Queried database: min_epoch = {min_epoch}, max_epoch = {max_epoch}")
        except Exception as e:
            logging.error(f"Failed to query epoch range from database: {e}. Cannot proceed without valid epoch range.")
            sys.exit(1)
    else:
        logging.error("Expected two command-line arguments (start and end epoch) or none")
        sys.exit(1)

    # Interactive input with min/max epochs as defaults
    while True:
        try:
            start_input = input(f"Enter start epoch number ({min_epoch}): ").strip()
            start_epoch = int(start_input) if start_input else min_epoch
            
            end_input = input(f"Enter end epoch number ({max_epoch}): ").strip()
            end_epoch = int(end_input) if end_input else max_epoch
            
            if start_epoch <= end_epoch:
                logging.info(f"Selected epochs: start_epoch = {start_epoch}, end_epoch = {end_epoch}")
                return start_epoch, end_epoch
            logging.warning("End epoch must be greater than or equal to start epoch")
            print("End epoch must be greater than or equal to start epoch")
        except ValueError:
            logging.warning("Invalid input provided for epoch numbers")
            print("Please enter valid integers for epochs or press Enter to accept defaults")

def read_epochs_per_year(epoch: int) -> float:
    """
    Read epochs_per_year from the JSON file for the given epoch.
    """
    json_file = os.path.expanduser(f"~/json/epoch{epoch}_epoch_aggregate_data.json")
    try:
        with open(json_file, 'r') as f:
            data = json.load(f)
            epochs_per_year = data.get('epochs_per_year', 183.0)  # Default to 183 if not found
            logging.info(f"Read epochs_per_year = {epochs_per_year} for epoch {epoch}")
            return epochs_per_year
    except (FileNotFoundError, json.JSONDecodeError, KeyError) as e:
        logging.warning(f"Could not read epochs_per_year for epoch {epoch}: {e}. Using default 183.0")
        return 183.0

def calculate_inflation_rate(epoch: int, epochs_per_year: float) -> float:
    """
    Calculate the inflation rate for the given epoch based on Solana's inflation schedule.
    """
    try:
        # Inflation parameters
        initial_rate = 0.08  # 8% at epoch 150
        disinflation_rate = 0.15  # 15% annual reduction
        long_term_rate = 0.015  # 1.5% terminal rate
        inflation_start_epoch = 150  # Inflation started at epoch 150

        # Calculate epoch-years since inflation start
        epochs_since_start = max(epoch - inflation_start_epoch, 0)  # Ensure non-negative
        epoch_years = epochs_since_start / epochs_per_year  # Fractional epoch-year for precision

        # Calculate inflation rate
        annual_rate = initial_rate * (1 - disinflation_rate) ** epoch_years
        inflation_rate = max(annual_rate, long_term_rate)
        logging.info(f"Calculated inflation rate for epoch {epoch}: {inflation_rate * 100:.4f}%")
        return inflation_rate
    except Exception as e:
        logging.error(f"Failed to calculate inflation rate for epoch {epoch}: {e}")
        return 0.0

def update_epoch_aggregate_data(epoch: int, inflation_rate: float, inflation_decay_rate: float = 0.15):
    """
    Update inflation_decay_rate and inflation_rate in epoch_aggregate_data for the given epoch.
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
            (inflation_decay_rate, inflation_rate, epoch)
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
                (epoch, inflation_decay_rate, inflation_rate)
            )
        conn.commit()
        cur.close()
        conn.close()
        logging.info(f"Updated epoch_aggregate_data for epoch {epoch}: "
                     f"inflation_decay_rate = {inflation_decay_rate}, inflation_rate = {inflation_rate}")
    except Exception as e:
        logging.error(f"Failed to update epoch_aggregate_data for epoch {epoch}: {e}")

def main():
    configure_logging()
    start_epoch, end_epoch = get_epoch_range()
    logging.info(f"Processing epochs {start_epoch} to {end_epoch}")

    for epoch in range(start_epoch, end_epoch + 1):
        # Skip epochs before inflation started
        if epoch < 150:
            logging.info(f"Skipping epoch {epoch}: Inflation started at epoch 150")
            continue

        # Read epochs_per_year from JSON
        epochs_per_year = read_epochs_per_year(epoch)

        # Calculate inflation rate
        inflation_rate = calculate_inflation_rate(epoch, epochs_per_year)
        if inflation_rate == 0.0:
            logging.error(f"Skipping epoch {epoch} due to calculation failure")
            continue

        # Update database with inflation_decay_rate (fixed at 0.15) and inflation_rate
        update_epoch_aggregate_data(epoch, inflation_rate, inflation_decay_rate=0.15)

if __name__ == "__main__":
    main()