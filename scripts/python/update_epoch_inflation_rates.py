import psycopg2
import importlib.util

# Setup unified logging
script_dir = os.path.dirname(os.path.abspath(__file__))
logging_config_path = os.path.join(script_dir, "999_logging_config.py")
spec = importlib.util.spec_from_file_location("logging_config", logging_config_path)
logging_config = importlib.util.module_from_spec(spec)
spec.loader.exec_module(logging_config)
logger = logging_config.setup_logging(os.path.basename(__file__).replace('.py', ''))
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
    # Logging config moved to unified configurations - %(levelname)s - %(message)s",
        handlers=[
            logging.FileHandler(log_file),
            logging.StreamHandler()
        ]
    )

def get_epoch_range() -> Tuple[int, int]:
    """
    Get the start and end epoch from command-line arguments or user input.
    """
    if len(sys.argv) == 3:
        try:
            start_epoch = int(sys.argv[1])
            end_epoch = int(sys.argv[2])
            return start_epoch, end_epoch
        except ValueError:
            logger.error("Invalid epoch numbers provided as arguments")
            sys.exit(1)
    while True:
        try:
            start_epoch = int(input("Enter start epoch number: "))
            end_epoch = int(input("Enter end epoch number: "))
            if start_epoch <= end_epoch:
                return start_epoch, end_epoch
            print("End epoch must be greater than or equal to start epoch")
        except ValueError:
            print("Please enter valid integers for epochs")

def read_epochs_per_year(epoch: int) -> float:
    """
    Read epochs_per_year from the JSON file for the given epoch.
    """
    json_file = os.path.expanduser(f"~/json/epoch{epoch}_epoch_aggregate_data.json")
    try:
        with open(json_file, 'r') as f:
            data = json.load(f)
            epochs_per_year = data.get('epochs_per_year', 183.0)  # Default to 183 if not found
            logger.info(f"Read epochs_per_year = {epochs_per_year} for epoch {epoch}")
            return epochs_per_year
    except (FileNotFoundError, json.JSONDecodeError, KeyError) as e:
        logger.warning(f"Could not read epochs_per_year for epoch {epoch}: {e}. Using default 183.0")
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
        epochs_since_start = epoch - inflation_start_epoch
        epoch_years = epochs_since_start / epochs_per_year  # Fractional epoch-year for precision

        # Calculate inflation rate
        annual_rate = initial_rate * (1 - disinflation_rate) ** epoch_years
        inflation_rate = max(annual_rate, long_term_rate)
        logger.info(f"Calculated inflation rate for epoch {epoch}: {inflation_rate * 100:.4f}%")
        return inflation_rate
    except Exception as e:
        logger.error(f"Failed to calculate inflation rate for epoch {epoch}: {e}")
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
        logger.info(f"Updated epoch_aggregate_data for epoch {epoch}: "
                     f"inflation_decay_rate = {inflation_decay_rate}, inflation_rate = {inflation_rate}")
    except Exception as e:
        logger.error(f"Failed to update epoch_aggregate_data for epoch {epoch}: {e}")

def main():
    configure_logging()
    start_epoch, end_epoch = get_epoch_range()
    logger.info(f"Processing epochs {start_epoch} to {end_epoch}")

    for epoch in range(start_epoch, end_epoch + 1):
        # Skip epochs before inflation started
        if epoch < 150:
            logger.info(f"Skipping epoch {epoch}: Inflation started at epoch 150")
            continue

        # Read epochs_per_year from JSON
        epochs_per_year = read_epochs_per_year(epoch)

        # Calculate inflation rate
        inflation_rate = calculate_inflation_rate(epoch, epochs_per_year)
        if inflation_rate == 0.0:
            logger.error(f"Skipping epoch {epoch} due to calculation failure")
            continue

        # Update database with inflation_decay_rate (fixed at 0.15) and inflation_rate
        update_epoch_aggregate_data(epoch, inflation_rate, inflation_decay_rate=0.15)

if __name__ == "__main__":
    main()