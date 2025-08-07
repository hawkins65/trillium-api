import psycopg2
import numpy as np
from scipy import stats
import importlib.util
import os

# Setup unified logging
script_dir = os.path.dirname(os.path.abspath(__file__))
logging_config_path = os.path.join(script_dir, "999_logging_config.py")
spec = importlib.util.spec_from_file_location("logging_config", logging_config_path)
logging_config = importlib.util.module_from_spec(spec)
spec.loader.exec_module(logging_config)
logger = logging_config.setup_logging(os.path.basename(__file__).replace('.py', ''))
import argparse
from decimal import Decimal
from db_config import db_params

# Configure logging
# Logging config moved to unified configuration
# Logger setup moved to unified configuration

# Client type mapping
CLIENT_TYPE_MAP = {
    0: 'Solana Labs',
    1: 'Jito Labs',
    2: 'Firedancer',
    3: 'Agave',
    4: 'Paladin',
    None: 'Unknown'
}

def get_db_connection_string(db_params):
    return f"postgresql://{db_params['user']}@{db_params['host']}:{db_params['port']}/{db_params['database']}?sslmode={db_params['sslmode']}"

def get_db_connection(db_params):
    try:
        conn = psycopg2.connect(**db_params)
        logger.debug(f"Created new DB connection: {conn}")
        return conn
    except psycopg2.Error as e:
        logger.error(f"Failed to connect to database: {e}")
        raise

def get_epoch_range():
    """
    Retrieves the minimum and maximum epoch numbers from the validator_stats_slot_duration table.
    
    Returns:
        tuple: (min_epoch, max_epoch) or (None, None) if no data is found.
    """
    try:
        with get_db_connection(db_params) as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT MIN(epoch), MAX(epoch)
                    FROM validator_stats_slot_duration;
                """)
                result = cur.fetchone()
                return result if result else (None, None)
    except psycopg2.Error as e:
        logger.error(f"Error retrieving epoch range: {e}")
        return None, None

def analyze_validator_performance(epoch, confidence_level=0.95):
    """
    Analyzes validator performance for a given epoch, calculating p-values and confidence intervals
    using stratified t-tests by metro (or city) and client_type, and updates the validator_stats_slot_duration table.

    Args:
        epoch (int): The epoch number to analyze.
        confidence_level (float): Confidence level for the confidence interval (default: 0.95).

    Returns:
        bool: True if successful, False otherwise.
    """
    db_connection_string = get_db_connection_string(db_params)
    
    try:
        with get_db_connection(db_params) as conn:
            with conn.cursor() as cur:
                # Query to get population mean for each metro-client_type combination
                cur.execute("""
                    SELECT 
                        COALESCE(vs.metro, vs.city) AS location,
                        vs.client_type,
                        AVG(vss.slot_duration_mean) AS group_mean
                    FROM validator_stats_slot_duration vss
                    LEFT JOIN validator_stats vs ON vss.identity_pubkey = vs.identity_pubkey AND vss.epoch = vs.epoch
                    WHERE vss.epoch = %s
                    GROUP BY COALESCE(vs.metro, vs.city), vs.client_type;
                """, (epoch,))
                group_means = {(row[0], row[1]): float(row[2]) if row[2] is not None else None for row in cur.fetchall()}
                if not group_means:
                    logger.error(f"No group means found for epoch {epoch}")
                    return False

                # Query to get validator stats, including count of slots, raw durations, metro (or city), and client_type
                cur.execute("""
                    SELECT 
                        vss.identity_pubkey,
                        vss.slot_duration_mean,
                        vss.slot_duration_stddev,
                        COUNT(*) AS slot_count,
                        ARRAY_AGG(sd.duration) AS slot_durations,
                        COALESCE(vs.metro, vs.city) AS location,
                        vs.client_type
                    FROM validator_stats_slot_duration vss
                    JOIN leader_schedule ls ON vss.identity_pubkey = ls.identity_pubkey AND vss.epoch = ls.epoch
                    JOIN slot_duration sd ON ls.epoch = sd.epoch AND ls.block_slot = sd.block_slot
                    LEFT JOIN validator_stats vs ON vss.identity_pubkey = vs.identity_pubkey AND vss.epoch = vs.epoch
                    WHERE vss.epoch = %s
                        AND ls.block_produced = true
                        AND sd.duration > 0
                    GROUP BY vss.identity_pubkey, vss.slot_duration_mean, vss.slot_duration_stddev, COALESCE(vs.metro, vs.city), vs.client_type;
                """, (epoch,))
                validator_stats = cur.fetchall()

                if not validator_stats:
                    logger.error(f"No validator stats found for epoch {epoch}")
                    return False

                # Calculate Bonferroni-corrected p-value threshold
                p_value_threshold = 0.05 / len(validator_stats) if validator_stats else 0.05
                logger.info(f"Using p-value threshold: {p_value_threshold:.7f}")

                # Process each validator and prepare updates
                update_data = []
                for identity_pubkey, mean_duration, stddev_duration, slot_count, slot_durations, location, client_type in validator_stats:
                    # Convert Decimal to float for calculations
                    mean_duration = float(mean_duration) if isinstance(mean_duration, Decimal) else mean_duration
                    stddev_duration = float(stddev_duration) if isinstance(stddev_duration, Decimal) else stddev_duration

                    # Convert to milliseconds for calculations
                    mean_duration_ms = mean_duration / 1_000_000.0

                    # Convert slot durations to float for t-test
                    slot_durations = [float(d) for d in slot_durations if d is not None]

                    # Calculate standard error from raw durations
                    standard_error = stddev_duration / np.sqrt(slot_count) if stddev_duration and slot_count > 0 else 0

                    # Get group-specific population mean
                    group_key = (location, client_type)
                    population_mean = group_means.get(group_key, mean_duration)  # Fallback to validator’s mean if no group mean

                    # Perform one-sample t-test using raw slot durations
                    if slot_count > 1 and stddev_duration > 0 and slot_durations:
                        t_stat, p_value = stats.ttest_1samp(
                            slot_durations,
                            popmean=population_mean,
                            alternative='greater'
                        )
                        p_value = float(p_value) / 2 if p_value is not None else None  # Convert np.float64 to Python float
                    else:
                        t_stat, p_value = None, None
                        logger.warning(f"Validator {identity_pubkey} skipped for t-test: "
                                      f"slot_count={slot_count}, stddev={stddev_duration}")

                    # Calculate confidence interval
                    z_score = float(stats.norm.ppf(1 - (1 - confidence_level) / 2))  # Convert np.float64 to Python float
                    ci_lower = mean_duration - z_score * standard_error if standard_error > 0 else mean_duration
                    ci_upper = mean_duration + z_score * standard_error if standard_error > 0 else mean_duration
                    ci_lower_ms = float(round(ci_lower / 1_000_000.0, 2))  # Convert to Python float
                    ci_upper_ms = float(round(ci_upper / 1_000_000.0, 2))  # Convert to Python float

                    # Determine if validator is lagging (p-value < threshold), ensure native bool
                    is_lagging = bool(p_value is not None and p_value < p_value_threshold)

                    # Collect data for database update
                    update_data.append({
                        "identity_pubkey": identity_pubkey,
                        "p_value": p_value,
                        "ci_lower_ms": ci_lower_ms,
                        "ci_upper_ms": ci_upper_ms,
                        "is_lagging": is_lagging
                    })

                # Update validator_stats_slot_duration with p-value, confidence intervals, and lagging status
                cur.executemany("""
                    UPDATE validator_stats_slot_duration
                    SET slot_duration_p_value = %s,
                        slot_duration_confidence_interval_lower_ms = %s,
                        slot_duration_confidence_interval_upper_ms = %s,
                        slot_duration_is_lagging = %s
                    WHERE epoch = %s AND identity_pubkey = %s;
                """, [(d["p_value"], d["ci_lower_ms"], d["ci_upper_ms"], d["is_lagging"], epoch, d["identity_pubkey"]) for d in update_data])
                conn.commit()

                return True

    except psycopg2.Error as e:
        logger.error(f"Database error: {e}")
        return False
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        return False

def main():
    parser = argparse.ArgumentParser(description="Update validator slot duration statistics in the database for a given epoch.")
    parser.add_argument("epoch", type=int, help="Epoch number to analyze", nargs='?', default=None)
    args = parser.parse_args()

    if args.epoch is None:
        min_epoch, max_epoch = get_epoch_range()
        if min_epoch is None or max_epoch is None:
            print("Error: No epoch data found in the validator_stats_slot_duration table.")
            return

        print(f"Available epoch range: {min_epoch} to {max_epoch}")
        print(f"Defaulting to the latest epoch: {max_epoch}")
        epoch = max_epoch
    else:
        epoch = args.epoch

    if analyze_validator_performance(epoch=epoch):
        print(f"Successfully updated validator stats for epoch {epoch}")
    else:
        print(f"Failed to update validator stats for epoch {epoch}")

if __name__ == "__main__":
    main()