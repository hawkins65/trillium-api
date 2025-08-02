import psycopg2
import json
import logging
import sys
import os
from decimal import Decimal
try:
    from db_config import db_params
except ImportError as e:
    print(f"Error importing db_config: {e}")
    sys.stdout.flush()
    sys.exit(1)

# Enable debug logging
DEBUG = False

# Configure logging with a file handler
LOG_FILE = os.path.expanduser("~/log/92_update_ead_slot_duration_stats.log")  # Use ~/log/
try:
    # Ensure log directory exists
    log_dir = os.path.dirname(LOG_FILE)
    if not os.path.exists(log_dir):
        os.makedirs(log_dir)
    logging.basicConfig(
        level=logging.DEBUG if DEBUG else logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(LOG_FILE, mode='a'),  # Append mode
            logging.StreamHandler(sys.stdout)  # Console output
        ]
    )
except Exception as e:
    print(f"Error configuring logging: {e}")
    sys.stdout.flush()
    sys.exit(1)
logger = logging.getLogger(__name__)

# Test logging and console output
try:
    logger.info("Script started")
    if DEBUG:
        logger.debug("Debug mode enabled")
    print("Script started")
    if DEBUG:
        print("Debug mode enabled")
    sys.stdout.flush()
except Exception as e:
    print(f"Error initializing logging: {e}")
    sys.stdout.flush()
    sys.exit(1)

def update_ead_slot_duration_stats(epoch):
    """
    Updates epoch-level slot duration statistics in the epoch_aggregate_data table.

    Args:
        epoch (int): The epoch number to analyze.

    Returns:
        bool: True if successful, False otherwise.
    """
    try:
        json_file = f"epoch_stats_{epoch}.json"
        json_file_path = os.path.abspath(json_file)
        logger.info(f"Epoch {epoch}: Log file path: {LOG_FILE}")
        print(f"Epoch {epoch}: Log file path: {LOG_FILE}")
        logger.info(f"Epoch {epoch}: JSON file will be saved to: {json_file_path}")
        print(f"Epoch {epoch}: JSON file will be saved to: {json_file_path}")
        if DEBUG:
            logger.debug(f"Epoch {epoch}: Resolved JSON file path: {json_file_path}")
            print(f"Epoch {epoch}: Resolved JSON file path: {json_file_path}")
        sys.stdout.flush()
    except Exception as e:
        logger.error(f"Epoch {epoch}: Error printing file paths: {e}")
        print(f"Epoch {epoch}: Error printing file paths: {e}")
        sys.stdout.flush()
        return False

    try:
        # Verify file permissions
        logger.info(f"Epoch {epoch}: Verifying write permissions for {LOG_FILE}")
        print(f"Epoch {epoch}: Verifying write permissions for {LOG_FILE}")
        if DEBUG:
            logger.debug(f"Epoch {epoch}: Attempting to write test data to log file")
            print(f"Epoch {epoch}: Attempting to write test data to log file")
        sys.stdout.flush()
        with open(LOG_FILE, 'a') as f:
            f.write(f"Test write to log file for epoch {epoch}\n")
        logger.info(f"Epoch {epoch}: Write permissions confirmed for log file")
        print(f"Epoch {epoch}: Write permissions confirmed for log file")
        sys.stdout.flush()

        logger.info(f"Epoch {epoch}: Verifying write permissions for {json_file_path}")
        print(f"Epoch {epoch}: Verifying write permissions for {json_file_path}")
        if DEBUG:
            logger.debug(f"Epoch {epoch}: Attempting to write test data to JSON file")
            print(f"Epoch {epoch}: Attempting to write test data to JSON file")
        sys.stdout.flush()
        with open(json_file, 'a') as f:
            f.write("")
        logger.info(f"Epoch {epoch}: Write permissions confirmed for JSON file")
        print(f"Epoch {epoch}: Write permissions confirmed for JSON file")
        sys.stdout.flush()
    except Exception as e:
        logger.error(f"Epoch {epoch}: File permission error: {e}")
        print(f"Epoch {epoch}: File permission error: {e}")
        sys.stdout.flush()
        return False

    try:
        logger.info(f"Epoch {epoch}: Attempting database connection with params: {db_params}")
        print(f"Epoch {epoch}: Attempting database connection with params: {db_params}")
        if DEBUG:
            logger.debug(f"Epoch {epoch}: Database parameters: host={db_params.get('host')}, port={db_params.get('port')}, database={db_params.get('database')}, user={db_params.get('user')}, sslmode={db_params.get('sslmode')}")
            print(f"Epoch {epoch}: Database parameters: host={db_params.get('host')}, port={db_params.get('port')}, database={db_params.get('database')}, user={db_params.get('user')}, sslmode={db_params.get('sslmode')}")
        sys.stdout.flush()
        with psycopg2.connect(**db_params) as conn:
            with conn.cursor() as cur:
                logger.info(f"Epoch {epoch}: Connected to database")
                print(f"Epoch {epoch}: Connected to database")
                sys.stdout.flush()

                # Clear stale slot_duration_ fields
                try:
                    logger.info(f"Epoch {epoch}: Clearing stale slot_duration_ fields in epoch_aggregate_data")
                    print(f"Epoch {epoch}: Clearing stale slot_duration_ fields in epoch_aggregate_data")
                    sys.stdout.flush()
                    cur.execute("""
                        UPDATE epoch_aggregate_data
                        SET 
                            avg_slot_duration_ms = NULL,
                            slot_duration_population_stddev_ms = NULL,
                            slot_duration_total_validators_analyzed = NULL,
                            slot_duration_validators_lagging = NULL,
                            slot_duration_percent_lagging = NULL,
                            slot_duration_min_mean_slot_duration_ms = NULL,
                            slot_duration_max_mean_slot_duration_ms = NULL,
                            slot_duration_median_slot_duration_ms = NULL,
                            slot_duration_avg_stddev_ms = NULL,
                            slot_duration_avg_confidence_interval_width_ms = NULL
                        WHERE epoch = %s;
                    """, (epoch,))
                    logger.info(f"Epoch {epoch}: Cleared stale slot_duration_ fields")
                    print(f"Epoch {epoch}: Cleared stale slot_duration_ fields")
                    if DEBUG:
                        logger.debug(f"Epoch {epoch}: Executed clear of stale slot_duration_ fields for epoch {epoch}")
                        print(f"Epoch {epoch}: Executed clear of stale slot_duration_ fields for epoch {epoch}")
                    sys.stdout.flush()
                except Exception as e:
                    logger.error(f"Epoch {epoch}: Error clearing stale fields: {e}")
                    print(f"Epoch {epoch}: Error clearing stale fields: {e}")
                    sys.stdout.flush()
                    return False

                # Verify schema of epoch_aggregate_data
                try:
                    logger.info(f"Epoch {epoch}: Checking schema of epoch_aggregate_data")
                    print(f"Epoch {epoch}: Checking schema of epoch_aggregate_data")
                    sys.stdout.flush()
                    cur.execute("""
                        SELECT column_name
                        FROM information_schema.columns
                        WHERE table_name = 'epoch_aggregate_data'
                        AND column_name LIKE 'slot_duration_%' OR column_name = 'avg_slot_duration_ms';
                    """)
                    columns = [row[0] for row in cur.fetchall()]
                    required_columns = [
                        'avg_slot_duration_ms',
                        'slot_duration_population_stddev_ms',
                        'slot_duration_total_validators_analyzed',
                        'slot_duration_validators_lagging',
                        'slot_duration_percent_lagging',
                        'slot_duration_min_mean_slot_duration_ms',
                        'slot_duration_max_mean_slot_duration_ms',
                        'slot_duration_median_slot_duration_ms',
                        'slot_duration_avg_stddev_ms',
                        'slot_duration_avg_confidence_interval_width_ms'
                    ]
                    missing_columns = [col for col in required_columns if col not in columns]
                    if missing_columns:
                        logger.error(f"Epoch {epoch}: Missing columns in epoch_aggregate_data: {missing_columns}")
                        print(f"Epoch {epoch}: Missing columns in epoch_aggregate_data: {missing_columns}")
                        sys.stdout.flush()
                        return False
                    logger.info(f"Epoch {epoch}: All required columns present in epoch_aggregate_data")
                    print(f"Epoch {epoch}: All required columns present in epoch_aggregate_data")
                    if DEBUG:
                        logger.debug(f"Epoch {epoch}: Found columns: {columns}")
                        print(f"Epoch {epoch}: Found columns: {columns}")
                    sys.stdout.flush()
                except Exception as e:
                    logger.error(f"Epoch {epoch}: Schema validation error: {e}")
                    print(f"Epoch {epoch}: Schema validation error: {e}")
                    sys.stdout.flush()
                    return False

                # Validate data in validator_stats_slot_duration
                try:
                    logger.info(f"Epoch {epoch}: Validating data in validator_stats_slot_duration")
                    print(f"Epoch {epoch}: Validating data in validator_stats_slot_duration")
                    sys.stdout.flush()
                    cur.execute("""
                        SELECT 
                            COUNT(*) AS row_count,
                            COUNT(*) FILTER (WHERE slot_duration_mean IS NULL OR slot_duration_mean <= 0) AS invalid_mean,
                            COUNT(*) FILTER (WHERE slot_duration_stddev IS NULL OR slot_duration_stddev <= 0) AS invalid_stddev,
                            COUNT(*) FILTER (WHERE slot_duration_p_value IS NULL) AS null_p_value,
                            COUNT(*) FILTER (WHERE slot_duration_confidence_interval_lower_ms IS NULL) AS null_ci_lower,
                            COUNT(*) FILTER (WHERE slot_duration_confidence_interval_upper_ms IS NULL) AS null_ci_upper,
                            COUNT(*) FILTER (WHERE slot_duration_confidence_interval_lower_ms = slot_duration_confidence_interval_upper_ms) AS zero_ci_width
                        FROM validator_stats_slot_duration
                        WHERE epoch = %s;
                    """, (epoch,))
                    validation = cur.fetchone()
                    row_count, invalid_mean, invalid_stddev, null_p_value, null_ci_lower, null_ci_upper, zero_ci_width = validation
                    logger.info(f"Epoch {epoch}: {row_count} rows, invalid counts - mean: {invalid_mean}, stddev: {invalid_stddev}, p_value: {null_p_value}, ci_lower: {null_ci_lower}, ci_upper: {null_ci_upper}, zero_ci_width: {zero_ci_width}")
                    print(f"Epoch {epoch}: {row_count} rows, invalid counts - mean: {invalid_mean}, stddev: {invalid_stddev}, p_value: {null_p_value}, ci_lower: {null_ci_lower}, ci_upper: {null_ci_upper}, zero_ci_width: {zero_ci_width}")
                    if DEBUG:
                        logger.debug(f"Epoch {epoch}: Validation query result: {validation}")
                        print(f"Epoch {epoch}: Validation query result: {validation}")
                    sys.stdout.flush()
                except Exception as e:
                    logger.error(f"Epoch {epoch}: Validation query error: {e}")
                    print(f"Epoch {epoch}: Validation query error: {e}")
                    sys.stdout.flush()
                    return False

                # Log validators with zero confidence interval width
                if zero_ci_width > 0:
                    try:
                        logger.info(f"Epoch {epoch}: Checking validators with zero CI width")
                        print(f"Epoch {epoch}: Checking validators with zero CI width")
                        sys.stdout.flush()
                        cur.execute("""
                            SELECT identity_pubkey, slot_duration_confidence_interval_lower_ms, slot_duration_confidence_interval_upper_ms
                            FROM validator_stats_slot_duration
                            WHERE epoch = %s AND slot_duration_confidence_interval_lower_ms = slot_duration_confidence_interval_upper_ms;
                        """, (epoch,))
                        zero_ci_validators = cur.fetchall()
                        logger.warning(f"Epoch {epoch}: {zero_ci_width} validators with zero CI width: {[(row[0], row[1], row[2]) for row in zero_ci_validators]}")
                        print(f"Epoch {epoch}: {zero_ci_width} validators with zero CI width: {[(row[0], row[1], row[2]) for row in zero_ci_validators]}")
                        if DEBUG:
                            logger.debug(f"Epoch {epoch}: Zero CI validators: {zero_ci_validators}")
                            print(f"Epoch {epoch}: Zero CI validators: {zero_ci_validators}")
                        sys.stdout.flush()
                    except Exception as e:
                        logger.error(f"Epoch {epoch}: Zero CI width query error: {e}")
                        print(f"Epoch {epoch}: Zero CI width query error: {e}")
                        sys.stdout.flush()
                        return False

                # Get the number of validators for Bonferroni correction
                num_validators = row_count
                p_value_threshold = 0.05 / num_validators if num_validators else 0.05
                logger.info(f"Epoch {epoch}: p-value threshold: {p_value_threshold:.7f}")
                print(f"Epoch {epoch}: p-value threshold: {p_value_threshold:.7f}")
                if DEBUG:
                    logger.debug(f"Epoch {epoch}: Calculated num_validators: {num_validators}, p_value_threshold: {p_value_threshold}")
                    print(f"Epoch {epoch}: Calculated num_validators: {num_validators}, p_value_threshold: {p_value_threshold}")
                sys.stdout.flush()

                if row_count == 0:
                    logger.warning(f"Epoch {epoch}: No validators found")
                    print(f"Epoch {epoch}: No validators found")
                    sys.stdout.flush()
                    epoch_stats = {
                        "epoch": epoch,
                        "avg_slot_duration_ms": None,
                        "slot_duration_population_stddev_ms": None,
                        "slot_duration_total_validators_analyzed": 0,
                        "slot_duration_validators_lagging": None,
                        "slot_duration_percent_lagging": None,
                        "slot_duration_min_mean_slot_duration_ms": None,
                        "slot_duration_max_mean_slot_duration_ms": None,
                        "slot_duration_median_slot_duration_ms": None,
                        "slot_duration_avg_stddev_ms": None,
                        "slot_duration_avg_confidence_interval_width_ms": None
                    }
                else:
                    # Calculate epoch-level statistics
                    try:
                        logger.info(f"Epoch {epoch}: Running aggregation query")
                        print(f"Epoch {epoch}: Running aggregation query")
                        sys.stdout.flush()
                        cur.execute("""
                            SELECT 
                                AVG(slot_duration_mean) / 1000000.0 AS avg_slot_duration_ms,
                                STDDEV(slot_duration_mean) / 1000000.0 AS slot_duration_population_stddev_ms,
                                COUNT(*) AS slot_duration_total_validators_analyzed,
                                COUNT(*) FILTER (WHERE slot_duration_p_value < %s AND slot_duration_p_value IS NOT NULL) AS slot_duration_validators_lagging,
                                ROUND((COUNT(*) FILTER (WHERE slot_duration_p_value < %s AND slot_duration_p_value IS NOT NULL) / NULLIF(COUNT(*), 0)::numeric) * 100, 2) AS slot_duration_percent_lagging,
                                MIN(slot_duration_mean) / 1000000.0 AS slot_duration_min_mean_slot_duration_ms,
                                MAX(slot_duration_mean) / 1000000.0 AS slot_duration_max_mean_slot_duration_ms,
                                PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY slot_duration_median) / 1000000.0 AS slot_duration_median_slot_duration_ms,
                                AVG(slot_duration_stddev) / 1000000.0 AS slot_duration_avg_stddev_ms,
                                AVG(NULLIF(slot_duration_confidence_interval_upper_ms - slot_duration_confidence_interval_lower_ms, 0)) AS slot_duration_avg_confidence_interval_width_ms
                            FROM validator_stats_slot_duration
                            WHERE epoch = %s;
                        """, (p_value_threshold, p_value_threshold, epoch))
                        result = cur.fetchone()
                        logger.info(f"Epoch {epoch}: Aggregation query result: {result}")
                        print(f"Epoch {epoch}: Aggregation query result: {result}")
                        if DEBUG:
                            logger.debug(f"Epoch {epoch}: Full aggregation result: {result}")
                            print(f"Epoch {epoch}: Full aggregation result: {result}")
                        sys.stdout.flush()
                        if not result or result[0] is None:
                            logger.warning(f"Epoch {epoch}: No valid data for aggregation")
                            print(f"Epoch {epoch}: No valid data for aggregation")
                            sys.stdout.flush()
                            epoch_stats = {
                                "epoch": epoch,
                                "avg_slot_duration_ms": None,
                                "slot_duration_population_stddev_ms": None,
                                "slot_duration_total_validators_analyzed": 0,
                                "slot_duration_validators_lagging": None,
                                "slot_duration_percent_lagging": None,
                                "slot_duration_min_mean_slot_duration_ms": None,
                                "slot_duration_max_mean_slot_duration_ms": None,
                                "slot_duration_median_slot_duration_ms": None,
                                "slot_duration_avg_stddev_ms": None,
                                "slot_duration_avg_confidence_interval_width_ms": None
                            }
                        else:
                            epoch_stats = {
                                "epoch": epoch,
                                "avg_slot_duration_ms": float(round(float(result[0]), 2)) if result[0] is not None else None,
                                "slot_duration_population_stddev_ms": float(round(float(result[1]), 2)) if result[1] is not None else None,
                                "slot_duration_total_validators_analyzed": int(result[2]) if result[2] is not None else 0,
                                "slot_duration_validators_lagging": int(result[3]) if result[3] is not None else None,
                                "slot_duration_percent_lagging": float(result[4]) if result[4] is not None else None,
                                "slot_duration_min_mean_slot_duration_ms": float(round(float(result[5]), 2)) if result[5] is not None else None,
                                "slot_duration_max_mean_slot_duration_ms": float(round(float(result[6]), 2)) if result[6] is not None else None,
                                "slot_duration_median_slot_duration_ms": float(round(float(result[7]), 2)) if result[7] is not None else None,
                                "slot_duration_avg_stddev_ms": float(round(float(result[8]), 2)) if result[8] is not None else None,
                                "slot_duration_avg_confidence_interval_width_ms": float(round(float(result[9]), 2)) if result[9] is not None else None
                            }
                            logger.info(f"Epoch {epoch}: Stats before insert: {epoch_stats}")
                            print(f"Epoch {epoch}: Stats before insert: {epoch_stats}")
                            if DEBUG:
                                logger.debug(f"Epoch {epoch}: Prepared epoch_stats: {epoch_stats}")
                                print(f"Epoch {epoch}: Prepared epoch_stats: {epoch_stats}")
                            sys.stdout.flush()
                    except Exception as e:
                        logger.error(f"Epoch {epoch}: Aggregation query error: {e}")
                        print(f"Epoch {epoch}: Aggregation query error: {e}")
                        sys.stdout.flush()
                        return False

                # Update epoch_aggregate_data
                try:
                    logger.info(f"Epoch {epoch}: Updating epoch_aggregate_data")
                    print(f"Epoch {epoch}: Updating epoch_aggregate_data")
                    sys.stdout.flush()
                    cur.execute("""
                        UPDATE epoch_aggregate_data
                        SET 
                            avg_slot_duration_ms = %s,
                            slot_duration_population_stddev_ms = %s,
                            slot_duration_total_validators_analyzed = %s,
                            slot_duration_validators_lagging = %s,
                            slot_duration_percent_lagging = %s,
                            slot_duration_min_mean_slot_duration_ms = %s,
                            slot_duration_max_mean_slot_duration_ms = %s,
                            slot_duration_median_slot_duration_ms = %s,
                            slot_duration_avg_stddev_ms = %s,
                            slot_duration_avg_confidence_interval_width_ms = %s
                        WHERE epoch = %s
                        RETURNING *;
                    """, (
                        epoch_stats["avg_slot_duration_ms"],
                        epoch_stats["slot_duration_population_stddev_ms"],
                        epoch_stats["slot_duration_total_validators_analyzed"],
                        epoch_stats["slot_duration_validators_lagging"],
                        epoch_stats["slot_duration_percent_lagging"],
                        epoch_stats["slot_duration_min_mean_slot_duration_ms"],
                        epoch_stats["slot_duration_max_mean_slot_duration_ms"],
                        epoch_stats["slot_duration_median_slot_duration_ms"],
                        epoch_stats["slot_duration_avg_stddev_ms"],
                        epoch_stats["slot_duration_avg_confidence_interval_width_ms"],
                        epoch
                    ))
                    inserted_row = cur.fetchone()
                    if inserted_row:
                        logger.info(f"Epoch {epoch}: Updated row: {inserted_row}")
                        print(f"Epoch {epoch}: Updated row: {inserted_row}")
                        if DEBUG:
                            logger.debug(f"Epoch {epoch}: Updated row details: {inserted_row}")
                            print(f"Epoch {epoch}: Updated row details: {inserted_row}")
                    else:
                        logger.warning(f"Epoch {epoch}: No row updated, attempting insert")
                        print(f"Epoch {epoch}: No row updated, attempting insert")
                        cur.execute("""
                            INSERT INTO epoch_aggregate_data (
                                epoch,
                                avg_slot_duration_ms,
                                slot_duration_population_stddev_ms,
                                slot_duration_total_validators_analyzed,
                                slot_duration_validators_lagging,
                                slot_duration_percent_lagging,
                                slot_duration_min_mean_slot_duration_ms,
                                slot_duration_max_mean_slot_duration_ms,
                                slot_duration_median_slot_duration_ms,
                                slot_duration_avg_stddev_ms,
                                slot_duration_avg_confidence_interval_width_ms
                            )
                            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                            RETURNING *;
                        """, (
                            epoch,
                            epoch_stats["avg_slot_duration_ms"],
                            epoch_stats["slot_duration_population_stddev_ms"],
                            epoch_stats["slot_duration_total_validators_analyzed"],
                            epoch_stats["slot_duration_validators_lagging"],
                            epoch_stats["slot_duration_percent_lagging"],
                            epoch_stats["slot_duration_min_mean_slot_duration_ms"],
                            epoch_stats["slot_duration_max_mean_slot_duration_ms"],
                            epoch_stats["slot_duration_median_slot_duration_ms"],
                            epoch_stats["slot_duration_avg_stddev_ms"],
                            epoch_stats["slot_duration_avg_confidence_interval_width_ms"]
                        ))
                        inserted_row = cur.fetchone()
                        logger.info(f"Epoch {epoch}: Inserted row: {inserted_row}")
                        print(f"Epoch {epoch}: Inserted row: {inserted_row}")
                        if DEBUG:
                            logger.debug(f"Epoch {epoch}: Inserted row details: {inserted_row}")
                            print(f"Epoch {epoch}: Inserted row details: {inserted_row}")
                    sys.stdout.flush()
                    conn.commit()
                except Exception as e:
                    logger.error(f"Epoch {epoch}: Database update error: {e}")
                    print(f"Epoch {epoch}: Database update error: {e}")
                    sys.stdout.flush()
                    return False

                # Save to JSON for reference
                try:
                    logger.info(f"Epoch {epoch}: Saving stats to JSON file")
                    print(f"Epoch {epoch}: Saving stats to JSON file")
                    sys.stdout.flush()
                    with open(json_file, "w") as f:
                        json.dump(epoch_stats, f, indent=2)
                    logger.info(f"Epoch {epoch}: Epoch stats saved to: {json_file_path}")
                    print(f"Epoch {epoch}: Epoch stats saved to: {json_file_path}")
                    print(json.dumps(epoch_stats, indent=2))
                    if DEBUG:
                        logger.debug(f"Epoch {epoch}: JSON file contents: {json.dumps(epoch_stats, indent=2)}")
                        print(f"Epoch {epoch}: JSON file contents: {json.dumps(epoch_stats, indent=2)}")
                    sys.stdout.flush()
                    return True
                except Exception as e:
                    logger.error(f"Epoch {epoch}: JSON file write error: {e}")
                    print(f"Epoch {epoch}: JSON file write error: {e}")
                    sys.stdout.flush()
                    return False
    except psycopg2.Error as e:
        logger.error(f"Epoch {epoch}: Database connection error: {e}")
        print(f"Epoch {epoch}: Database connection error: {e}")
        sys.stdout.flush()
        return False
    except Exception as e:
        logger.error(f"Epoch {epoch}: Unexpected error: {e}")
        print(f"Epoch {epoch}: Unexpected error: {e}")
        sys.stdout.flush()
        return False

if __name__ == "__main__":
    import sys
    if len(sys.argv) != 2:
        logger.error("Usage: python3 92_update_ead_slot_duration_stats.py <epoch>")
        print("Usage: python3 92_update_ead_slot_duration_stats.py <epoch>")
        sys.stdout.flush()
        sys.exit(1)
    try:
        epoch = int(sys.argv[1])
        logger.info(f"Main: Starting update for epoch {epoch}")
        print(f"Main: Starting update for epoch {epoch}")
        sys.stdout.flush()
        success = update_ead_slot_duration_stats(epoch)
        logger.info(f"Main: Script completed with success={success}")
        print(f"Main: Script completed with success={success}")
        sys.stdout.flush()
        sys.exit(0 if success else 1)
    except ValueError as e:
        logger.error(f"Main: Invalid epoch argument: {e}")
        print(f"Main: Invalid epoch argument: {e}")
        sys.stdout.flush()
        sys.exit(1)
