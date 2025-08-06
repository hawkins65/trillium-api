import os
# -*- coding: utf-8 -*-
"""solana_block_laggards_db_update.py

Modified version to read from PostgreSQL database and analyze validator laggards
Fixed to convert nanoseconds to milliseconds for display
"""

import pandas as pd
import statsmodels.formula.api as smf
import numpy as np
import psycopg2
from psycopg2.extras import execute_values
import importlib.util

# Setup unified logging
script_dir = os.path.dirname(os.path.abspath(__file__))
logging_config_path = os.path.join(script_dir, "999_logging_config.py")
spec = importlib.util.spec_from_file_location("logging_config", logging_config_path)
logging_config = importlib.util.module_from_spec(spec)
spec.loader.exec_module(logging_config)
logger = logging_config.setup_logging(os.path.basename(__file__).replace('.py', ''))
import argparse
import re

# Import your database configuration
from db_config import db_params

# Client type mapping
CLIENT_TYPE_MAP = {
    0: 'Solana Labs',
    1: 'Jito Labs',
    2: 'Firedancer',
    3: 'Agave',
    4: 'Paladin',
    None: 'Unknown'
}

# Conversion factor: nanoseconds to milliseconds
NS_TO_MS = 1_000_000

# Set up logging
# Logging config moved to unified configuration
# Logger setup moved to unified configuration

def get_db_connection_string(db_params):
    return f"postgresql://{db_params['user']}@{db_params['host']}:{db_params['port']}/{db_params['database']}?sslmode={db_params['sslmode']}"

def get_db_connection(db_params):
    conn = psycopg2.connect(**db_params)
    logger.debug(f"Created new DB connection: {conn}")
    return conn

def get_epoch_range():
    """Get available epoch range from database"""
    try:
        with get_db_connection(db_params) as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT MIN(epoch), MAX(epoch)
                    FROM validator_stats_slot_duration;
                """)
                result = cur.fetchone()
                return result if result else (None, None)
    except Exception as e:
        logger.error(f"Error retrieving epoch range: {e}")
        return None, None

def fetch_validator_data_from_db(epoch):
    """Fetch validator data from database for specified epoch"""
    logger.info(f"Fetching validator data from database for epoch {epoch}...")
    
    try:
        with get_db_connection(db_params) as conn:
            with conn.cursor() as cur:
                # Query to get validator data by joining the three tables
                cur.execute("""
                    SELECT 
                        vss.identity_pubkey,
                        vs.vote_account_pubkey,
                        vss.epoch,
                        vs.version,
                        vi.name,
                        vs.continent,
                        vs.stake_percentage,
                        vs.client_type,
                        vss.slot_duration_mean,
                        vss.slot_duration_stddev,
                        vss.slot_duration_min,
                        vss.slot_duration_max,
                        vss.slot_duration_median,
                        vs.leader_slots
                    FROM validator_stats_slot_duration vss
                    LEFT JOIN validator_stats vs ON vss.identity_pubkey = vs.identity_pubkey AND vss.epoch = vs.epoch
                    LEFT JOIN validator_info vi ON vss.identity_pubkey = vi.identity_pubkey
                    WHERE vss.epoch = %s
                        AND vss.slot_duration_mean IS NOT NULL
                    ORDER BY vss.identity_pubkey;
                """, (epoch,))
                
                results = cur.fetchall()
                
                if not results:
                    logger.error(f"No data found for epoch {epoch}")
                    return []
                
                # Convert to list of dictionaries (similar to API format)
                data = []
                for row in results:
                    data.append({
                        'identity_pubkey': row[0],
                        'vote_account_pubkey': row[1] if row[1] else row[0],  # Fallback to identity if vote_account is null
                        'epoch': row[2],
                        'version': row[3] if row[3] else '',
                        'name': row[4] if row[4] else None,
                        'continent': row[5] if row[5] else '',
                        'stake_percentage': float(row[6]) if row[6] is not None else 0.0,
                        'client_type': row[7],
                        'slot_duration_mean': float(row[8]) if row[8] is not None else None,
                        'slot_duration_stddev': float(row[9]) if row[9] is not None else None,
                        'slot_duration_min': row[10],
                        'slot_duration_max': row[11],
                        'slot_duration_median': row[12],
                        'leader_slots': int(row[13]) if row[13] is not None else None
                    })
                
                logger.info(f"Successfully fetched data for {len(data)} validators")
                return data
                
    except Exception as e:
        logger.error(f"Error fetching data from database: {e}")
        raise

def get_epoch_from_data(data):
    """Extract epoch from the database data"""
    if data and len(data) > 0:
        epoch = data[0].get('epoch')
        if epoch is not None:
            return int(epoch)
    raise ValueError("Could not determine epoch from database data")

def process_validator_data(data):
    """Process validator data from database into DataFrame"""
    logger.info("Processing validator data...")
    
    extracted_data = []
    for entry in data:
        # Get client type from mapping
        client_type_int = entry.get("client_type")
        logger.info(f"client_type_int {client_type_int}")
        client = CLIENT_TYPE_MAP.get(client_type_int, 'Unknown')
        logger.info(f"client {client}")
        
        # Convert nanoseconds to milliseconds for timing data
        slot_duration_mean_ns = entry.get("slot_duration_mean")
        slot_duration_stddev_ns = entry.get("slot_duration_stddev")
        slot_duration_min_ns = entry.get("slot_duration_min")
        slot_duration_max_ns = entry.get("slot_duration_max")
        slot_duration_median_ns = entry.get("slot_duration_median")
        
        row = {
            "validator": str(entry.get("identity_pubkey", "")),
            "vote_account_pubkey": str(entry.get("vote_account_pubkey", "")),
            "epoch": int(entry.get("epoch", 0)) if entry.get("epoch") is not None else None,
            "version": str(entry.get("version", "")).strip(),
            "name": entry.get("name") if entry.get("name") else None,
            "client": client,
            "continent": str(entry.get("continent", "")).strip(),
            "stake_percentage": float(entry.get("stake_percentage", 0)) if entry.get("stake_percentage") is not None else 0.0,
            # Convert nanoseconds to milliseconds
            "block_time_mean": float(slot_duration_mean_ns / NS_TO_MS) if slot_duration_mean_ns is not None else None,
            "block_time_stdev": float(slot_duration_stddev_ns / NS_TO_MS) if slot_duration_stddev_ns is not None else None,
            "leader_slots": int(entry.get("leader_slots", 0)) if entry.get("leader_slots") is not None else None,
            # Keep original nanosecond values for database operations
            "slot_duration_min_ns": slot_duration_min_ns,
            "slot_duration_max_ns": slot_duration_max_ns,
            "slot_duration_median_ns": slot_duration_median_ns,
            # Also convert these for display purposes
            "slot_duration_min": float(slot_duration_min_ns / NS_TO_MS) if slot_duration_min_ns is not None else None,
            "slot_duration_max": float(slot_duration_max_ns / NS_TO_MS) if slot_duration_max_ns is not None else None,
            "slot_duration_median": float(slot_duration_median_ns / NS_TO_MS) if slot_duration_median_ns is not None else None
        }
        extracted_data.append(row)

    df = pd.DataFrame(extracted_data)
    df = df[df.block_time_mean.notnull()]
    
    logger.info(f"Processed {df.shape[0]} validators with valid block timing data")
    logger.info(f"Converted timing data from nanoseconds to milliseconds (division by {NS_TO_MS:,})")
    return df

def calculate_validator_statistics(df):
    """Calculate statistical analysis for each validator"""
    logger.info("Calculating validator statistics...")
    
    validators = df['validator'].unique()
    results = []
    
    # Set p_value threshold with Bonferroni correction
    p_value_threshold = 0.05 / len(validators)
    
    for i, validator in enumerate(validators):
        if i % 100 == 0:
            logger.info(f"Processing validator {i+1}/{len(validators)}")
            
        df['validator_indicator'] = (df['validator'] == validator).astype(int)
        
        try:
            model = smf.wls('block_time_mean ~ validator_indicator + C(continent) + C(client)',
                           data=df,
                           weights=1 / (df['block_time_stdev'] ** 2)
                           ).fit()
            
            validator_p_value = model.pvalues['validator_indicator']
            validator_coef = model.params['validator_indicator']

            # Calculate confidence intervals
            conf_int_90 = model.conf_int(alpha=0.20).loc['validator_indicator']
            conf_int_95 = model.conf_int(alpha=0.05).loc['validator_indicator']

            lower_ci_90, upper_ci_90 = conf_int_90[0], conf_int_90[1]
            lower_ci_95, upper_ci_95 = conf_int_95[0], conf_int_95[1]

            # Determine if validator is lagging (using both p-value and confidence interval)
            is_lagging_pvalue = validator_p_value < p_value_threshold
            is_lagging_ci = lower_ci_95 > 0
            is_lagging = is_lagging_pvalue and is_lagging_ci

            results.append({
                'validator': validator,
                'p_value': validator_p_value,
                'coef': validator_coef,
                'lower_ci_90': lower_ci_90,
                'upper_ci_90': upper_ci_90,
                'lower_ci_95': lower_ci_95,
                'upper_ci_95': upper_ci_95,
                'is_lagging': is_lagging
            })
            
        except Exception as e:
            logger.warning(f"Error processing validator {validator}: {e}")
            # Add placeholder values for failed calculations
            results.append({
                'validator': validator,
                'p_value': None,
                'coef': None,
                'lower_ci_90': None,
                'upper_ci_90': None,
                'lower_ci_95': None,
                'upper_ci_95': None,
                'is_lagging': False
            })

    results_df = pd.DataFrame(results)
    logger.info(f"Completed statistical analysis for {len(results)} validators")
    
    return results_df

def display_laggards(df, results_df):
    """Display detailed information about lagging validators"""
    # Merge dataframes to get all information
    merged_df = df.merge(results_df, left_on='validator', right_on='validator', how='left')
    
    # Filter for lagging validators
    laggards = merged_df[merged_df['is_lagging'] == True].copy()
    
    if len(laggards) == 0:
        print("No lagging validators found!")
        return
    
    # Sort by coefficient (how much slower they are) - worst first
    laggards = laggards.sort_values('coef', ascending=False)
    
    # Calculate total stake percentage for laggards
    total_laggard_stake = laggards['stake_percentage'].sum()
    
    # Create display columns
    display_data = []
    for _, row in laggards.iterrows():
        # Format name - use vote_account_pubkey if name is missing
        name = row.get('name')
        if pd.isna(name) or name == '' or name == 'None' or name is None or str(name).strip() == '':
            vote_pubkey = str(row.get('vote_account_pubkey', ''))
            if len(vote_pubkey) >= 12:
                name_display = f"{vote_pubkey[:6]}...{vote_pubkey[-6:]}"
            else:
                name_display = vote_pubkey[:15]
        else:
            name_display = str(name)[:18]  # Slightly shorter to fit stake column
        
        # Format client and version
        client = str(row.get('client', ''))
        version = str(row.get('version', ''))
        client_version = f"{client} {version}"[:13]  # Shorter to fit stake column
        
        # Format other columns
        continent = str(row.get('continent', ''))[:10]  # Truncate continent
        stake_pct = f"{row.get('stake_percentage', 0):.3f}%" if pd.notna(row.get('stake_percentage')) else "0.000%"
        mean_ms = f"{row['block_time_mean']:.1f}" if pd.notna(row['block_time_mean']) else "N/A"
        coef_ms = f"{row['coef']:.1f}" if pd.notna(row['coef']) else "N/A"
        ci_95_lower = f"{row['lower_ci_95']:.1f}" if pd.notna(row['lower_ci_95']) else "N/A"
        ci_95_upper = f"{row['upper_ci_95']:.1f}" if pd.notna(row['upper_ci_95']) else "N/A"
        
        # Simplify p-value display since they're so small
        p_val = row['p_value']
        if pd.notna(p_val) and p_val != "N/A":
            if p_val < 1e-10:
                p_value_fmt = "<1e-10"
            else:
                p_value_fmt = f"{p_val:.1e}"
        else:
            p_value_fmt = "N/A"
        
        display_data.append([name_display, client_version, continent, stake_pct, mean_ms, coef_ms, ci_95_lower, ci_95_upper, p_value_fmt])
    
    # Create DataFrame for display
    display_cols = pd.DataFrame(display_data, columns=['Name', 'Client/Ver', 'Continent', 'Stake%', 'Mean(ms)', 'Coef(ms)', 'CI95_Low', 'CI95_High', 'P-Value'])
    
    # Clean output without logger prefixes
    print(f"\n{'='*105}")
    print(f"LAGGING VALIDATORS FOUND: {len(laggards)} (Sorted by Coefficient - Worst First)")
    print(f"TOTAL STAKE PERCENTAGE OF LAGGARDS: {total_laggard_stake:.3f}%")
    print(f"{'='*105}")
    print("\nExplanation:")
    print("- Stake%: Validator's percentage of total network stake")
    print("- Mean(ms): Average slot duration in milliseconds")
    print("- Coef(ms): Regression coefficient (positive = slower than average)")
    print("- CI95_Low/High: 95% confidence interval bounds")
    print("- P-Value: Statistical significance (<1e-10 means extremely significant)")
    print("- NOTE: All timing values converted from nanoseconds to milliseconds for display")
    print(f"\n{display_cols.to_string(index=False)}")
    print(f"{'='*105}")

def prepare_database_records(df, results_df):
    """Prepare records for database insertion"""
    logger.info("Preparing database records...")
    
    # Merge the dataframes
    merged_df = df.merge(results_df, left_on='validator', right_on='validator', how='left')
    
    records = []
    for _, row in merged_df.iterrows():
        # Convert millisecond confidence intervals back to nanoseconds for database storage
        lower_ci_95_ns = float(row['lower_ci_95'] * NS_TO_MS) if pd.notna(row['lower_ci_95']) else None
        upper_ci_95_ns = float(row['upper_ci_95'] * NS_TO_MS) if pd.notna(row['upper_ci_95']) else None
        lower_ci_90_ns = float(row['lower_ci_90'] * NS_TO_MS) if pd.notna(row['lower_ci_90']) else None
        upper_ci_90_ns = float(row['upper_ci_90'] * NS_TO_MS) if pd.notna(row['upper_ci_90']) else None
        coef_ns = float(row['coef'] * NS_TO_MS) if pd.notna(row['coef']) else None
        
        record = (
            row['validator'],  # identity_pubkey
            int(row['epoch']) if pd.notna(row['epoch']) else None,  # epoch
            int(float(row['slot_duration_min_ns'])) if pd.notna(row['slot_duration_min_ns']) else None,  # slot_duration_min (nanoseconds)
            int(float(row['slot_duration_max_ns'])) if pd.notna(row['slot_duration_max_ns']) else None,  # slot_duration_max (nanoseconds)
            int(float(row['block_time_mean'] * NS_TO_MS)) if pd.notna(row['block_time_mean']) else None,  # slot_duration_mean (convert back to nanoseconds)
            int(float(row['slot_duration_median_ns'])) if pd.notna(row['slot_duration_median_ns']) else None,  # slot_duration_median (nanoseconds)
            float(row['block_time_stdev'] * NS_TO_MS) if pd.notna(row['block_time_stdev']) else None,  # slot_duration_stddev (convert back to nanoseconds)
            float(row['p_value']) if pd.notna(row['p_value']) else None,  # slot_duration_p_value
            lower_ci_95_ns,  # slot_duration_confidence_interval_lower_ms (actually nanoseconds)
            upper_ci_95_ns,  # slot_duration_confidence_interval_upper_ms (actually nanoseconds)
            bool(row['is_lagging']) if pd.notna(row['is_lagging']) else False,  # slot_duration_is_lagging
            coef_ns,  # slot_duration_coef (convert back to nanoseconds)
            lower_ci_90_ns,  # slot_duration_ci_lower_90_ms (actually nanoseconds)
            upper_ci_90_ns,  # slot_duration_ci_upper_90_ms (actually nanoseconds)
            lower_ci_95_ns,  # slot_duration_ci_lower_95_ms (actually nanoseconds)
            upper_ci_95_ns,  # slot_duration_ci_upper_95_ms (actually nanoseconds)
        )
        records.append(record)
    
    logger.info(f"Prepared {len(records)} records for database insertion")
    logger.info("Note: Statistical values converted back to nanoseconds for database storage")
    return records

def update_database(records, epoch):
    """Update the database with calculated statistics"""
    logger.info(f"Updating database for epoch {epoch}...")
    
    conn = get_db_connection(db_params)
    try:
        with conn.cursor() as cursor:
            # Use proper UPSERT - no DELETE, just INSERT with ON CONFLICT DO UPDATE
            insert_query = """
                INSERT INTO validator_stats_slot_duration (
                    identity_pubkey, epoch, slot_duration_min, slot_duration_max,
                    slot_duration_mean, slot_duration_median, slot_duration_stddev,
                    slot_duration_p_value, slot_duration_confidence_interval_lower_ms,
                    slot_duration_confidence_interval_upper_ms, slot_duration_is_lagging,
                    slot_duration_coef, slot_duration_ci_lower_90_ms,
                    slot_duration_ci_upper_90_ms, slot_duration_ci_lower_95_ms,
                    slot_duration_ci_upper_95_ms
                ) VALUES %s
                ON CONFLICT (identity_pubkey, epoch) 
                DO UPDATE SET
                    slot_duration_p_value = EXCLUDED.slot_duration_p_value,
                    slot_duration_confidence_interval_lower_ms = EXCLUDED.slot_duration_confidence_interval_lower_ms,
                    slot_duration_confidence_interval_upper_ms = EXCLUDED.slot_duration_confidence_interval_upper_ms,
                    slot_duration_is_lagging = EXCLUDED.slot_duration_is_lagging,
                    slot_duration_coef = EXCLUDED.slot_duration_coef,
                    slot_duration_ci_lower_90_ms = EXCLUDED.slot_duration_ci_lower_90_ms,
                    slot_duration_ci_upper_90_ms = EXCLUDED.slot_duration_ci_upper_90_ms,
                    slot_duration_ci_lower_95_ms = EXCLUDED.slot_duration_ci_lower_95_ms,
                    slot_duration_ci_upper_95_ms = EXCLUDED.slot_duration_ci_upper_95_ms,
                    slot_duration_min = COALESCE(validator_stats_slot_duration.slot_duration_min, EXCLUDED.slot_duration_min),
                    slot_duration_max = COALESCE(validator_stats_slot_duration.slot_duration_max, EXCLUDED.slot_duration_max),
                    slot_duration_mean = COALESCE(validator_stats_slot_duration.slot_duration_mean, EXCLUDED.slot_duration_mean),
                    slot_duration_median = COALESCE(validator_stats_slot_duration.slot_duration_median, EXCLUDED.slot_duration_median),
                    slot_duration_stddev = COALESCE(validator_stats_slot_duration.slot_duration_stddev, EXCLUDED.slot_duration_stddev)
            """
            
            execute_values(cursor, insert_query, records, template=None, page_size=1000)
            
            conn.commit()
            logger.info(f"Successfully upserted {len(records)} records (no deletions)")
            
    except Exception as e:
        conn.rollback()
        logger.error(f"Error updating database: {e}")
        raise
    finally:
        conn.close()

def main():
    """Main execution function"""
    parser = argparse.ArgumentParser(description="Analyze validator slot duration laggards for a given epoch.")
    parser.add_argument("epoch", type=int, help="Epoch number to analyze", nargs='?', default=None)
    args = parser.parse_args()
    
    try:
        if args.epoch is None:
            min_epoch, max_epoch = get_epoch_range()
            if min_epoch is None or max_epoch is None:
                logger.error("No epoch data found in the validator_stats_slot_duration table.")
                return
            
            logger.info(f"Available epoch range: {min_epoch} to {max_epoch}")
            logger.info(f"Defaulting to the latest epoch: {max_epoch}")
            epoch = max_epoch
        else:
            epoch = args.epoch
        
        # Fetch data from database instead of API
        raw_data = fetch_validator_data_from_db(epoch)
        if not raw_data:
            logger.error(f"No data available for epoch {epoch}")
            return
            
        df = process_validator_data(raw_data)
        
        logger.info(f"Processing data for epoch {epoch}")
        
        # Show basic stats
        stats = df.groupby(["client", "continent"]).agg({
            "validator": "count",
            "block_time_mean": "mean",
        })
        logger.info(f"Basic statistics by client and continent (in milliseconds):\n{stats}")
        
        # Calculate statistics
        results_df = calculate_validator_statistics(df)
        
        # Display lagging validators in detail
        display_laggards(df, results_df)
        
        # Prepare database records and update (only the statistical analysis fields)
        records = prepare_database_records(df, results_df)
        update_database(records, epoch)
        
        # Print summary
        laggards_count = sum(1 for record in records if record[10])
        logger.info(f"\nAnalysis complete! Found {laggards_count} lagging validators out of {len(records)} total validators")
        
    except Exception as e:
        logger.error(f"Error in main execution: {e}")
        raise

if __name__ == "__main__":
    main()