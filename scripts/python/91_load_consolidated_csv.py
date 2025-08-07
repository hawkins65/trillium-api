import os
import sys
import psycopg2
import glob
import csv
import time
import re
import json
import importlib.util

# Setup unified logging
script_dir = os.path.dirname(os.path.abspath(__file__))
logging_config_path = os.path.join(script_dir, "999_logging_config.py")
spec = importlib.util.spec_from_file_location("logging_config", logging_config_path)
logging_config = importlib.util.module_from_spec(spec)
spec.loader.exec_module(logging_config)
logger = logging_config.setup_logging(os.path.basename(__file__).replace('.py', ''))

epoch_number = input("Enter the epoch number: ")
logger.info(f"🚀 Processing epoch number: {epoch_number}")

#initialize the timers
start_time_1 = time.time()
start_time_2 = time.time()
start_time_3 = time.time()
start_time_4 = time.time()
start_time_5 = time.time()
start_time_6 = time.time()

# Start the timer for setup
start_time_1 = time.time()

# PostgreSQL database connection parameters
from db_config import db_params

# Construct the base directory dynamically
# Use the TRILLIUM_DATA_EPOCHS environment variable or fall back to the standard path
base_directory = os.path.join(
    os.environ.get('TRILLIUM_DATA_EPOCHS', '/home/smilax/trillium_api/data/epochs'),
    f"epoch{epoch_number}"
)

# Create a connection to the PostgreSQL database
conn = psycopg2.connect(**db_params)
conn.autocommit = True  # Enable autocommit mode
cursor = conn.cursor()

# Drop temp tables if they exist
cursor.execute("DROP TABLE IF EXISTS temp_epoch_votes;")
cursor.execute("DROP TABLE IF EXISTS temp_validator_data;")
cursor.execute("DROP TABLE IF EXISTS temp_validator_stats;")

# Create temp_epoch_votes table with columns for all CSV elements
cursor.execute("""
    CREATE TABLE temp_epoch_votes (
        epoch SMALLINT,
        block_slot INTEGER,
        block_hash TEXT,
        identity_pubkey CHAR(44),
        vote_account_pubkey CHAR(44)
    );
""")

# Create temp_validator_data table with the same structure as the original table, excluding indexes
cursor.execute("CREATE TABLE temp_validator_data (LIKE validator_data);")
conn.commit()
logger.info("✅ Temp tables created successfully")

# Search for directories matching the pattern "run*"
run_directories = sorted(glob.glob(os.path.join(base_directory, "run*")), key=lambda x: [int(y) if y.isdigit() else y.lower() for y in re.split(r'(\d+)', os.path.basename(x))])
total_run_directories = len(run_directories)
logger.info(f"📁 Found {total_run_directories} run directories")

# Initialize a global variable to track mismatches
mismatch_found = False

# Initialize a counter for total rows in CSV files
total_csv_rows = 0

# Iterate over the run directories
for index, run_directory in enumerate(run_directories, start=1):
    logger.info(f"🔄 Processing run directory {index} of {total_run_directories}: {run_directory}")

    # Find all epoch_votes CSV files in the run directory
    epoch_votes_files = glob.glob(os.path.join(run_directory, "epoch_votes_*.csv"))
    epoch_votes_count = len(epoch_votes_files)
    logger.info(f"📊 Found {epoch_votes_count} epoch_votes CSV files")

    # Find all slot_data CSV files in the run directory
    slot_data_files = glob.glob(os.path.join(run_directory, "slot_data_*.csv"))
    slot_data_count = len(slot_data_files)
    logger.info(f"📊 Found {slot_data_count} slot_data CSV files")

    # Sanity check
    count_difference = epoch_votes_count - slot_data_count
    if count_difference > 10:
        print(f"Warning: The difference between the number of epoch_votes and slot_data CSV files is: {count_difference}.")
        user_input = input("Do you want to continue? (y/n) [y]: ").strip().lower() or 'y'
        if user_input != 'y':
            print("Exiting the script.")
            exit()

    print("Process epoch_votes CSV files")
    for epoch_votes_file in epoch_votes_files:
        with open(epoch_votes_file, "r") as file:
            cursor.copy_expert("COPY temp_epoch_votes FROM STDIN WITH CSV HEADER", file)

    print("Process slot_data CSV files")
    for slot_data_file in slot_data_files:
        with open(slot_data_file, "r") as file:
            # Count the number of rows in the CSV file (subtract 1 for the header)
            csv_row_count = sum(1 for row in file) - 1
            total_csv_rows += csv_row_count
            print(f"Processing {slot_data_file} - {csv_row_count} rows")
            file.seek(0)  # Reset file pointer to the beginning
            cursor.copy_expert("COPY temp_validator_data FROM STDIN WITH CSV HEADER", file)

    # Get the size of the temp_epoch_votes table
    cursor.execute("SELECT pg_table_size('temp_epoch_votes')")
    temp_epoch_votes_size = cursor.fetchone()[0]
    #print(f"Size of temp_epoch_votes table: {temp_epoch_votes_size} bytes")

    # Get the size of the temp_validator_data table
    cursor.execute("SELECT pg_table_size('temp_validator_data')")
    temp_validator_data_size = cursor.fetchone()[0]
    #print(f"Size of temp_validator_data table: {temp_validator_data_size} bytes")

    total_size = temp_epoch_votes_size + temp_validator_data_size
    #print(f"Total size of temp tables: {total_size} bytes")

    # Check if the temporary tables are empty
    if total_size == 8192:
        print("Skipping to the next run directory due to empty temp tables.")
        continue

# jrh 2024-10-14 debugging to see why we are losing so many slots
# After loading all CSV files, check the count in temp_validator_data
cursor.execute("SELECT COUNT(*) FROM temp_validator_data")
temp_table_count = cursor.fetchone()[0]

print(f"Total rows in CSV files: {total_csv_rows}")
print(f"Rows in temp_validator_data: {temp_table_count}")

if total_csv_rows != temp_table_count:
    print(f"Warning: Mismatch in row counts. Difference: {total_csv_rows - temp_table_count}")
else:
    print("Row counts match between CSV files and temp_validator_data table.")

# Additional checks
cursor.execute("SELECT COUNT(DISTINCT block_slot) FROM temp_validator_data")
distinct_slots = cursor.fetchone()[0]
print(f"Distinct block_slots in temp_validator_data: {distinct_slots}")

# jrh 2024-10-14 -- I had some early automation runs where epoch was not set correctly.  
# luckily I had the default as 1964 to find this error
# let's fix it here for now

# Constants for epoch calculation
STARTING_BLOCK_SLOT = 259200000
SLOTS_PER_EPOCH = 432000
EPOCH_OFFSET = 600

# Function to calculate the correct epoch for a given block slot
def calculate_epoch(block_slot):
    return ((block_slot - STARTING_BLOCK_SLOT) // SLOTS_PER_EPOCH) + EPOCH_OFFSET

# Function to perform the update for a given table
def update_table_epoch(table_name):
    # Fetch block_slot and epoch values from the specified table for the given range
    query = f"""
        SELECT block_slot, epoch 
        FROM {table_name} 
    """
    cursor.execute(query)
    records = cursor.fetchall()

    # List to hold the records that need updating
    updates = []

    # Check if the stored epoch matches the calculated epoch
    for block_slot, stored_epoch in records:
        calculated_epoch = calculate_epoch(block_slot)
        if stored_epoch != calculated_epoch:
            updates.append((calculated_epoch, block_slot))

    # Update the table with the correct epoch where necessary
    if updates:
        update_query = f"""
            UPDATE {table_name} 
            SET epoch = %s 
            WHERE block_slot = %s
        """
        cursor.executemany(update_query, updates)

    # Commit the transaction to save the changes
    conn.commit()

# Call the function for both tables
# jrh 2024-10-19 the problem seems to be resolved 
# update_table_epoch('temp_validator_data')
# update_table_epoch('temp_epoch_votes')

# Check for potential duplicates
cursor.execute("""
    SELECT block_slot, COUNT(*) 
    FROM temp_validator_data 
    GROUP BY block_slot 
    HAVING COUNT(*) > 1 
    ORDER BY COUNT(*) DESC 
    LIMIT 5
""")

potential_duplicates = cursor.fetchall()
if potential_duplicates:
    print("Top 5 potential duplicate block_slots in temp_validator_data:")
    for slot, count in potential_duplicates:
        print(f"  Block slot {slot}: {count} occurrences")

usr_input = 'y'
#usr_input = input("Check the temp tables and let me know if you want to continue").strip().lower() or 'y'
if usr_input != 'y':
    print("Exiting the script.")
    exit()

# Start the timer for processing slot_data chain verification
start_time_2 = time.time()

# Get a list of block_slot and parent_slot from temp_validator_data
# to verify the chain links are all there and in order
cursor.execute("SELECT block_slot, parent_slot FROM temp_validator_data")
slots = cursor.fetchall()
# Sort slots in descending order by block_slot
slots.sort(key=lambda x: x[0], reverse=True)
# Convert the list to a dictionary for quick lookup
slot_dict = {block_slot: parent_slot for block_slot, parent_slot in slots}
# Check to see if we are missing any slots in the chain
error_found = False

# Set up logging
# Get the basename of the current script
script_name = os.path.basename(sys.argv[0]).replace('.py', '')
# Set log directory in home folder
log_dir = os.path.expanduser('~/log')
# Construct the full log file path
log_file = os.path.join(log_dir, f"{script_name}.log")

# jrh 2025-05-19 -- this was used for skip blame -- no longer needed
#output_file = f"epoch_{epoch_number}_ordered_slots.csv"
#print("Save the ordered list to a CSV file for later inspection")
#with open(output_file, 'w', newline='') as csvfile:
#    csv_writer = csv.writer(csvfile)
#    csv_writer.writerow(['block_slot', 'parent_slot'])
#    for block_slot, parent_slot in slots:
#        csv_writer.writerow([block_slot, parent_slot])

print("Walk the slot chain list to find any missing parent slots and duplicate slots")
with open(log_file, 'w') as log:
    min_block_slot = min(slot_dict.keys())
    missing_parent_slots = []
    duplicate_slots = {}

    # First pass: identify duplicate slots
    for block_slot, parent_slot in slots:
        if block_slot in duplicate_slots:
            duplicate_slots[block_slot].append(parent_slot)
        else:
            duplicate_slots[block_slot] = [parent_slot]

    # Second pass: check for missing parent slots and report duplicates
    for block_slot in slot_dict:
        parent_slot = slot_dict[block_slot]
        if parent_slot not in slot_dict:
            if block_slot == min_block_slot:
                log.write(f"Warning: Parent slot {parent_slot} not found for the lowest numbered block slot {block_slot}. This is expected and will be ignored.\n")
            else:
                log.write(f"Error: Parent slot {parent_slot} not found for block slot {block_slot}\n")
                missing_parent_slots.append(parent_slot)
        
        # Report duplicate slots
        if len(duplicate_slots[block_slot]) > 1:
            log.write(f"Warning: Duplicate entries found for block slot {block_slot}. Parent slots: {', '.join(map(str, duplicate_slots[block_slot]))}\n")

    if len(missing_parent_slots) > 20:
        error_found = True
        log.write(f"Total missing parent slots (excluding the lowest numbered block slot): {len(missing_parent_slots)}\n")
    else:
        log.write("No missing parent slots found (excluding the lowest numbered block slot).\n")

    duplicate_slot_count = sum(len(parents) > 1 for parents in duplicate_slots.values())
    if duplicate_slot_count > 0:
        log.write(f"Total number of slots with duplicates: {duplicate_slot_count}\n")
    else:
        log.write("No duplicate slots found.\n")

with open(log_file, 'r') as log:
    log_contents = log.read()
    if log_contents.strip():
        print(f"Contents of {log_file}:")
        print(log_contents)
        user_input = 'y'
        #user_input = input("Errors found in the slot data chain verification. Do you want to proceed anyway? (y/n) [n]: ").strip().lower() or 'n'
        if user_input == 'y':
            print("Proceeding with the data loading process...")
            error_found = False
        else:
            print("Aborting the data loading process.")
    else:
        print(f"No errors found in {log_file}. Proceeding with the data loading process...")

# Start the timer for slot data finalize
start_time_3 = time.time()

if not error_found:
    cursor.execute("SELECT COUNT(*) FROM validator_data WHERE epoch = %s", (epoch_number,))
    before_insert = cursor.fetchone()[0]
    print(f"Rows in validator_data for epoch {epoch_number} before insert: {before_insert}")

    print("Delete existing data for the epoch")
    cursor.execute(f"DELETE FROM validator_data WHERE epoch = {epoch_number}")

    print("Copy data from the temporary table to the validator_data table")
    cursor.execute("""
        INSERT INTO validator_data 
        SELECT * FROM temp_validator_data 
        WHERE epoch = %s
    """, (epoch_number,))

    cursor.execute("SELECT COUNT(*) FROM validator_data WHERE epoch = %s", (epoch_number,))
    after_insert = cursor.fetchone()[0]
    print(f"Rows in validator_data for epoch {epoch_number} after insert: {after_insert}")

    print(f"Difference: {after_insert - before_insert}")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_validator_data_epoch_identity_pubkey ON validator_data (epoch, identity_pubkey)")
    print(f"Data loaded successfully into validator_data")
else:
    print(f"Errors found during temp_validator_data parent_slot chain check. Data loading aborted. Check the log file '{log_file}' for details.")

# Start the timer for processing epoch votes
start_time_4 = time.time()

if not error_found:
    # Start the timer for processing slot data
    start_time_5 = time.time()

    # Create a temporary table to hold the aggregated vote counts
    cursor.execute("""
        CREATE TABLE temp_validator_stats (
            identity_pubkey CHAR(44),
            vote_account_pubkey CHAR(44),
            epoch INTEGER,
            votes_cast INTEGER
        );
    """)

    print("Insert aggregated vote counts into the temporary table")
    cursor.execute("""
        INSERT INTO temp_validator_stats (identity_pubkey, vote_account_pubkey, epoch, votes_cast)
        SELECT identity_pubkey, vote_account_pubkey, %s, COUNT(*) AS votes_cast
        FROM temp_epoch_votes
        WHERE epoch = %s
        GROUP BY identity_pubkey, vote_account_pubkey
    """, (epoch_number, epoch_number))

    print("Merge data from the temporary table into the validator_stats table, choosing the rows where votes_cast is large and ignoring any rows where votes_cast <=5")
    print(f"Starting merge for epoch {epoch_number}: Merging data from temp_validator_stats into validator_stats.")
    print("Choosing rows with votes_cast > 5 and prioritizing highest votes_cast.")

    try:
        # Command 1: Stage the final, authoritative data into a temporary table.
        # This data determines the "winning" identity for each vote_account (by highest votes_cast)
        # and then the "winning" vote_account for each identity (by highest votes_cast).
        # The result has unique (identity_pubkey, epoch) and unique (vote_account_pubkey, epoch).
        sql_command_1_create_temp_data = """
        CREATE TEMPORARY TABLE temp_final_validator_data AS
        WITH ranked_by_vote AS (
            SELECT
                identity_pubkey,
                vote_account_pubkey,
                epoch,
                SUM(votes_cast) AS votes_cast,
                ROW_NUMBER() OVER (PARTITION BY vote_account_pubkey, epoch ORDER BY SUM(votes_cast) DESC, identity_pubkey) AS vote_rn
            FROM temp_validator_stats
            WHERE epoch = %s
            GROUP BY identity_pubkey, vote_account_pubkey, epoch
            HAVING SUM(votes_cast) > 5
        ),
        filtered_by_vote AS (
            SELECT
                identity_pubkey,
                vote_account_pubkey,
                epoch,
                votes_cast, -- This is SUM(votes_cast) from ranked_by_vote
                ROW_NUMBER() OVER (PARTITION BY identity_pubkey, epoch ORDER BY votes_cast DESC, vote_account_pubkey) AS identity_rn
            FROM ranked_by_vote
            WHERE vote_rn = 1
        )
        SELECT
            identity_pubkey,
            vote_account_pubkey,
            epoch,
            votes_cast
        FROM filtered_by_vote
        WHERE identity_rn = 1;
        """
        cursor.execute(sql_command_1_create_temp_data, (epoch_number,))
        print(f"Epoch {epoch_number}: Step 1 - Staging authoritative data completed.")

        # Command 2: Delete rows from validator_stats that would conflict with validator_stats_unique_vote_epoch.
        # This removes existing records where the (vote_account_pubkey, epoch) is now "owned" by a different identity_pubkey
        # according to our authoritative temp_final_validator_data.
        sql_command_2_delete_conflicts = """
        DELETE FROM validator_stats vs
        WHERE vs.epoch = %s
          AND EXISTS (
            SELECT 1
            FROM temp_final_validator_data tfvd
            WHERE tfvd.epoch = vs.epoch  -- Ensuring comparison is within the same epoch
              AND tfvd.vote_account_pubkey = vs.vote_account_pubkey
              AND tfvd.identity_pubkey != vs.identity_pubkey -- Key: the vote_account is now claimed by a NEW identity
        );
        """
        cursor.execute(sql_command_2_delete_conflicts, (epoch_number,))
        print(f"Epoch {epoch_number}: Step 2 - Deletion of outdated conflicting vote_account associations completed. ({cursor.rowcount} rows deleted)")

        # Command 3: Perform the main INSERT ... ON CONFLICT operation.
        # The conflict target is validator_stats_unique_identity_epoch (identity_pubkey, epoch).
        # Updates will set votes_cast and potentially change vote_account_pubkey.
        # The previous DELETE ensures that changing vote_account_pubkey won't hit validator_stats_unique_vote_epoch.
        sql_command_3_upsert_data = """
        INSERT INTO validator_stats (identity_pubkey, vote_account_pubkey, epoch, votes_cast)
        SELECT
            identity_pubkey,
            vote_account_pubkey,
            epoch,
            votes_cast
        FROM temp_final_validator_data
        ON CONFLICT ON CONSTRAINT validator_stats_unique_identity_epoch
        DO UPDATE SET
            votes_cast = EXCLUDED.votes_cast,
            vote_account_pubkey = EXCLUDED.vote_account_pubkey;
        """
        cursor.execute(sql_command_3_upsert_data)
        print(f"Epoch {epoch_number}: Step 3 - Upsert into validator_stats completed. ({cursor.rowcount} rows affected/inserted)")

        # Commit the transaction
        conn.commit()
        print(f"Epoch {epoch_number}: Data loaded successfully into validator_stats and transaction committed.")

    except psycopg2.Error as e:
        print(f"Epoch {epoch_number}: A database error occurred: {e}")
        print(f"SQL Error Code: {e.pgcode}")
        print(f"Details: {e.pgerror}")
        if conn:
            conn.rollback()
        print(f"Epoch {epoch_number}: Transaction rolled back.")
    except Exception as e:
        print(f"Epoch {epoch_number}: A non-database error occurred: {e}")
        if conn:
            conn.rollback()
        print(f"Epoch {epoch_number}: Transaction rolled back due to non-database error.")
    finally:
        # Command 4: Clean up the temporary table.
        # This runs whether the try block succeeded or failed (after commit/rollback).
        if cursor:
            try:
                sql_command_4_drop_temp_table = "DROP TABLE IF EXISTS temp_final_validator_data;"
                cursor.execute(sql_command_4_drop_temp_table)
                conn.commit() # Commit the drop if DDL requires it outside main transaction or for visibility
                print(f"Epoch {epoch_number}: Step 4 - Temporary table temp_final_validator_data dropped.")
            except psycopg2.Error as drop_e:
                print(f"Epoch {epoch_number}: Error dropping temporary table: {drop_e}")
            # No need to rollback here if drop fails, main transaction is already handled.
else:
    logger.error(f"❌ Errors found during temp_validator_data parent_slot chain check. Epoch vote data loading skipped for epoch {epoch_number}.")

logger.info("🔄 Processing run0 JSON files for validator_xshin table")

run0_directory = os.path.join(base_directory, "run0")
if not os.path.exists(run0_directory):
    logger.warning(f"⚠️ run0 directory not found: {run0_directory}")
else:
    logger.info(f"📁 Processing run0 directory: {run0_directory}")

json_files = ["good.json", "poor.json"]
all_data = []
processed_files = []

for json_file in json_files:
    file_path = os.path.join(run0_directory, json_file)
    
    if not os.path.exists(file_path):
        logger.warning(f"⚠️ {json_file} not found in {run0_directory}")
        continue

    try:
        with open(file_path, 'r') as file:
            data = json.load(file)
        processed_files.append(json_file)
        logger.info(f"✅ Loaded {json_file} - {len(data.get('voters', []))} validators")
    except json.JSONDecodeError as e:
        logger.error(f"❌ Error decoding JSON in {file_path}: {e}")
        continue
    except Exception as e:
        logger.error(f"❌ Unexpected error loading {file_path}: {e}")
        continue

    # Get the epochs from data_epochs
    epochs = data.get('data_epochs', [])
    if len(epochs) < 2:
        logger.warning(f"⚠️ Skipping {json_file}: invalid or missing data_epochs (got: {epochs})")
        continue

    # Check if either epoch matches the target epoch_number
    epoch_number_int = int(epoch_number)
    if not any(epoch == epoch_number_int for epoch in epochs):
        logger.warning(f"⚠️ Skipping {json_file}: neither epoch {epochs[0]} nor {epochs[1]} matches target epoch {epoch_number_int}")
        continue

    logger.info(f"📊 Processing {json_file} with epochs {epochs} for target epoch {epoch_number_int}")

    # Process voter data if at least one epoch is within range
    category = json_file.replace('.json', '')  # 'good' or 'poor'
    voters_processed = 0
    none_values_count = 0

    for voter in data['voters']:
        # Handle None values by converting to NULL-compatible values
        average_vl = voter.get('average_vl')
        average_llv = voter.get('average_llv')
        average_cv = voter.get('average_cv')
        
        # Count None values for logging
        if average_vl is None or average_llv is None or average_cv is None:
            none_values_count += 1
        
        all_data.append((
            epoch_number_int,
            voter['vote_pubkey'],
            voter['identity_pubkey'],
            voter['stake'],
            average_vl,  # Can be None, will become NULL in PostgreSQL
            average_llv,
            average_cv,
            voter.get('vo', False),
            voter['is_foundation_staked']
        ))
        voters_processed += 1

    logger.info(f"📈 Processed {voters_processed} {category} validators ({none_values_count} with None performance values)")

# Insert all data at once with upsert if we have data
if all_data:
    try:
        logger.info(f"💾 Inserting {len(all_data)} validator records into validator_xshin table...")
        cursor.executemany("""
            INSERT INTO validator_xshin (
                epoch, vote_account_pubkey, identity_pubkey, stake, 
                average_vl, average_llv, average_cv, vo, is_foundation_staked
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (epoch, identity_pubkey) DO UPDATE SET
                vote_account_pubkey = EXCLUDED.vote_account_pubkey,
                stake = EXCLUDED.stake,
                average_vl = EXCLUDED.average_vl,
                average_llv = EXCLUDED.average_llv,
                average_cv = EXCLUDED.average_cv,
                vo = EXCLUDED.vo,
                is_foundation_staked = EXCLUDED.is_foundation_staked
        """, all_data)
        conn.commit()
        
        # Provide summary statistics
        good_count = sum(1 for d in all_data if 'good' in str(processed_files))
        poor_count = len(all_data) - good_count
        logger.info(f"✅ Successfully processed run0 JSON files:")
        logger.info(f"   📊 Files processed: {', '.join(processed_files)}")
        logger.info(f"   📈 Total validators: {len(all_data)}")
        logger.info(f"   💾 Upserted into validator_xshin table for epoch {epoch_number}")
        
    except psycopg2.Error as e:
        logger.error(f"❌ Error inserting run0 data into database: {e}")
        conn.rollback()
        raise
    except Exception as e:
        logger.error(f"❌ Unexpected error during run0 database operations: {e}")
        conn.rollback()
        raise
else:
    if os.path.exists(run0_directory):
        logger.warning(f"⚠️ No run0 data processed for epoch {epoch_number}")
    else:
        logger.info(f"ℹ️ No run0 directory found - skipping validator performance data")

# Close the database connection
cursor.close()
conn.close()

# Stop the timer and calculate the elapsed time
end_time = time.time()

# Calculate elapsed times
elapsed_time_1 = start_time_2 - start_time_1
elapsed_time_2 = start_time_3 - start_time_2
elapsed_time_3 = start_time_4 - start_time_3
elapsed_time_4 = start_time_5 - start_time_4
elapsed_time_5 = end_time - start_time_5
total_elapsed_time = end_time - start_time_1

# Convert elapsed times to minutes and seconds
def format_time(seconds):
    minutes = int(seconds // 60)
    seconds = int(seconds % 60)
    return f"{minutes}:{seconds:02d}"

print(f"Time for section 1 setup:                           {elapsed_time_1:.2f} seconds ({format_time(elapsed_time_1)} minutes:seconds)")
print(f"Time for section 2 slot_data chain verification:    {elapsed_time_2:.2f} seconds ({format_time(elapsed_time_2)} minutes:seconds)")
print(f"Time for section 3 slot data finalize:              {elapsed_time_3:.2f} seconds ({format_time(elapsed_time_3)} minutes:seconds)")
print(f"Time for section 4 processing epoch votes:          {elapsed_time_4:.2f} seconds ({format_time(elapsed_time_4)} minutes:seconds)")
print(f"Time for section 5 slot data finalize:              {elapsed_time_5:.2f} seconds ({format_time(elapsed_time_5)} minutes:seconds)")
print(f"Total processing time:                              {total_elapsed_time:.2f} seconds ({format_time(total_elapsed_time)} minutes:seconds)")