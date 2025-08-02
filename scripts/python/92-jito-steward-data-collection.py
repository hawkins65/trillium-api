import subprocess
import sys
import os
import psycopg2
import logging
from datetime import datetime
from db_config import db_params  # Using your provided db_config.py
from rpc_config import RPC_ENDPOINT  # Import the centralized RPC endpoint

# Use the imported RPC endpoint
RPC_URL = RPC_ENDPOINT

# Constants
STEWARD_CONFIG = 'jitoVjT9jRUyeXHzvCwzPgHj7yWNRhLcUoXtes4wtjv'

# Set up logging
# Get the basename of the current script
script_name = os.path.basename(sys.argv[0]).replace('.py', '')
# Set log directory in home folder
log_dir = os.path.expanduser('~/log')
# Construct the full log file path
log_file = os.path.join(log_dir, f"{script_name}.log")

# Create logger
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Create formatter
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')

# File handler
file_handler = logging.FileHandler(log_file)
file_handler.setLevel(logging.INFO)
file_handler.setFormatter(formatter)

# Console handler
console_handler = logging.StreamHandler()
console_handler.setLevel(logging.INFO)
console_handler.setFormatter(formatter)

# Add handlers to logger
logger.addHandler(file_handler)
logger.addHandler(console_handler)

def generate_steward_file(epoch):
    """Generate the steward state file for given epoch using the CLI command"""
    filename = f'jito-steward-state-all-validators-epoch-{epoch}.txt'
    # command resolves to this:
    # /home/smilax/stakenet/target/release/steward-cli --json-rpc-url https://side-silent-county.solana-mainnet.quiknode.pro/2ffa9d32adcd0102e7b78a8ba107f5c49b9420d8/ view-state --steward-config jitoVjT9jRUyeXHzvCwzPgHj7yWNRhLcUoXtes4wtjv --verbose
    # see this file for details on output:  /home/smilax/stakenet/docs/developers/cli.md
    # TO-DO -- add a table for stake pool delegation.  Retrieve "Active Lamports: 3398839 (0.00 ◎)" per validator to get Jito Delegation amount
    command = [
        '/home/smilax/stakenet/target/release/steward-cli',
        '--json-rpc-url', RPC_URL,
        'view-state',
        '--steward-config', STEWARD_CONFIG,
        '--verbose'
    ]
    
    try:
        with open(filename, 'w') as f:
            subprocess.run(command, stdout=f, check=True)
        logger.info(f"Generated steward file: {filename}")
        return filename
    except subprocess.CalledProcessError as e:
        logger.error(f"Error generating steward file: {e}")
        sys.exit(1)

def parse_steward_file(filename):
    """Parse the steward file and extract required fields for each vote account"""
    validator_data = {}
    current_validator = {}
    
    with open(filename, 'r') as f:
        lines = f.readlines()
        
        for line in lines:
            line = line.strip()
            if line.startswith("Vote Account:"):
                if current_validator:  # Save previous validator data
                    validator_data[current_validator['vote_account']] = current_validator
                current_validator = {'vote_account': line.split(": ")[1]}
            elif line.startswith("Steward List Index:"):
                current_validator['steward_list_index'] = int(line.split(": ")[1])
            elif line.startswith("Overall Rank:"):
                rank_value = line.split(": ")[1]
                if rank_value == "N/A":
                    current_validator['overall_rank'] = 111111
                else:
                    try:
                        current_validator['overall_rank'] = int(rank_value)
                    except ValueError:
                        current_validator['overall_rank'] = 999999
                        logger.warning(f"Invalid Overall Rank value '{rank_value}' for vote account {current_validator['vote_account']}")
            elif line.startswith("Validator History Index:"):
                current_validator['validator_history_index'] = int(line.split(": ")[1])
        
        # Save the last validator
        if current_validator:
            validator_data[current_validator['vote_account']] = current_validator
    
    logger.info(f"Parsed data for {len(validator_data)} validators")
    return validator_data

def update_database(epoch, validator_data):
    """Update validator_stats table with parsed data"""
    try:
        conn = psycopg2.connect(**db_params)
        cur = conn.cursor()
        
        # Get existing validators for this epoch
        cur.execute(
            """
            SELECT DISTINCT vote_account_pubkey 
            FROM validator_stats 
            WHERE epoch = %s
            ORDER BY vote_account_pubkey
            """, 
            (epoch,)
        )
        existing_validators = set(row[0] for row in cur.fetchall())
        
        # Update records where vote_account matches
        updated_count = 0
        for vote_account, data in validator_data.items():
            if vote_account in existing_validators:
                cur.execute(
                    """
                    UPDATE validator_stats 
                    SET 
                        jito_steward_list_index = %s,
                        jito_steward_overall_rank = %s,
                        jito_steward_validator_history_index = %s
                    WHERE vote_account_pubkey = %s AND epoch = %s
                    """,
                    (
                        data['steward_list_index'],
                        data['overall_rank'],  # Will be None if N/A
                        data['validator_history_index'],
                        vote_account,
                        epoch
                    )
                )
                updated_count += 1
        
        conn.commit()
        logger.info(f"Updated {updated_count} validator records for epoch {epoch}")
        
    except psycopg2.Error as e:
        logger.error(f"Database error: {e}")
        conn.rollback()
        sys.exit(1)
    finally:
        cur.close()
        conn.close()

def main():
    # Get epoch from command line or prompt
    if len(sys.argv) > 1:
        try:
            epoch = int(sys.argv[1])
            logger.info(f"Starting process for epoch {epoch}")
        except ValueError:
            logger.error("Invalid epoch number provided")
            print("Invalid epoch number provided")
            sys.exit(1)
    else:
        try:
            epoch = int(input("Please enter the epoch number: "))
            logger.info(f"Starting process for epoch {epoch} (entered interactively)")
        except ValueError:
            logger.error("Invalid epoch number entered")
            print("Invalid epoch number entered")
            sys.exit(1)
    
    # Generate the file
    filename = generate_steward_file(epoch)
    print(f"Generated file: {filename}")
    
    # Parse the file
    validator_data = parse_steward_file(filename)
    print(f"Parsed data for {len(validator_data)} validators")
    
    # Update the database
    update_database(epoch, validator_data)

if __name__ == "__main__":
    main()