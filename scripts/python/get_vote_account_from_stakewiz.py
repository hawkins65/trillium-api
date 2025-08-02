import psycopg2
import requests
import csv
import logging

# PostgreSQL database connection parameters
from db_config import db_params

# Configure logging to log both to console and a log file
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s',
                    handlers=[
                        logging.FileHandler("validator_update.log"),
                        logging.StreamHandler()
                    ])

# Create a connection to the PostgreSQL database
conn = psycopg2.connect(**db_params)
conn.autocommit = True  # Enable autocommit mode
cursor = conn.cursor()

# Function to get validators data from the API
def get_validators_data():
    response = requests.get('https://api.stakewiz.com/validators')
    if response.status_code == 200:
        return response.json()
    else:
        logging.error(f"Failed to fetch validators data: {response.status_code}")
        raise Exception(f"Failed to fetch validators data: {response.status_code}")

# Load the missing identity_pubkey values from the CSV file
def load_missing_identity_pubkeys(csv_file):
    missing_pubkeys = []
    with open(csv_file, mode='r') as file:
        csv_reader = csv.reader(file)
        for row in csv_reader:
            missing_pubkeys.append(row[0])
    return missing_pubkeys

# Update the vote_account_pubkey in the database
def update_vote_account_pubkey(identity_pubkey, vote_account_pubkey):
    try:
        cursor.execute(
            """
            UPDATE validator_stats 
            SET vote_account_pubkey = %s 
            WHERE identity_pubkey = %s
            """, (vote_account_pubkey, identity_pubkey)
        )
        logging.info(f"Updated vote_account_pubkey for identity_pubkey {identity_pubkey}")
    except Exception as e:
        logging.error(f"Error updating vote_account_pubkey for {identity_pubkey}: {e}")

def main():
    # Load the missing identity_pubkeys from the CSV
    missing_identity_pubkeys = load_missing_identity_pubkeys('missing_vote_account_pubkey.csv')
    logging.info(f"Loaded {len(missing_identity_pubkeys)} missing identity_pubkeys from CSV")

    # Fetch the validator data from the API
    validators_data = get_validators_data()
    logging.info(f"Fetched {len(validators_data)} records from the validators API")

    # Loop through missing pubkeys and update their vote_account_pubkey
    for identity_pubkey in missing_identity_pubkeys:
        for validator in validators_data:
            if validator['identity'] == identity_pubkey:
                vote_account_pubkey = validator['vote_identity']
                update_vote_account_pubkey(identity_pubkey, vote_account_pubkey)
                break
        else:
            logging.warning(f"No match found for identity_pubkey {identity_pubkey}")

if __name__ == '__main__':
    main()

# Close the cursor and connection
cursor.close()
conn.close()
