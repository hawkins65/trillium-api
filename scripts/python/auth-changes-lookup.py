import csv
import json
import psycopg2
from db_config import db_params

def read_auth_changes(file_path):
    """
    Reads the auth_changes CSV and structures the data.
    """
    data = {}

    with open(file_path, 'r') as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            vote_account_address = row['vote_account_address']
            data[vote_account_address] = {
                'epochs_voters_before': row['epochs_voters_before'],
                'epochs_voters_after': row['epochs_voters_after'],
                'epochs_voters_current': row['epochs_voters_current'],
                'matches': []  # Placeholder for matches from the database
            }
    
    return data

def fetch_validator_stats(epochs, vote_account_address, voters):
    """
    Fetches matching validator stats for the given epochs and voters.
    Adjusts epochs:
    - Replaces epoch 709 with 708
    - Replaces epochs < 600 with 600
    """
    adjusted_epochs = [
        708 if epoch == 709 else (600 if epoch < 600 else epoch)
        for epoch in epochs
    ]

    query = """
    SELECT epoch, vote_account_pubkey, identity_pubkey 
    FROM validator_stats 
    WHERE (vote_account_pubkey = %s OR identity_pubkey = ANY(%s))
    AND epoch = ANY(%s)
    ORDER BY epoch;
    """

    conn = psycopg2.connect(**db_params)
    cur = conn.cursor()

    cur.execute(query, (vote_account_address, voters, adjusted_epochs))
    results = cur.fetchall()

    cur.close()
    conn.close()

    return results

def parse_epochs_and_voters(column_data):
    """
    Parses the epochs and voters from a CSV column like '273:4cheZ7Qm; 706:PUmpKiNnS'.
    Returns a tuple of epochs and voters.
    """
    epochs = []
    voters = []

    pairs = column_data.split(';')
    for pair in pairs:
        if ':' in pair:
            epoch, voter = pair.split(':')
            epochs.append(int(epoch))
            voters.append(voter.strip())

    return epochs, voters

def process_data(data):
    """
    Processes the data to fetch matching records from the database.
    """
    for vote_account_address, details in data.items():
        # Parse the epochs and voters for before, after, and current
        before_epochs, before_voters = parse_epochs_and_voters(details['epochs_voters_before'])
        after_epochs, after_voters = parse_epochs_and_voters(details['epochs_voters_after'])
        current_epochs, current_voters = parse_epochs_and_voters(details['epochs_voters_current'])

        # Combine all epochs and voters for the database query
        all_epochs = before_epochs + after_epochs + current_epochs
        all_voters = list(set(before_voters + after_voters + current_voters))  # Unique voters

        # Fetch matches from the database
        matches = fetch_validator_stats(all_epochs, vote_account_address, all_voters)

        # Add matches to the details
        details['matches'] = [
            {
                'epoch': match[0],
                'vote_account_pubkey': match[1],
                'identity_pubkey': match[2]
            }
            for match in matches
        ]

    return data

def main():
    # Path to the auth_changes.csv file
    input_file = 'auth_changes.csv'
    output_file = 'auth_changes_lookup.json'

    # Read and structure the CSV data
    data = read_auth_changes(input_file)

    # Process the data to find matches
    processed_data = process_data(data)

    # Write the results to a JSON file
    with open(output_file, 'w') as jsonfile:
        json.dump(processed_data, jsonfile, indent=4)

    print(f"Output written to {output_file}")

if __name__ == "__main__":
    main()
