import json
import psycopg2
import csv
from db_config import db_params

# Create a connection to the PostgreSQL database
conn = psycopg2.connect(**db_params)
cursor = conn.cursor()

# Function to fetch validator information from the database
def fetch_validator_info(identity_pubkey):
    query = """
    SELECT vi.name, vs.city, vs.country, vs.asn, vs.asn_org, vs.activated_stake, 
           vs.stake_percentage
    FROM validator_info vi
    JOIN validator_stats vs ON vi.identity_pubkey = vs.identity_pubkey
    WHERE vi.identity_pubkey = %s;
    """
    cursor.execute(query, (identity_pubkey,))
    return cursor.fetchone()

# Read the gossip.json file
with open('gossip.json', 'r') as file:
    gossip_data = json.load(file)

# Prepare CSV file for output
with open('validator_info.csv', 'w', newline='') as csvfile:
    csvwriter = csv.writer(csvfile)
    csvwriter.writerow(['IPAddress', 'Name', 'City', 'Country', 'ASN', 'ASN Org', 'Activated Stake', 'Version', 'Stake Percentage'])

    # Process each entry in the gossip data
    for entry in gossip_data:
        identity_pubkey = entry.get('identityPubkey')
        ip_address = entry.get('ipAddress')
        version = entry.get('version', 'NULL')  # Get version from JSON, default to 'NULL' if not present
        validator_info = fetch_validator_info(identity_pubkey) if identity_pubkey else None

        # If no data found, fill with 'NULL' for the database fields
        if validator_info is None:
            validator_info = ['NULL'] * 7  # Ensures list has 7 elements for the database fields

        # Write to CSV
        csvwriter.writerow([ip_address] + list(validator_info) + [version])
        # Display on the screen
        print(f"IPAddress: {ip_address}, Name: {validator_info[0]}, City: {validator_info[1]}, Country: {validator_info[2]}, "
              f"ASN: {validator_info[3]}, ASN Org: {validator_info[4]}, Activated Stake: {validator_info[5]}, "
              f"Version: {version}, Stake Percentage: {validator_info[6]}")

# Close the cursor and connection
cursor.close()
conn.close()
