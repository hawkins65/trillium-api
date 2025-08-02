import sys
import psycopg2
import json
import logging
from db_config import db_params
from decimal import Decimal

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler("vx-call-export.log"),
        logging.StreamHandler()
    ]
)

def fetch_data(epoch, vote_accounts=None):
    """Fetch data from the votes_table for the specified epoch and optional list of vote_account_pubkey."""
    query = """
        SELECT
            epoch,
            vote_account_pubkey,
            vote_credits,
            voted_slots,
            ROUND(avg_credit_per_voted_slot, 3) AS avg_credit_per_voted_slot,
            max_vote_latency,
            ROUND(mean_vote_latency, 3) AS mean_vote_latency,
            ROUND(median_vote_latency, 3) AS median_vote_latency
        FROM votes_table
        WHERE epoch = %s
    """
    params = [epoch]

    if vote_accounts:
        placeholders = ', '.join(['%s'] * len(vote_accounts))
        query += f" AND vote_account_pubkey IN ({placeholders})"
        params.extend(vote_accounts)

    try:
        conn = psycopg2.connect(**db_params)
        cur = conn.cursor()
        cur.execute(query, params)
        data = cur.fetchall()
        colnames = [desc[0] for desc in cur.description]
        cur.close()
        conn.close()

        # Convert result into a list of dictionaries
        return [dict(zip(colnames, row)) for row in data]
    except Exception as e:
        logging.error(f"Failed to fetch data from the database: {e}")
        raise

def write_json(data, filename):
    """Write the fetched data to a JSON file."""
    def decimal_to_float(obj):
        if isinstance(obj, Decimal):
            return float(obj)
        raise TypeError("Type not serializable")

    try:
        with open(filename, "w") as json_file:
            json.dump(data, json_file, indent=4, default=decimal_to_float)
        logging.info(f"Data successfully written to {filename}")
    except Exception as e:
        logging.error(f"Failed to write JSON to file: {e}")
        raise

if __name__ == "__main__":
    if len(sys.argv) < 2:
        logging.error("Usage: python3 vx-call-export.py <epoch> [vote_account_pubkey1,vote_account_pubkey2,...]")
        sys.exit(1)

    # Parse arguments
    epoch = int(sys.argv[1])
    vote_accounts = sys.argv[2].split(',') if len(sys.argv) > 2 else None

    # Fetch data
    try:
        data = fetch_data(epoch, vote_accounts)
        if not data:
            logging.info("No data found for the specified parameters.")
            sys.exit(0)

        # Construct filename
        filename = f"export_epoch_{epoch}"
        if vote_accounts:
            shortened_accounts = "_".join([account[:3] for account in vote_accounts])
            filename += f"_{shortened_accounts}"
        filename += ".json"

        # Write data to JSON
        write_json(data, filename)
    except Exception as e:
        logging.error(f"An error occurred: {e}")
        sys.exit(1)
