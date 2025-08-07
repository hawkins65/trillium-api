import json
import psycopg2
from db_config import db_params

# Paths to JSON files
GOOD_JSON = '/home/smilax/trillium_api/data/temp/good.json'
POOR_JSON = '/home/smilax/trillium_api/data/temp/poor.json'

def create_table_if_not_exists(cursor):
    """Create the voter_classifications table if it doesn't exist."""
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS voter_classifications (
            vote_pubkey VARCHAR(44) PRIMARY KEY,
            identity_pubkey VARCHAR(44) NOT NULL,
            stake BIGINT,
            average_vl NUMERIC,
            average_llv NUMERIC,
            average_cv NUMERIC,
            vo BOOLEAN,
            is_foundation_staked BOOLEAN,
            classification TEXT NOT NULL,
            epoch INTEGER NOT NULL
        );
        CREATE INDEX IF NOT EXISTS idx_voter_classifications_vote_pubkey 
        ON voter_classifications (vote_pubkey);
    """)

def import_voters_from_json(cursor, json_file, classification):
    """Import voters from a JSON file into the voter_classifications table."""
    with open(json_file, 'r') as f:
        data = json.load(f)
    
    # Get the first epoch from data_epochs
    epoch = data['data_epochs'][0]
    
    # Extract voters
    voters = data['voters']
    
    # Prepare the insert query
    insert_query = """
        INSERT INTO voter_classifications (
            vote_pubkey, identity_pubkey, stake, average_vl, average_llv, 
            average_cv, vo, is_foundation_staked, classification, epoch
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (vote_pubkey) DO UPDATE SET
            identity_pubkey = EXCLUDED.identity_pubkey,
            stake = EXCLUDED.stake,
            average_vl = EXCLUDED.average_vl,
            average_llv = EXCLUDED.average_llv,
            average_cv = EXCLUDED.average_cv,
            vo = EXCLUDED.vo,
            is_foundation_staked = EXCLUDED.is_foundation_staked,
            classification = EXCLUDED.classification,
            epoch = EXCLUDED.epoch;
    """
    
    # Insert each voter
    for voter in voters:
        cursor.execute(insert_query, (
            voter['vote_pubkey'],
            voter['identity_pubkey'],
            voter['stake'],
            voter['average_vl'],
            voter['average_llv'],
            voter['average_cv'],
            voter['vo'],
            voter['is_foundation_staked'],
            classification,
            epoch
        ))

def main():
    # Connect to the database
    conn = psycopg2.connect(**db_params)
    cur = conn.cursor()
    
    try:
        # Create the table if it doesn't exist
        create_table_if_not_exists(cur)
        
        # Import data from good.json
        import_voters_from_json(cur, GOOD_JSON, 'good')
        
        # Import data from poor.json
        import_voters_from_json(cur, POOR_JSON, 'poor')
        
        # Commit the transaction
        conn.commit()
        print("Data imported successfully.")
    
    except Exception as e:
        print(f"Error: {e}")
        conn.rollback()
    
    finally:
        cur.close()
        conn.close()

if __name__ == "__main__":
    main()