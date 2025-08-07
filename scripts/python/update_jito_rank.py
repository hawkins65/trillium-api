import psycopg2
import subprocess
from db_config import db_params

def fetch_all_vote_account_epochs():
    # Connect to the database and retrieve all distinct vote_account_pubkey, epoch pairs
    conn = psycopg2.connect(**db_params)
    cur = conn.cursor()
    cur.execute("SELECT DISTINCT vote_account_pubkey, epoch FROM validator_stats WHERE vote_account_pubkey IS NOT NULL ORDER BY vote_account_pubkey, epoch;")
    results = cur.fetchall()
    cur.close()
    conn.close()
    return results

def update_jito_rank(vote_account_pubkey, epoch, jito_rank):
    # Update the jito_rank for the given vote_account_pubkey and epoch
    conn = psycopg2.connect(**db_params)
    cur = conn.cursor()
    cur.execute("""
        UPDATE validator_stats
        SET jito_rank = %s
        WHERE vote_account_pubkey = %s AND epoch = %s
    """, (jito_rank, vote_account_pubkey, epoch))
    conn.commit()
    cur.close()
    conn.close()

def get_validator_history(vote_account_pubkey):
    # Run validator-history-cli to get history for a vote pubkey
    # Adjust the RPC URL as needed
    RPC_URL = 'https://silent-frequent-firefly.solana-mainnet.quiknode.pro/2059a05165e13886cb8226c6b87081ad579860e3/'
    cmd = ['validator-history-cli', '--json-rpc-url', RPC_URL, 'history', vote_account_pubkey]
    result = subprocess.run(cmd, capture_output=True, text=True)
    
    if result.returncode != 0:
        # If there's an error, log it or handle as needed. For now, just return None.
        print(f"Error retrieving history for {vote_account_pubkey}: {result.stderr}")
        return None
    
    return result.stdout

def parse_jito_rank(history_output, target_epoch):
    # Parse the output from validator-history-cli and find jito_rank for target_epoch
    # The lines we expect look something like:
    # Epoch: <epoch> | Commission: <val> | Credits: <val> | Mev Commission: <val> | Mev Earned: <val> | Stake: <val> | Jito Rank: <val> | Superminority: <val> | IP: <val> | Client Type: <val> | Version: <val>
    lines = history_output.strip().split('\n')
    for line in lines:
        if line.startswith('Epoch'):
            parts = line.split('|')
            # parts might look like:
            # ["Epoch: 1234", "Commission: 8", "Credits: 100000", "Mev Commission: 50", "Mev Earned: 1.0", "Stake: 5000", "Jito Rank: 42", "Superminority: 0", "IP: 1.2.3.4", "Client Type: 1", "Version: 1.13"]
            if len(parts) < 7:
                # Not enough parts to contain jito_rank, skip
                continue
            
            # Extract the epoch number and jito_rank
            epoch_str = parts[0].split(':')[1].strip()
            if not epoch_str.isdigit():
                # Not a valid epoch number
                continue
            
            epoch_number = int(epoch_str)
            if epoch_number == target_epoch:
                # Find the jito_rank portion, assuming fixed order of fields
                # The "Rank" field should be the 7th part: parts[6].split(':')[1].strip()
                for p in parts:
                    p = p.strip()
                    if p.lower().startswith("rank"):
                        rank_str = p.split(':', 1)[1].strip()
                        if rank_str.isdigit():
                            return int(rank_str)
    return None

def main():
    vote_account_epochs = fetch_all_vote_account_epochs()

    current_vote_pubkey = None
    history_output = None

    # We can optimize calls to validator-history-cli by grouping epochs per vote_account_pubkey
    # For each unique vote_account_pubkey, call validator-history-cli once
    # Then parse out the epochs you need from that output.
    from collections import defaultdict
    vote_epoch_map = defaultdict(list)
    for v, e in vote_account_epochs:
        vote_epoch_map[v].append(e)

    for vote_account_pubkey, epochs in vote_epoch_map.items():
        # Get the history once per vote_account_pubkey
        history_output = get_validator_history(vote_account_pubkey)
        if history_output is None:
            # Could not retrieve or error encountered, skip
            continue

        # For each epoch, parse jito_rank and update
        for epoch in epochs:
            jito_rank = parse_jito_rank(history_output, epoch)
            if jito_rank is not None:
                update_jito_rank(vote_account_pubkey, epoch, jito_rank)
            # If jito_rank is None, it means that epoch wasn't found in the history output
            # or no jito_rank data was available. We just skip updating in that case.

if __name__ == "__main__":
    main()
