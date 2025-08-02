import psycopg2
import subprocess
from db_config import db_params

TEST_VOTE_PUBKEY = 'tri1cHBy47fPyhCvrCf6FnR7Mz6XdSoSBah2FsZVQeT'
RPC_URL = 'https://silent-frequent-firefly.solana-mainnet.quiknode.pro/2059a05165e13886cb8226c6b87081ad579860e3/'

def fetch_epochs_for_vote_pubkey(vote_pubkey):
    conn = psycopg2.connect(**db_params)
    cur = conn.cursor()
    cur.execute("""
        SELECT DISTINCT epoch 
        FROM validator_stats 
        WHERE vote_account_pubkey = %s 
        ORDER BY epoch
    """, (vote_pubkey,))
    epochs = [row[0] for row in cur.fetchall()]
    cur.close()
    conn.close()
    return epochs

def update_jito_rank(vote_account_pubkey, epoch, jito_rank):
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
    cmd = ['validator-history-cli', '--json-rpc-url', RPC_URL, 'history', vote_account_pubkey]
    result = subprocess.run(cmd, capture_output=True, text=True)
    
    if result.returncode != 0:
        print(f"Error retrieving history for {vote_account_pubkey}: {result.stderr}")
        return None
    
    return result.stdout

def parse_jito_rank(history_output, target_epoch):
    # The output format is like:
    # Epoch: 707 | Commission: 0 | Epoch Credits: 6716677 | MEV Commission: 5000 | MEV Earned: 67.94 | Stake: ... | Rank: 102 | Superminority: 0 | IP: ... | Client Type: 1 | Client Version: ... | Last Updated: ...
    lines = history_output.strip().split('\n')
    for line in lines:
        if line.startswith('Epoch'):
            parts = [part.strip() for part in line.split('|')]
            # Expecting parts like:
            # ["Epoch: 707", "Commission: 0", "Epoch Credits: 6716677", "MEV Commission: 5000", "MEV Earned: 67.94", "Stake: ...", "Rank: 102", "Superminority: 0", "IP: ...", "Client Type: 1", "Client Version: ...", "Last Updated: ..."]
            if len(parts) < 7:
                continue
            
            # Extract epoch
            epoch_str = parts[0].split(':', 1)[1].strip()
            if not epoch_str.isdigit():
                continue
            epoch_number = int(epoch_str)

            if epoch_number == target_epoch:
                # Find the part that starts with "Rank:"
                for p in parts:
                    if p.lower().startswith("rank:"):
                        rank_str = p.split(':', 1)[1].strip()
                        if rank_str.isdigit():
                            return int(rank_str)
    return None

def main():
    epochs = fetch_epochs_for_vote_pubkey(TEST_VOTE_PUBKEY)
    if not epochs:
        print(f"No epochs found in the database for vote_account_pubkey: {TEST_VOTE_PUBKEY}")
        return

    history_output = get_validator_history(TEST_VOTE_PUBKEY)
    if history_output is None:
        print("Could not retrieve validator history.")
        return

    # For each epoch in the database, parse out the rank if present
    updates = []
    for epoch in epochs:
        rank = parse_jito_rank(history_output, epoch)
        if rank is not None:
            updates.append((epoch, rank))

    if updates:
        print("The following epochs will be updated with their respective ranks:")
        for epoch, rank in updates:
            print(f"Epoch: {epoch}, Rank: {rank}")
        
        # If you're sure you want to update the database, uncomment the following lines:
        # for epoch, rank in updates:
        #     update_jito_rank(TEST_VOTE_PUBKEY, epoch, rank)
        # print("Database updates completed.")
    else:
        print("No epochs from the CLI output match the database records, or no Rank field found.")

if __name__ == "__main__":
    main()
