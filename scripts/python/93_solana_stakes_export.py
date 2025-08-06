import psycopg2
import csv
from db_config import db_params

from db_config import db_params
import psycopg2

def get_db_connection_string(db_params):
    return f"postgresql://{db_params['user']}@{db_params['host']}:{db_params['port']}/{db_params['database']}?sslmode={db_params['sslmode']}"

def get_db_connection():
    conn_string = get_db_connection_string(db_params)
    return psycopg2.connect(conn_string)
    
# Define the withdrawer names dictionary
withdrawer_names = {
    "4ZJhPQAgUseCsWhKvJLTmmRRUV74fdoTpQLNfKoekbPY": "Solana Foundation Withdraw Authority",
    "mpa4abUkjQoAvPzREkh5Mo75hZhPFQ2FSH6w7dWKuQ5": "Solana Foundation Delegation",
    "6iQKfEyhr3bZMotVkW6beNZz5CPAkiwvgV2CTje9pVSS": "Jito (JitoSOL) Stake Pool Withdraw Authority",
    "FZEaZMmrRC3PDPFMzqooKLS2JjoyVkKNd2MkHjr7Xvyq": "Edgevana (edgeSOL) Stake Pool Withdraw Authority",
    "6WecYymEARvjG5ZyqkrVQ6YkhPfujNzWpSPwNKXHCbV2": "BlazeStake (bSOL) Stake Pool Withdraw Authority",
    "HbJTxftxnXgpePCshA8FubsRj9MW4kfPscfuUfn44fnt": "JPool (JSOL) Stake Pool Withdraw Authority",
    "GdNXJobf8fbTR5JSE7adxa6niaygjx4EEbnnRaDCHMMW": "The Vault (vSOL) Stake Pool Withdraw Authority",
    "1so1ctTM24PdU7RLZJzJKYYVYri3gjNeCd8nmHbpdXg": "Solscan",
    "TdbUsGdmK2PfyLYQmnvJJcK6xWrs2AFtHjc575WRsmW": "Stake Pool Withdraw Authority",
    "7cgg6KhPd1G8oaoB48RyPDWu7uZs51jUpDYB3eq4VebH": "Marinade Bond Stake Authority",
    "3b7XQeZ8nSMyjcQGTFJS5kBw4pXS2SqtB9ooHCnF2xV9": "Liquid (LST) Stake Pool Withdraw Authority",
    "3rBnnH9TTgd3xwu48rnzGsaQkSr1hR64nY71DrDt6VrQ": "Sanctum Pool (SOL) Reserves",
    "63XKbuGWYkWejCPmhrnBwvPyyt72HqBh68sCFSwi48Tx": "funny",
    "9wKLKndJeLqK1JtQ773YBdjVzwbEX9xqdu4i94coacX3": "Olnaava",
    "9eG63CdHjsfhHmobHgLtESGC8GabbmRcaSpHAZrtmhco": "Marinade (mSOL) Stake Pool Withdraw Authority",
    "EXsJCamTqHJqRqNaB4ZAszGpFw6psMsk9HfjkrrWwJBc": "Colossus",
    "stWirqFCf2Uts1JBL1Jsd3r6VBWhgnpdPxCTe1MFjrq": "Marinade Native 2"
}

# Function to get human-readable withdrawer name
def get_withdrawer_name(withdrawer):
    return withdrawer_names.get(withdrawer, withdrawer)  # Returns name if found, otherwise original key

# Get a database connection
def get_db_connection():
    conn_string = get_db_connection_string(db_params)
    return psycopg2.connect(conn_string)

# List of columns to format with thousands separators and 5 decimal places
sol_columns = {
    "total_active_stake", "total_activating_stake", "total_deactivating_stake",
    "activating_vs_deactivating_stake", "max_active_stake", "avg_active_stake",
    "median_active_stake", "min_active_stake"
}

# Save query result to a CSV file with withdrawer name substitution and formatted SOL and account_count values
def export_to_csv(cursor, query, filename):
    cursor.execute(query)
    rows = cursor.fetchall()
    with open(filename, 'w', newline='') as csvfile:
        writer = csv.writer(csvfile)
        headers = [desc[0] for desc in cursor.description]
        writer.writerow(headers)  # write headers

        # Write each row, applying withdrawer name substitution, SOL formatting, and account_count formatting
        for row in rows:
            row = list(row)
            # Identify the 'withdrawer' column by name
            if 'withdrawer' in headers:
                withdrawer_index = headers.index('withdrawer')
                row[withdrawer_index] = get_withdrawer_name(row[withdrawer_index])

            # Format specified SOL columns and account_count columns with thousands separators
            for i, header in enumerate(headers):
                if header in sol_columns:
                    # Convert to float if possible and format with thousands separators
                    try:
                        row[i] = f"{float(row[i]):,.5f}"
                    except ValueError:
                        pass  # If conversion fails, keep the original value
                elif 'account_count' in header:
                    # Convert to int if possible and format with thousands separators
                    try:
                        row[i] = f"{int(row[i]):,}"
                    except ValueError:
                        pass  # If conversion fails, keep the original value

            writer.writerow(row)

# Define your queries
queries = {
    "1_total_stake_by_vote_account.csv": """
        SELECT epoch, vote_account_pubkey,
               ROUND(COALESCE(SUM(active_stake), 0) / 1000000000.0, 5) AS total_active_stake,
               ROUND(COALESCE(SUM(activating_stake), 0) / 1000000000.0, 5) AS total_activating_stake,
               ROUND(COALESCE(SUM(deactivating_stake), 0) / 1000000000.0, 5) AS total_deactivating_stake,
               ROUND(COALESCE(SUM(activating_stake - deactivating_stake), 0) / 1000000000.0, 5) AS activating_vs_deactivating_stake
        FROM stake_accounts
        GROUP BY epoch, vote_account_pubkey
        ORDER BY total_active_stake DESC;
    """,
    "2a_total_stake_by_staker.csv": """
        SELECT epoch, staker,
               ROUND(COALESCE(SUM(active_stake), 0) / 1000000000.0, 5) AS total_active_stake,
               ROUND(COALESCE(SUM(activating_stake), 0) / 1000000000.0, 5) AS total_activating_stake,
               ROUND(COALESCE(SUM(deactivating_stake), 0) / 1000000000.0, 5) AS total_deactivating_stake,
               ROUND(COALESCE(SUM(activating_stake - deactivating_stake), 0) / 1000000000.0, 5) AS activating_vs_deactivating_stake
        FROM stake_accounts
        GROUP BY epoch, staker
        ORDER BY total_active_stake DESC;
    """,
    "2b_total_stake_by_withdrawer.csv": """
        SELECT epoch, withdrawer,
               ROUND(COALESCE(SUM(active_stake), 0) / 1000000000.0, 5) AS total_active_stake,
               ROUND(COALESCE(SUM(activating_stake), 0) / 1000000000.0, 5) AS total_activating_stake,
               ROUND(COALESCE(SUM(deactivating_stake), 0) / 1000000000.0, 5) AS total_deactivating_stake,
               ROUND(COALESCE(SUM(activating_stake - deactivating_stake), 0) / 1000000000.0, 5) AS activating_vs_deactivating_stake
        FROM stake_accounts
        GROUP BY epoch, withdrawer
        ORDER BY total_active_stake DESC;
    """,
    "2c_total_stake_by_custodian.csv": """
        SELECT epoch, custodian,
               ROUND(COALESCE(SUM(active_stake), 0) / 1000000000.0, 5) AS total_active_stake,
               ROUND(COALESCE(SUM(activating_stake), 0) / 1000000000.0, 5) AS total_activating_stake,
               ROUND(COALESCE(SUM(deactivating_stake), 0) / 1000000000.0, 5) AS total_deactivating_stake,
               ROUND(COALESCE(SUM(activating_stake - deactivating_stake), 0) / 1000000000.0, 5) AS activating_vs_deactivating_stake
        FROM stake_accounts
        GROUP BY epoch, custodian
        ORDER BY total_active_stake DESC;
    """,
    "3_aggregate_totals.csv": """
        SELECT epoch,
               ROUND(COALESCE(SUM(active_stake), 0) / 1000000000.0, 5) AS total_active_stake,
               ROUND(COALESCE(SUM(activating_stake), 0) / 1000000000.0, 5) AS total_activating_stake,
               ROUND(COALESCE(SUM(deactivating_stake), 0) / 1000000000.0, 5) AS total_deactivating_stake,
               ROUND(COALESCE(SUM(activating_stake - deactivating_stake), 0) / 1000000000.0, 5) AS activating_vs_deactivating_stake
        FROM stake_accounts
        GROUP BY epoch
        ORDER BY total_active_stake DESC;
    """,
    "4_active_stake_distribution.csv": """
        WITH stake_distribution AS (
            SELECT epoch,
                   active_stake,
                   ROW_NUMBER() OVER (PARTITION BY epoch ORDER BY active_stake) AS rn,
                   COUNT(*) OVER (PARTITION BY epoch) AS total_count
            FROM stake_accounts
        )
        SELECT epoch,
               ROUND(COALESCE(MIN(active_stake), 0) / 1000000000.0, 5) AS min_active_stake,
               ROUND(COALESCE(MAX(active_stake), 0) / 1000000000.0, 5) AS max_active_stake,
               ROUND(COALESCE(AVG(active_stake), 0) / 1000000000.0, 5) AS avg_active_stake,
               ROUND(CAST(PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY active_stake) AS numeric) / 1000000000.0, 5) AS median_active_stake
        FROM stake_accounts
        GROUP BY epoch
        ORDER BY avg_active_stake DESC;
    """,
    "5_zero_active_stake_accounts.csv": """
        SELECT epoch, stake_pubkey, vote_account_pubkey,
               ROUND(COALESCE(active_stake, 0) / 1000000000.0, 5) AS active_stake,
               ROUND(COALESCE(activating_stake, 0) / 1000000000.0, 5) AS activating_stake,
               ROUND(COALESCE(deactivating_stake, 0) / 1000000000.0, 5) AS deactivating_stake
        FROM stake_accounts
        WHERE COALESCE(active_stake, 0) = 0
        ORDER BY epoch, stake_pubkey;
    """,
    "6a_accounts_per_vote_account.csv": """
        SELECT epoch, vote_account_pubkey,
               COUNT(*) AS account_count
        FROM stake_accounts
        GROUP BY epoch, vote_account_pubkey
        ORDER BY account_count DESC;
    """,
    "6b_accounts_per_custodian.csv": """
        SELECT epoch, custodian,
               COUNT(*) AS account_count
        FROM stake_accounts
        GROUP BY epoch, custodian
        ORDER BY account_count DESC;
    """,
    "7_stake_changes_for_specific_vote_account.csv": """
        SELECT epoch, vote_account_pubkey,
               ROUND(SUM(active_stake) / 1000000000.0, 5) AS total_active_stake
        FROM stake_accounts
        WHERE vote_account_pubkey = 'tri1cHBy47fPyhCvrCf6FnR7Mz6XdSoSBah2FsZVQeT'
        GROUP BY epoch, vote_account_pubkey
        ORDER BY total_active_stake DESC;
    """
}

def main():
    # Connect to the database
    conn = get_db_connection()
    cursor = conn.cursor()

    # Execute each query and export the results to CSV
    for filename, query in queries.items():
        #print(f"Exporting {filename}...")
        #export_to_csv(cursor, query, f"/home/smilax/trillium_api/solana-stakes/{filename}")
        continue
    # Clean up
    cursor.close()
    conn.close()

if __name__ == "__main__":
    main()
