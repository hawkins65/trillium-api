import psycopg2
from db_config import db_params

def main():
    # 1. Load the list of sandwichers from sandwichers.txt.
    try:
        with open('./sandwichers.txt', 'r') as f:
            sandwichers_set = {line.strip() for line in f if line.strip()}
    except Exception as e:
        print("Error reading sandwichers.txt:", e)
        return

    # 2. Connect to the PostgreSQL database.
    try:
        conn = psycopg2.connect(**db_params)
        cur = conn.cursor()
    except Exception as e:
        print("Database connection error:", e)
        return

    try:
        # 3. Process each line of edgevana-vote-keys.txt.
        with open('./edgevana-vote-keys.txt', 'r') as f:
            for line in f:
                original_line = line.strip()
                if not original_line:
                    continue  # skip empty lines

                # Expected format: "first4...last4"
                if "..." in original_line:
                    parts = original_line.split("...")
                    if len(parts) != 2:
                        print(f"{original_line}: Skipping line with unexpected format.")
                        continue
                    first_four, last_four = parts[0], parts[1]
                else:
                    print(f"{original_line}: Skipping line; expected '...' separator not found.")
                    continue

                # 4. Query validator_stats for all rows with matching vote_account_pubkey segments.
                cur.execute(
                    """
                    SELECT vote_account_pubkey, identity_pubkey 
                    FROM validator_stats 
                    WHERE left(vote_account_pubkey, 4) = %s 
                      AND right(vote_account_pubkey, 4) = %s
                    """,
                    (first_four, last_four)
                )
                rows = cur.fetchall()

                if not rows:
                    print(f"{original_line}: No match found for vote_account segments: {first_four} ... {last_four}")
                    continue

                # 5. Build a dictionary to deduplicate by identity_pubkey.
                # For each unique identity_pubkey, we save one representative vote_account_pubkey.
                unique_identities = {}
                for vote_account_pubkey, identity_pubkey in rows:
                    if identity_pubkey not in unique_identities:
                        unique_identities[identity_pubkey] = vote_account_pubkey

                # 6. For each unique identity_pubkey, look up the name and determine sandwicher status.
                for identity_pubkey, vote_account_pubkey in unique_identities.items():
                    cur.execute(
                        "SELECT name FROM validator_info WHERE identity_pubkey = %s",
                        (identity_pubkey,)
                    )
                    row = cur.fetchone()
                    name = row[0] if row else "Not found"
                    sandwicher = "true" if identity_pubkey in sandwichers_set else "false"

                    # 7. Output the result, starting with the original edgevana line.
                    print(f"{original_line}: {identity_pubkey}, {vote_account_pubkey}, {name}, sandwicher={sandwicher}")
    except Exception as e:
        print("Error processing edgevana-vote-keys.txt:", e)
    finally:
        cur.close()
        conn.close()

if __name__ == '__main__':
    main()
