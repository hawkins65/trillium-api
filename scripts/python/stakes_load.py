import json
import csv
import psycopg2


# PostgreSQL database connection parameters
db_params = {
    "host": "private-dbaas-db-9382663-do-user-15771670-0.c.db.ondigitalocean.com",
    "port": "25060",
    "database": "sol_blocks",
    "user": "smilax",
    "sslmode": "require"
}

# JSON file name
json_file = "stakes.json"

def load_stakes_data(json_file, db_params, error_file="error_stakes.csv"):
    """Loads stake data from a JSON file into the PostgreSQL database,
    handling missing required fields and logging records with errors to a CSV file."""

    with open(json_file, 'r') as f:
        data = json.load(f)

    error_rows = []

    try:
        with psycopg2.connect(**db_params) as conn:
            with conn.cursor() as cur:
                for stake in data:
                    # Check for required fields
                    required_fields = ['stakePubkey', 'stakeType', 'accountBalance', 'creditsObserved', 'delegatedStake', 'activeStake', 'delegatedVoteAccountAddress', 'activationEpoch', 'staker', 'withdrawer', 'rentExemptReserve']
                    missing_fields = [field for field in required_fields if field not in stake]

                    if missing_fields:
                        error_rows.append(stake)
                        print(f"Skipping record with missing fields: {missing_fields} - {stake['stakePubkey']}")
                        continue  # Skip to the next record

                    # Handle optional deactivationEpoch
                    deactivation_epoch = stake.get('deactivationEpoch', None)

                    cur.execute("""
                        INSERT INTO stakes (stakePubkey, stakeType, accountBalance, creditsObserved, delegatedStake, activeStake, delegatedVoteAccountAddress, activationEpoch, staker, withdrawer, rentExemptReserve, deactivationEpoch)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        ON CONFLICT (stakePubkey) DO UPDATE SET
                            stakeType = EXCLUDED.stakeType,
                            accountBalance = EXCLUDED.accountBalance,
                            creditsObserved = EXCLUDED.creditsObserved,
                            delegatedStake = EXCLUDED.delegatedStake,
                            activeStake = EXCLUDED.activeStake,
                            delegatedVoteAccountAddress = EXCLUDED.delegatedVoteAccountAddress,
                            activationEpoch = EXCLUDED.activationEpoch,
                            staker = EXCLUDED.staker,
                            withdrawer = EXCLUDED.withdrawer,
                            rentExemptReserve = EXCLUDED.rentExemptReserve,
                            deactivationEpoch = EXCLUDED.deactivationEpoch 
                    """, (
                        stake['stakePubkey'], stake['stakeType'], stake['accountBalance'],
                        stake['creditsObserved'], stake['delegatedStake'], stake['activeStake'],
                        stake['delegatedVoteAccountAddress'], stake['activationEpoch'],
                        stake['staker'], stake['withdrawer'], stake['rentExemptReserve'], 
                        deactivation_epoch
                    ))
    except psycopg2.Error as e:
        print(f"Database error: {e}")
    finally:
        # Write error rows to CSV with all fields (including deactivationEpoch)
        with open(error_file, 'w', newline='') as f:
            fieldnames = required_fields + ['deactivationEpoch']  
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(error_rows)

        print("Stakes data loaded/updated. Records with errors saved to:", error_file)

if __name__ == "__main__":
    load_stakes_data(json_file, db_params)