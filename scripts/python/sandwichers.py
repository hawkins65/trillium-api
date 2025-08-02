import psycopg2
from db_config import db_params

def main():
    # Connect to the database.
    conn = psycopg2.connect(**db_params)
    cur = conn.cursor()

    # Read the pubkeys from the file, stripping any extra whitespace/newlines.
    with open('./sandwichers.txt', 'r') as f:
        pubkeys = [line.strip() for line in f if line.strip()]

    # For each pubkey, query the validator_info table to get the name.
    for pubkey in pubkeys:
        cur.execute(
            "SELECT name FROM validator_info WHERE identity_pubkey = %s",
            (pubkey,)
        )
        result = cur.fetchone()
        if result:
            name = result[0]
        else:
            name = "Not found"  # or you can choose to skip or handle differently

        # Output the pubkey and name.
        print(f"{pubkey}, {name}")

    # Clean up.
    cur.close()
    conn.close()

if __name__ == '__main__':
    main()
