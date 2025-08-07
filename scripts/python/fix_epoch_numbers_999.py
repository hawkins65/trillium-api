import psycopg2
import math
# PostgreSQL database connection parameters
from db_config import db_params

def update_epochs():
    try:
        # Establish the database connection
        conn = psycopg2.connect(**db_params)
        cur = conn.cursor()
        
        # Define the starting block_slot and slots per epoch
        base_epoch = 666
        base_block_slot = 287712000
        slots_per_epoch = 432000

        # Find all rows with epoch=999
        cur.execute("SELECT block_slot FROM validator_data WHERE epoch = 999")
        rows = cur.fetchall()

        for row in rows:
            block_slot = row[0]
            # Calculate the correct epoch
            correct_epoch = base_epoch + math.floor((block_slot - base_block_slot) / slots_per_epoch)

            # Update the row with the correct epoch
            cur.execute(
                "UPDATE validator_data SET epoch = %s WHERE block_slot = %s",
                (correct_epoch, block_slot)
            )

        # Commit the changes
        conn.commit()

        print(f"Updated {cur.rowcount} rows.")

    except Exception as e:
        print(f"Error: {e}")

    finally:
        cur.close()
        conn.close()

if __name__ == "__main__":
    update_epochs()
