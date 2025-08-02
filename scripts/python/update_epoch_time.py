import psycopg2
from decimal import Decimal
import logging

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# PostgreSQL database connection parameters
db_params = {
    "host": "private-dbaas-db-9382663-do-user-15771670-0.c.db.ondigitalocean.com",
    "port": "25060",
    "database": "sol_blocks",
    "user": "smilax",
    "sslmode": "require"
}

def update_elapsed_time_per_epoch():
    try:
        conn = psycopg2.connect(**db_params)
        cursor = conn.cursor()
        
        cursor.execute("""
            UPDATE epoch_aggregate_data
            SET elapsed_time_per_epoch = (
                ((max_block_time - min_block_time)::NUMERIC / NULLIF((max_slot - min_slot), 0)) * 432000
            );
        """)
        
        conn.commit()
        logger.info("Updated elapsed_time_per_epoch for all rows successfully.")
    
    except Exception as error:
        logger.error(f"Error updating elapsed_time_per_epoch: {error}")
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

def calculate_and_update_epochs_per_year():
    weights = [Decimal('0.2649'), Decimal('0.1987'), Decimal('0.1490'), Decimal('0.1118'), Decimal('0.0838'), 
               Decimal('0.0629'), Decimal('0.0471'), Decimal('0.0354'), Decimal('0.0265'), Decimal('0.0199')]

    try:
        conn = psycopg2.connect(**db_params)
        cursor = conn.cursor()
        
        cursor.execute("SELECT epoch FROM epoch_aggregate_data;")
        epochs = cursor.fetchall()
        
        for epoch_row in epochs:
            current_epoch = epoch_row[0]
            epochs_per_year = Decimal('0')
            weight_sum = Decimal('0')            
            for i in range(10):
                if current_epoch - i > 0:
                    cursor.execute("""
                        SELECT (365 * 24 * 60 * 60) / NULLIF(elapsed_time_per_epoch, 0)
                        FROM epoch_aggregate_data
                        WHERE epoch = %s;
                    """, (current_epoch - i,))
                    
                    temp_epochs_per_year = cursor.fetchone()
                    
                    if temp_epochs_per_year and temp_epochs_per_year[0] is not None:
                        temp_epochs_per_year = Decimal(str(temp_epochs_per_year[0]))
                        epochs_per_year += temp_epochs_per_year * weights[i]
                        weight_sum += weights[i]
            
            if weight_sum > 0:
                epochs_per_year /= weight_sum
            
            cursor.execute("""
                UPDATE epoch_aggregate_data
                SET epochs_per_year = %s
                WHERE epoch = %s;
            """, (round(float(epochs_per_year), 2), current_epoch))
        
        conn.commit()
        logger.info("Calculated and updated epochs_per_year for all rows successfully.")
    
    except Exception as error:
        logger.error(f"Error calculating and updating epochs_per_year: {error}")
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

def main():
    logger.info("Starting database update process...")
    update_elapsed_time_per_epoch()
    calculate_and_update_epochs_per_year()
    logger.info("Database update process completed.")

if __name__ == "__main__":
    main()