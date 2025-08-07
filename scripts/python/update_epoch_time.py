import psycopg2
from decimal import Decimal
import importlib.util
import os

# Setup unified logging
script_dir = os.path.dirname(os.path.abspath(__file__))
logging_config_path = os.path.join(script_dir, "999_logging_config.py")
spec = importlib.util.spec_from_file_location("logging_config", logging_config_path)
logging_config = importlib.util.module_from_spec(spec)
spec.loader.exec_module(logging_config)
logger = logging_config.setup_logging(os.path.basename(__file__).replace('.py', ''))
# Set up logging
# Logging config moved to unified configurations - %(levelname)s - %(message)s')
# Logger setup moved to unified configuration

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