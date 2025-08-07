import sys
import psycopg2
import argparse
from typing import Dict, List, Tuple
from time import perf_counter
import importlib.util
import os

# Setup unified logging
script_dir = os.path.dirname(os.path.abspath(__file__))
logging_config_path = os.path.join(script_dir, "999_logging_config.py")
spec = importlib.util.spec_from_file_location("logging_config", logging_config_path)
logging_config = importlib.util.module_from_spec(spec)
spec.loader.exec_module(logging_config)
logger = logging_config.setup_logging(os.path.basename(__file__).replace('.py', ''))
from statistics import mean, median
from collections import defaultdict

def get_db_connection_string(db_params: Dict[str, str]) -> str:
    return f"postgresql://{db_params['user']}@{db_params['host']}:{db_params['port']}/{db_params['database']}?sslmode={db_params['sslmode']}"

def execute_query(cur, query: str, params: tuple = None) -> List[tuple]:
    try:
        start_time = perf_counter()
        if params:
            cur.execute(query, params)
        else:
            cur.execute(query)
        result = cur.fetchall()
        end_time = perf_counter()
        logger.info(f"Query completed in {end_time - start_time:.4f} seconds")
        return result
    except Exception as e:
        logger.error(f"Error executing query: {str(e)}")
        logger.error(f"Query: {query}")
        logger.error(f"Params: {params}")
        raise

def get_block_sequences(data: List[tuple]) -> List[List[tuple]]:
    sequences = []
    current_sequence = []
    
    for row in data:
        if not current_sequence:
            current_sequence.append(row)
            continue
            
        if row[0] == current_sequence[-1][0] + 1:
            current_sequence.append(row)
        else:
            if len(current_sequence) > 1:
                sequences.append(current_sequence)
            current_sequence = [row]
    
    if len(current_sequence) > 1:
        sequences.append(current_sequence)
    
    return sequences

def calculate_block_times(sequences: List[List[tuple]]) -> Tuple[float, float]:
    sequence_averages = []
    
    for sequence in sequences:
        if len(sequence) < 2:
            continue
            
        first_block = sequence[0]
        last_block = sequence[-1]
        
        if first_block[1] is None or last_block[1] is None:
            continue
            
        total_time = last_block[1] - first_block[1]
        num_blocks = len(sequence) - 1
        
        avg_block_time = total_time / num_blocks
        if 0.2 <= avg_block_time <= 1.0:
            sequence_averages.append(avg_block_time)
    
    logger.info(f"Found {len(sequence_averages)} valid sequence averages")
    
    if sequence_averages:
        return mean(sequence_averages), median(sequence_averages)
    return None, None

def analyze_blocks_by_epoch(cur, conn, target_epoch: int = None) -> Dict[int, Dict[str, float]]:
    logger.info("Starting block analysis...")
    
    if target_epoch is not None:
        query = """
            SELECT block_slot, block_time, epoch
            FROM validator_data
            WHERE epoch = %(epoch)s
            ORDER BY block_slot;
        """
        min_epoch = max_epoch = target_epoch
    else:
        query = """
            SELECT block_slot, block_time, epoch
            FROM validator_data
            WHERE epoch >= %(epoch_start)s AND epoch <= %(epoch_end)s
            ORDER BY block_slot;
        """
        cur.execute("SELECT MIN(epoch), MAX(epoch) FROM validator_data;")
        min_epoch, max_epoch = cur.fetchone()
    
    results = {}
    BATCH_SIZE = 10 if target_epoch is None else 1
    total_epochs = max_epoch - min_epoch + 1
    logger.info(f"Processing {total_epochs} epochs in batches of {BATCH_SIZE}")
    
    for epoch_start in range(min_epoch, max_epoch + 1, BATCH_SIZE):
        epoch_end = min(epoch_start + BATCH_SIZE - 1, max_epoch)
        logger.info(f"Processing epochs {epoch_start} to {epoch_end}")
        
        if target_epoch is not None:
            data = execute_query(cur, query, {'epoch': target_epoch})
        else:
            data = execute_query(cur, query, {'epoch_start': epoch_start, 'epoch_end': epoch_end})
            
        logger.info(f"Retrieved {len(data)} blocks for processing")
        
        epoch_data = defaultdict(list)
        for row in data:
            epoch_data[row[2]].append((row[0], row[1]))
            
        for epoch, epoch_blocks in epoch_data.items():
            sequences = get_block_sequences(sorted(epoch_blocks))
            logger.info(f"Epoch {epoch}: Found {len(sequences)} consecutive block sequences")
            
            mean_time, median_time = calculate_block_times(sequences)
            if mean_time is not None and median_time is not None:
                mean_ms = round(mean_time * 1000)
                median_ms = round(median_time * 1000)
                results[epoch] = {
                    'mean': mean_ms,
                    'median': median_ms
                }
                cur.execute("""
                    UPDATE epoch_aggregate_data 
                    SET avg_slot_duration_ms = %(mean)s 
                    WHERE epoch = %(epoch)s
                """, {'mean': mean_ms, 'epoch': epoch})
                conn.commit()
    
    return results

def main():
    # Logging config moved to unified configuration
    
    if len(sys.argv) > 1:
        target_epoch = int(sys.argv[1])
    else:
        target_epoch = None

    # Logging config moved to unified configuration
    
    try:
        from db_config import db_params
        conn = psycopg2.connect(get_db_connection_string(db_params))
        cur = conn.cursor()
        
        results = analyze_blocks_by_epoch(cur, conn, target_epoch)
        
        print("\nBlock Production Time Analysis by Epoch:")
        print("-" * 50)
        print(f"{'Epoch':<10} {'Mean (ms)':<15} {'Median (ms)':<15}")
        print("-" * 50)
        
        for epoch in sorted(results.keys()):
            print(f"{epoch:<10} {results[epoch]['mean']:<15.2f} {results[epoch]['median']:<15.2f}")
            
    except Exception as e:
        logger.error(f"Error in main: {str(e)}")
        raise
    finally:
        if 'cur' in locals():
            cur.close()
        if 'conn' in locals():
            conn.close()

if __name__ == "__main__":
    main()