from db_config import db_params
import psycopg2
import pandas as pd
from datetime import datetime, timedelta
import math
import warnings
warnings.filterwarnings("ignore", message="pandas only supports SQLAlchemy connectable")

def get_slot_data(conn, start_epoch, end_epoch):
    query = """
    SELECT epoch, block_slot, block_time
    FROM validator_data
    WHERE epoch BETWEEN %s AND %s
    ORDER BY epoch, block_slot
    """
    
    df = pd.read_sql_query(query, conn, params=(start_epoch, end_epoch))
    df['block_time'] = pd.to_datetime(df['block_time'], unit='s')
    return df

def calculate_avg_slot_duration(df, slots_per_epoch=432000, interval_size=1000):
    df = df.sort_values(['epoch', 'block_slot'])
    df['slot_duration'] = df.groupby('epoch')['block_time'].diff().dt.total_seconds()
    
    results = []
    for epoch in df['epoch'].unique():
        epoch_df = df[df['epoch'] == epoch]
        epoch_start_slot = epoch * slots_per_epoch
        epoch_end_slot = (epoch + 1) * slots_per_epoch - 1
        epoch_start_time = epoch_df['block_time'].min()
        
        for interval in range(1, 433):  # 432 intervals per epoch
            interval_start = epoch_start_slot + (interval - 1) * interval_size
            interval_end = min(interval_start + interval_size - 1, epoch_end_slot)
            
            interval_df = epoch_df[(epoch_df['block_slot'] >= interval_start) & (epoch_df['block_slot'] <= interval_end)]
            
            if not interval_df.empty:
                avg_duration = interval_df['slot_duration'].mean()
                interval_start_time = interval_df['block_time'].min()
                interval_end_time = interval_df['block_time'].max()
                interval_total_duration = (interval_end_time - interval_start_time).total_seconds()
                blocks_produced = len(interval_df)
            else:
                avg_duration = None
                estimated_start_time = epoch_start_time + timedelta(seconds=(interval - 1) * interval_size * 0.4)
                estimated_end_time = estimated_start_time + timedelta(seconds=interval_size * 0.4)
                interval_start_time = estimated_start_time
                interval_end_time = estimated_end_time
                interval_total_duration = interval_size * 0.4
                blocks_produced = 0

            results.append({
                'epoch': epoch,
                'interval': interval,
                'interval_start_slot': interval_start,
                'interval_end_slot': interval_end,
                'interval_start_time': interval_start_time,
                'interval_end_time': interval_end_time,
                'interval_total_duration': interval_total_duration,
                'avg_slot_duration': avg_duration,
                'interval_blocks_produced': blocks_produced
            })
    
    return pd.DataFrame(results)

def get_max_epoch(conn):
    query = "SELECT MAX(epoch) FROM validator_data"
    with conn.cursor() as cur:
        cur.execute(query)
        return cur.fetchone()[0]

def main():
    conn = psycopg2.connect(**db_params)
    
    try:
        max_epoch = get_max_epoch(conn)
        print(f"Maximum epoch in the database: {max_epoch}")

        # Generate CSVs for every 10 epochs starting from 600
        start_epoch = 600
        while start_epoch <= max_epoch:
            end_epoch = min(start_epoch + 9, max_epoch)
            
            print(f"Processing epochs {start_epoch} to {end_epoch}...")
            df = get_slot_data(conn, start_epoch, end_epoch)
            results = calculate_avg_slot_duration(df)
            
            csv_filename = f'avg_slot_duration_epochs_{start_epoch}_to_{end_epoch}.csv'
            results.to_csv(csv_filename, index=False)
            print(f"Results saved to {csv_filename}")
            
            start_epoch += 10

        # Generate CSV for the most recent 10 epochs
        start_epoch = max(600, max_epoch - 9)
        print(f"Processing most recent 10 epochs ({start_epoch} to {max_epoch})...")
        df = get_slot_data(conn, start_epoch, max_epoch)
        results = calculate_avg_slot_duration(df)
        
        csv_filename = f'avg_slot_duration_most_recent_10_epochs_{start_epoch}_to_{max_epoch}.csv'
        results.to_csv(csv_filename, index=False)
        print(f"Results for most recent 10 epochs saved to {csv_filename}")

        print("All processing complete.")
        
    finally:
        conn.close()

if __name__ == "__main__":
    main()