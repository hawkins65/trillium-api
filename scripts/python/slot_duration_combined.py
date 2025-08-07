import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import psycopg2
from db_config import db_params
from datetime import datetime, timedelta
import os
from PIL import Image
import numpy as np
from matplotlib.offsetbox import OffsetImage, AnnotationBbox
import matplotlib.ticker as ticker

def add_banner_to_figure(fig, scale_factor=0.03, banner_path="/var/www/html/images/favicon.ico"):
    banner_image = Image.open(banner_path)
    fig_width_inches = fig.get_figwidth()
    banner_width_inches = fig_width_inches * scale_factor
    aspect_ratio = banner_image.width / banner_image.height
    banner_height_inches = banner_width_inches / aspect_ratio
    banner_width_pixels = int(banner_width_inches * fig.dpi)
    banner_height_pixels = int(banner_height_inches * fig.dpi)
    banner_image = banner_image.resize((banner_width_pixels, banner_height_pixels))
    banner_offset = OffsetImage(np.array(banner_image), zoom=1)
    banner_ab = AnnotationBbox(banner_offset, 
                               (0.5, 0.03),  # Adjusted position
                               xycoords='figure fraction', 
                               box_alignment=(0.5, 0),
                               bboxprops=dict(edgecolor='none', facecolor='none'))
    fig.add_artist(banner_ab)
    fig.text(0.5, 0.01, "Fueled by Trillium | Solana", fontsize=16, fontweight='bold', 
             ha='center', va='center', fontfamily='sans-serif')

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
                'interval_total_duration': interval_total_duration * 1000, # Convert to milliseconds
                'avg_slot_duration': avg_duration,
                'blocks_produced': blocks_produced
            })
    
    return pd.DataFrame(results)

def create_visualizations(df, epoch_range, output_dir):
    os.makedirs(output_dir, exist_ok=True)
    
    # 1. Average Slot Duration Over Epochs
    plt.figure(figsize=(15, 7))
    for epoch in df['epoch'].unique():
        epoch_data = df[df['epoch'] == epoch]
        plt.plot(epoch_data['interval'], epoch_data['avg_slot_duration'], label=f'Epoch {epoch}')
    plt.title(f'Average Slot Duration Over Epochs ({epoch_range})')
    plt.xlabel('Interval')
    plt.ylabel('Average Slot Duration (seconds)')
    plt.legend(bbox_to_anchor=(1.05, 1), loc='upper left')
    plt.tight_layout()
    add_banner_to_figure(plt.gcf())
    plt.savefig(f'{output_dir}/avg_slot_duration_{epoch_range}.png')
    plt.close()

    # 2. Blocks Produced per Interval Across Epochs
    plt.figure(figsize=(15, 7))
    for epoch in df['epoch'].unique():
        epoch_data = df[df['epoch'] == epoch]
        plt.plot(epoch_data['interval'], epoch_data['blocks_produced'], label=f'Epoch {epoch}')
    plt.title(f'Blocks Produced per Interval Across Epochs ({epoch_range})')
    plt.xlabel('Interval')
    plt.ylabel('Blocks Produced')
    plt.legend(bbox_to_anchor=(1.05, 1), loc='upper left')
    plt.tight_layout()
    add_banner_to_figure(plt.gcf())
    plt.savefig(f'{output_dir}/blocks_produced_{epoch_range}.png')
    plt.close()

    # 3. Heatmap of Average Slot Duration
    pivot_df = df.pivot(index='epoch', columns='interval', values='avg_slot_duration')
    plt.figure(figsize=(20, 10))
    sns.heatmap(pivot_df, cmap='YlOrRd', cbar_kws={'label': 'Average Slot Duration (seconds)'})
    plt.title(f'Heatmap of Average Slot Duration ({epoch_range})')
    plt.xlabel('Interval')
    plt.ylabel('Epoch')
    plt.tight_layout()
    add_banner_to_figure(plt.gcf())
    plt.savefig(f'{output_dir}/heatmap_avg_slot_duration_{epoch_range}.png')
    plt.close()

    # 4. Interval Total Duration Across Epochs
    plt.figure(figsize=(15, 7))
    for epoch in df['epoch'].unique():
        epoch_data = df[df['epoch'] == epoch]
        plt.plot(epoch_data['interval'], epoch_data['interval_total_duration'], label=f'Epoch {epoch}')
    plt.title(f'Interval Total Duration Across Epochs ({epoch_range})')
    plt.xlabel('Interval')
    plt.ylabel('Interval Total Duration (seconds)')
    plt.legend(bbox_to_anchor=(1.05, 1), loc='upper left')
    plt.tight_layout()
    add_banner_to_figure(plt.gcf())
    plt.savefig(f'{output_dir}/interval_total_duration_{epoch_range}.png')
    plt.close()

def create_combined_visualizations(dataframes, epoch_ranges):
    chart_types = ['avg_slot_duration', 'blocks_produced', 'heatmap_avg_slot_duration', 'interval_total_duration']
    
    for chart_type in chart_types:
        fig, axes = plt.subplots(len(dataframes), 1, figsize=(20, 8 * len(dataframes)), squeeze=False)
        fig.suptitle(f'Combined {chart_type.replace("_", " ").title()} Charts', fontsize=24, y=0.98)
        
        vmin, vmax = float('inf'), float('-inf')
        for df in dataframes:
            if chart_type != 'heatmap_avg_slot_duration':
                vmin = min(vmin, df[chart_type].min())
                vmax = max(vmax, df[chart_type].max())

        for i, (df, epoch_range) in enumerate(zip(dataframes, epoch_ranges)):
            ax = axes[i, 0]
            
            if chart_type == 'heatmap_avg_slot_duration':
                pivot_df = df.pivot(index='epoch', columns='interval', values='avg_slot_duration')
                sns.heatmap(pivot_df, cmap='YlOrRd', cbar_kws={'label': 'Average Slot Duration (ms)'}, ax=ax)
                ax.set_title(f'Heatmap of Average Slot Duration ({epoch_range})', fontsize=16, pad=20)
                ax.set_xlabel('Relative Slot Number')
                
                # Adjust x-axis ticks
                ax.xaxis.set_major_locator(ticker.MultipleLocator(10))
                ax.xaxis.set_major_formatter(ticker.FuncFormatter(lambda x, p: f'{int(x*1000):,}'))
            else:
                for epoch in df['epoch'].unique():
                    epoch_data = df[df['epoch'] == epoch]
                    relative_slot = (epoch_data['interval'] - 1) * 1000
                    ax.plot(relative_slot, epoch_data[chart_type], label=f'Epoch {epoch}')
                ax.set_title(f'{chart_type.replace("_", " ").title()} ({epoch_range})', fontsize=16, pad=20)
                ax.legend(bbox_to_anchor=(1.05, 1), loc='upper left')
                ax.set_xlabel('Relative Slot Number')
                
                # Adjust x-axis ticks
                ax.xaxis.set_major_formatter(ticker.FuncFormatter(lambda x, p: f'{int(x):,}'))
                
            if chart_type == 'avg_slot_duration':
                ax.set_ylabel('Average Slot Duration (ms)')
                ax.yaxis.set_major_formatter(ticker.FuncFormatter(lambda x, p: f'{int(x*1000)}'))
            elif chart_type == 'blocks_produced':
                ax.set_ylabel('Blocks Produced')
                ax.set_ylim(700, 1100)  # Set y-axis scale from 700 to 1100
            elif chart_type == 'interval_total_duration':
                ax.set_ylabel('Interval Total Duration (ms)')
                ax.yaxis.set_major_formatter(ticker.FuncFormatter(lambda x, p: f'{int(x/1000)}'))

            ax.tick_params(axis='x', rotation=45)

        plt.tight_layout()
        fig.subplots_adjust(top=0.95, bottom=0.10, hspace=0.4)  # Adjust top, bottom margins and space between subplots
        add_banner_to_figure(fig)
        plt.savefig(f'combined_{chart_type}_chart.png', bbox_inches='tight', dpi=300)
        plt.close()

def get_epoch_range(conn):
    with conn.cursor() as cur:
        cur.execute("SELECT MIN(epoch), MAX(epoch) FROM validator_data")
        min_epoch, max_epoch = cur.fetchone()
    return min_epoch, max_epoch

def main():
    conn = psycopg2.connect(**db_params)
    
    try:
        # Get the minimum and maximum epoch
        min_epoch, max_epoch = get_epoch_range(conn)
        print(f"Epoch range in database: {min_epoch} to {max_epoch}")
        
        # Calculate the number of full 10-epoch sets, starting from the max_epoch
        num_sets = (max_epoch - min_epoch + 1) // 10
        
        all_dataframes = []
        all_epoch_ranges = []
        
        for i in range(num_sets):
            end_epoch = max_epoch - (i * 10)
            start_epoch = max(min_epoch, end_epoch - 9)  # Ensure we don't go below min_epoch
            
            print(f"Processing epochs {start_epoch} to {end_epoch}...")
            
            df = get_slot_data(conn, start_epoch, end_epoch)
            results = calculate_avg_slot_duration(df)
            
            epoch_range = f"{start_epoch}_to_{end_epoch}"
            csv_filename = f'avg_slot_duration_epochs_{epoch_range}.csv'
            results.to_csv(csv_filename, index=False)
            print(f"CSV file saved: {csv_filename}")
            
            # Debug information
            print(f"DataFrame shape: {results.shape}")
            print(f"DataFrame columns: {results.columns.tolist()}")
            print(f"First few rows of the DataFrame:")
            print(results.head())
            
            output_dir = f'visualizations_{epoch_range}'
            create_visualizations(results, epoch_range, output_dir)
            print(f"Visualizations saved in: {output_dir}")
            
            all_dataframes.append(results)
            all_epoch_ranges.append(epoch_range)
            
            # Break if we've reached or gone past the minimum epoch
            if start_epoch == min_epoch:
                break
        
        # Create combined visualizations
        if all_dataframes:
            create_combined_visualizations(all_dataframes, all_epoch_ranges)
            print("Combined visualizations created.")
        else:
            print("No data to create combined visualizations.")
        
    except Exception as e:
        print(f"An error occurred: {str(e)}")
        import traceback
        traceback.print_exc()
    finally:
        conn.close()

if __name__ == "__main__":
    main()