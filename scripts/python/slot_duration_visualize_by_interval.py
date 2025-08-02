import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import glob
import os

def create_visualizations(csv_file):
    # Read the CSV file
    df = pd.read_csv(csv_file)
    
    # Extract epoch range from filename
    filename = os.path.basename(csv_file)
    epoch_range = filename.split('epochs_')[1].split('.csv')[0]
    
    # Create a directory for the outputs
    output_dir = f'visualizations_{epoch_range}'
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
    plt.savefig(f'{output_dir}/avg_slot_duration_{epoch_range}.png')
    plt.close()

    # 2. Blocks Produced per Interval Across Epochs
    plt.figure(figsize=(15, 7))
    for epoch in df['epoch'].unique():
        epoch_data = df[df['epoch'] == epoch]
        plt.plot(epoch_data['interval'], epoch_data['interval_blocks_produced'], label=f'Epoch {epoch}')
    plt.title(f'Blocks Produced per Interval Across Epochs ({epoch_range})')
    plt.xlabel('Interval')
    plt.ylabel('Blocks Produced')
    plt.legend(bbox_to_anchor=(1.05, 1), loc='upper left')
    plt.tight_layout()
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
    plt.savefig(f'{output_dir}/interval_total_duration_{epoch_range}.png')
    plt.close()

    print(f"Visualizations for {epoch_range} saved in {output_dir}")

# Process all CSV files
for csv_file in glob.glob('avg_slot_duration_epochs_*.csv'):
    create_visualizations(csv_file)