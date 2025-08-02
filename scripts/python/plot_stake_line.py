import pandas as pd
import matplotlib.pyplot as plt

# File paths to your CSVs
stake_buckets_file = '/home/smilax/api/stake_buckets_refined.csv'
total_stake_file = '/home/smilax/api/total_active_stake.csv'

# Load the data
df_buckets = pd.read_csv(stake_buckets_file)
df_total_stake = pd.read_csv(total_stake_file)

# Pivot the stake buckets data
pivot_df = df_buckets.pivot(index='epoch', columns='stake_bucket', values='total_percentage_in_bucket')
buckets = ['0-0.001%', '0.001-0.005%', '0.005-0.01%', '0.01-0.05%', '0.05-0.1%', '0.1-1%', '1%+']
for bucket in buckets:
    if bucket not in pivot_df.columns:
        pivot_df[bucket] = 0
pivot_df = pivot_df[buckets]

# Ensure total stake data is aligned by epoch and convert lamports to SOL
df_total_stake = df_total_stake[df_total_stake['epoch'].between(600, 747)].set_index('epoch')
df_total_stake['total_active_stake_sol'] = df_total_stake['total_active_stake'] / 1_000_000_000  # Convert lamports to SOL

# Apply smoothing with a rolling mean (window size = 5 epochs)
window_size = 5
smoothed_pivot_df = pivot_df.rolling(window=window_size, min_periods=1, center=True).mean()
smoothed_total_stake = df_total_stake['total_active_stake_sol'].rolling(window=window_size, min_periods=1, center=True).mean()

# Set up the plot with two y-axes
fig, ax1 = plt.subplots(figsize=(14, 8))

# Plot smoothed stake bucket percentages on the primary y-axis (left)
for column in smoothed_pivot_df.columns:
    ax1.plot(smoothed_pivot_df.index, smoothed_pivot_df[column], label=column, linewidth=2)

# Customize primary y-axis (percentage)
ax1.set_title(f'Smoothed Stake Distribution and Total Active Stake (Epochs 600-747, {window_size}-Epoch Moving Average)', fontsize=16, pad=20)
ax1.set_xlabel('Epoch', fontsize=12)
ax1.set_ylabel('Percentage of Total Stake (%)', fontsize=12, color='tab:blue')
ax1.set_ylim(0, 50)  # Primary y-axis: 0-50%
ax1.tick_params(axis='y', labelcolor='tab:blue')
ax1.grid(True, linestyle='--', alpha=0.5)

# Create secondary y-axis for smoothed total active stake in SOL (right)
ax2 = ax1.twinx()
ax2.plot(smoothed_total_stake.index, smoothed_total_stake, label='Total Active Stake', color='black', linewidth=2, linestyle='--')

# Customize secondary y-axis (total stake in SOL)
ax2.set_ylabel('Total Active Stake (SOL)', fontsize=12, color='black')
ax2.tick_params(axis='y', labelcolor='black')

# Adjust x-axis ticks
plt.xticks(range(600, 751, 20), rotation=45)

# Combine legends from both axes
lines1, labels1 = ax1.get_legend_handles_labels()
lines2, labels2 = ax2.get_legend_handles_labels()
ax1.legend(lines1 + lines2, labels1 + labels2, title='Stake Buckets & Total', loc='upper left', bbox_to_anchor=(1.05, 1), fontsize=10)

# Adjust layout
plt.tight_layout()

# Save the plot
plt.savefig('/home/smilax/api/smoothed_stake_distribution_with_total_sol_line_plot.png', dpi=300, bbox_inches='tight')

# Display the plot
plt.show()