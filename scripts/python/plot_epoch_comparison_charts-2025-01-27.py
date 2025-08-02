def plot_epoch_comparison_charts(start_epoch, end_epoch, output_file="epoch_comparison_charts.png"):
    """
    Queries the validator_stats table to gather data for each epoch within a specified range
    and generate charts for avg_mev_per_block, avg_priority_fees_per_block, avg_signature_fees_per_block,
    avg_user_tx_per_block, avg_vote_tx_per_block, and avg_cu_per_block.

    If start_epoch equals end_epoch, the function retrieves only the latest 10 epochs up to end_epoch.
    Otherwise, it retrieves all epochs in the specified range.

    The resulting plots are saved in a single PNG file.
    """

    # Set up logging
    logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')

    # Conversion factor from lamports to SOL
    LAMPORTS_PER_SOL = 1_000_000_000

    # Load the cluster average values from the JSON file
    with open('last_ten_epoch_aggregate_data.json', 'r') as json_file:
        cluster_average_data = json.load(json_file)

    # Extract cluster average values for charts
    cluster_avg_mev_priority_signature = [
        (item['avg_mev_per_block'] + item['avg_priority_fees_per_block'] + item['avg_signature_fees_per_block'])
        for item in cluster_average_data
    ]
    cluster_avg_tx_per_block = [item['avg_tx_per_block'] for item in cluster_average_data]
    cluster_avg_cu_per_block = [item['avg_cu_per_block'] for item in cluster_average_data]

    # Log the cluster average values
    logging.debug(f"Cluster Avg MEV + Priority + Signature Fees per Block: {cluster_avg_mev_priority_signature}")
    logging.debug(f"Cluster Avg TX per Block: {cluster_avg_tx_per_block}")
    logging.debug(f"Cluster Avg CU per Block: {cluster_avg_cu_per_block}")

    # Define the items to get unique colors for each version of each metric (only for versions 1 and 2)
    metrics = ['avg_mev_per_block', 'avg_priority_fees_per_block', 'avg_signature_fees_per_block',
               'avg_user_tx_per_block', 'avg_vote_tx_per_block', 'avg_cu_per_block']
    items = [f"{metric}_v{version}" for metric in metrics for version in ['0', '1', '2']]
    color_map = get_color_map(items)
    color_map['avg_signature_fees_per_block_v0'] = 'orange'
    color_map['avg_signature_fees_per_block_v1'] = 'green'

    # Determine the epoch range to query
    if start_epoch == end_epoch:
        epoch_query = f"SELECT DISTINCT epoch FROM validator_stats WHERE epoch <= {end_epoch} ORDER BY epoch DESC LIMIT 10"
        conn = psycopg2.connect(**db_params)
        recent_epochs = pd.read_sql(epoch_query, conn)
        conn.close()
        epoch_range = recent_epochs['epoch'].tolist()
    else:
        epoch_range = list(range(start_epoch, end_epoch + 1))

    # Step 1: Connect to the database and query the data
    conn = psycopg2.connect(**db_params)
    query = f"""
        SELECT epoch, version, avg_mev_per_block, avg_priority_fees_per_block, avg_signature_fees_per_block,
               avg_cu_per_block, avg_user_tx_per_block, avg_vote_tx_per_block
        FROM validator_stats
        WHERE epoch IN %s
    """
    data = pd.read_sql(query, conn, params=(tuple(epoch_range),))
    conn.close()

    # Step 2: Process data
    data['version_digit'] = data['version'].apply(lambda x: re.match(r'[0-2]', str(x)).group() if re.match(r'[0-2]', str(x)) else None)
    data = data.dropna(subset=['version_digit'])  # Drop rows where version_digit is None

    # Calculate sample size for versions for each epoch
    sample_size = data[data['version_digit'].isin(['0', '1', '2'])].groupby(['epoch', 'version_digit']).size().unstack(fill_value=0)

    # Print sample size for each epoch for versions 1 and 2 in the specified format
    print("The sample sizes by epoch are (Epoch, V0, V1, V2):")
    for epoch in sample_size.index:
        print(f"{epoch}, {sample_size.loc[epoch, '0']}, {sample_size.loc[epoch, '1']}, {sample_size.loc[epoch, '2']}")

    # Step 3: Set up subplots for combined output with reduced spacing
    fig, axs = plt.subplots(3, 1, figsize=(12, 16))
    fig.suptitle("Solana Validator Client Version Comparison", y=0.96, fontweight='bold')

    # Define bar width and offsets for each version
    bar_width = 0.25
    version_offsets = {'0': -bar_width, '1': 0, '2': bar_width}

    # Chart 1: avg_mev_per_block, avg_priority_fees_per_block, and avg_signature_fees_per_block
    chart1_data = data.dropna(subset=['avg_mev_per_block', 'avg_priority_fees_per_block', 'avg_signature_fees_per_block'])
    grouped_data1 = chart1_data.groupby(['epoch', 'version_digit'])[['avg_mev_per_block', 'avg_priority_fees_per_block', 'avg_signature_fees_per_block']].mean().reset_index()

    # Plot the bar charts for each version
    for version_digit, offset in version_offsets.items():
        subset = grouped_data1[grouped_data1['version_digit'] == version_digit]
        if not subset.empty:
            axs[0].bar(
                subset['epoch'] + offset,
                (subset['avg_mev_per_block'] / LAMPORTS_PER_SOL).round(7),
                width=bar_width,
                label=f"MEV (v{version_digit})",
                color=color_map[f'avg_mev_per_block_v{version_digit}']
            )
            axs[0].bar(
                subset['epoch'] + offset,
                (subset['avg_priority_fees_per_block'] / LAMPORTS_PER_SOL).round(7),
                width=bar_width,
                label=f"Priority (v{version_digit})",
                bottom=(subset['avg_mev_per_block'] / LAMPORTS_PER_SOL).round(7),
                color=color_map[f'avg_priority_fees_per_block_v{version_digit}']
            )
            axs[0].bar(
                subset['epoch'] + offset,
                (subset['avg_signature_fees_per_block'] / LAMPORTS_PER_SOL).round(7),
                width=bar_width,
                label=f"Signature (v{version_digit})",
                bottom=((subset['avg_mev_per_block'] + subset['avg_priority_fees_per_block']) / LAMPORTS_PER_SOL).round(7),
                color=color_map[f'avg_signature_fees_per_block_v{version_digit}']
            )

    # Plot the cluster average line across all versions for the combined value
    #if cluster_avg_mev_priority_signature:
        # axs[0].plot(
        #     epoch_range,
        #     cluster_avg_mev_priority_signature,
        #     label="Cluster Average",
        #     color='blue',
        #     linestyle='--',
        #     marker='o'
        # )

    axs[0].set_title("Avg MEV, Priority, and Signature Fees per Block by Epoch (in SOL)")
    axs[0].set_xlabel("Epoch")
    axs[0].set_ylabel("Values (SOL)")
    axs[0].legend(loc='upper left')

    # Chart 2: avg_user_tx_per_block and avg_vote_tx_per_block
    chart2_data = data.dropna(subset=['avg_user_tx_per_block', 'avg_vote_tx_per_block'])
    grouped_data2 = chart2_data.groupby(['epoch', 'version_digit'])[['avg_user_tx_per_block', 'avg_vote_tx_per_block']].mean().reset_index()

    # Plot the bar charts for each version
    for version_digit, offset in version_offsets.items():
        subset = grouped_data2[grouped_data2['version_digit'] == version_digit]
        axs[1].bar(
            subset['epoch'] + offset,
            subset['avg_user_tx_per_block'],
            width=bar_width,
            label=f"User TX (v{version_digit})",
            color=color_map[f'avg_user_tx_per_block_v{version_digit}']
        )
        axs[1].bar(
            subset['epoch'] + offset,
            subset['avg_vote_tx_per_block'],
            width=bar_width,
            label=f"Vote TX (v{version_digit})",
            bottom=subset['avg_user_tx_per_block'],
            color=color_map[f'avg_vote_tx_per_block_v{version_digit}']
        )

    # Plot the cluster average line across all versions for avg_tx_per_block
    # axs[1].plot(
    #     epoch_range,
    #     cluster_avg_tx_per_block,
    #     label="Cluster Average",
    #     color='blue',
    #     linestyle='--',
    #     marker='o'
    # )


    axs[1].set_title("Avg User and Vote TX per Block by Epoch")
    axs[1].set_xlabel("Epoch")
    axs[1].set_ylabel("Values")
    axs[1].legend(loc='upper left')

    # Chart 3: avg_cu_per_block
    chart3_data = data.dropna(subset=['avg_cu_per_block'])
    grouped_data3 = chart3_data.groupby(['epoch', 'version_digit'])[['avg_cu_per_block']].mean().reset_index()

    for version_digit, offset in version_offsets.items():
        subset = grouped_data3[grouped_data3['version_digit'] == version_digit]
        axs[2].bar(
            subset['epoch'] + offset,
            subset['avg_cu_per_block'],
            width=bar_width,
            label=f"CU (v{version_digit})",
            color=color_map[f'avg_cu_per_block_v{version_digit}']
        )

    # Plot the cluster average line for avg_cu_per_block
    # axs[2].plot(
    #     epoch_range,
    #     cluster_avg_cu_per_block,
    #     label="Cluster Average",
    #     color='blue',
    #     linestyle='--',
    #     marker='o'
    # )

    axs[2].set_title("Avg Compute Units per Block by Epoch")
    axs[2].set_xlabel("Epoch")
    axs[2].set_ylabel("Values")
    axs[2].legend(loc='upper left')

    # Adding the banner using the add_banner_to_figure function with scale_factor=0.02
    add_banner_to_figure(fig, scale_factor=0.02, banner_path="/var/www/html/images/favicon.ico", banner_position=(0.5, 0.02))

    # Save the plot as a PNG file
    plt.tight_layout(rect=[0, 0.03, 1, 0.92])  # Adjust layout to fit the banner at the bottom
    plt.savefig(output_file)
    plt.close(fig)

    # Print confirmation of saved file
    print(f"The filename is saved as {output_file}")