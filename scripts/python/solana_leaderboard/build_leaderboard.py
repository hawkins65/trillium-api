import sys
import os
from sqlalchemy import create_engine

# Import from parent directory using absolute imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from db_config import db_params
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import importlib
logging_config = importlib.import_module('999_logging_config')
setup_logging = logging_config.setup_logging

# Import from current package - add current directory to path for absolute imports
current_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, current_dir)

from db_operations import get_validator_stats, get_epoch_aggregate_data, write_validator_stats_to_json, write_epoch_aggregate_data_to_json, get_min_max_epochs
from epoch_aggregation import generate_last_ten_epochs_data, generate_ten_epoch_validator_rewards, generate_ten_epoch_aggregate_data, generate_weighted_average_validator_rewards
from stake_statistics import calculate_stake_statistics, calculate_stake_statistics_metro
from visualizations import plot_votes_cast_metrics, plot_latency_and_consensus_charts, plot_epoch_comparison_charts, plot_epoch_metrics_with_stake_colors
from utils import get_output_path

# Initialize logger
logger = setup_logging('build_leaderboard')

def main(start_epoch=None, end_epoch=None):
    engine = create_engine(
        f"postgresql+psycopg2://{db_params['user']}@{db_params['host']}:{db_params['port']}/{db_params['database']}?sslmode={db_params['sslmode']}"
    )
    
    min_epoch, max_epoch = get_min_max_epochs(engine)
    DEBUG = True
    if DEBUG:
        print("\nAvailable epoch range:")
        print(f"Minimum epoch: {min_epoch}")
        print(f"Maximum epoch: {max_epoch}")
        print()

    if start_epoch is not None and end_epoch is not None:
        if not (min_epoch <= start_epoch <= end_epoch <= max_epoch):
            print(f"Error: Epochs {start_epoch} and {end_epoch} must be between {min_epoch} and {max_epoch}, with start <= end.")
            sys.exit(1)
    else:
        if end_epoch is None:
            while True:
                end_epoch_input = input(f"Enter end epoch (default is {max_epoch}, press Enter for default): ").strip()
                if end_epoch_input == "":
                    end_epoch = max_epoch
                    break
                try:
                    end_epoch = int(end_epoch_input)
                    if min_epoch <= end_epoch <= max_epoch:
                        break
                    else:
                        print(f"Please enter a value between {min_epoch} and {max_epoch}.")
                except ValueError:
                    print("Please enter a valid integer.")

        default_start_epoch = end_epoch if start_epoch is None else start_epoch
        if start_epoch is None:
            while True:
                start_epoch_input = input(f"Enter start epoch (default is {default_start_epoch}, press Enter for default): ").strip()
                if start_epoch_input == "":
                    start_epoch = default_start_epoch
                    break
                try:
                    start_epoch = int(start_epoch_input)
                    if min_epoch <= start_epoch <= end_epoch:
                        break
                    else:
                        print(f"Please enter a value between {min_epoch} and {end_epoch}.")
                except ValueError:
                    print("Please enter a valid integer.")

    epochs = range(start_epoch, end_epoch + 1)
    missing_data_epochs = []

    for epoch in epochs:
        print(f"Processing epoch: {epoch}")
        try:
            logger.info(f"get_validator_stats for epoch {epoch}")
            validator_stats = get_validator_stats(epoch, engine, debug=DEBUG)
            logger.info(f"write_validator_stats_to_json for epoch {epoch}")
            write_validator_stats_to_json(epoch, validator_stats)
            epoch_aggregate_data = get_epoch_aggregate_data(epoch, engine)
            if epoch_aggregate_data is not None:
                logger.info(f"write_epoch_aggregate_data_to_json for epoch {epoch}")
                write_epoch_aggregate_data_to_json(epoch, epoch_aggregate_data)
            else:
                logger.warning(f"Skipping epoch_aggregate_data for epoch {epoch} due to missing data")
                missing_data_epochs.append(epoch)

            logger.info(f"calculate_stake_statistics for epoch {epoch}")
            country_df, continent_df, region_df = calculate_stake_statistics(epoch, max_epoch, engine)
            
            logger.info(f"country_df epoch{epoch}_country_stats.csv")
            country_df.to_csv(get_output_path(f'epoch{epoch}_country_stats.csv', 'csv'), index=True)
            logger.info(f"continent_df epoch{epoch}_continent_stats.csv")
            continent_df.to_csv(get_output_path(f'epoch{epoch}_continent_stats.csv', 'csv'), index=True)
            logger.info(f"region_df epoch{epoch}_region_stats.csv")
            region_df.to_csv(get_output_path(f'epoch{epoch}_region_stats.csv', 'csv'), index=True)

            logger.info(f"calculate_stake_statistics_metro for epoch {epoch}")
            country_df_metro, metro_df = calculate_stake_statistics_metro(epoch, max_epoch, engine)
            logger.info(f"country_df epoch{epoch}_country_stats_metro.csv")
            country_df_metro.to_csv(get_output_path(f'epoch{epoch}_country_stats_metro.csv', 'csv'), index=True)
            logger.info(f"metro_df epoch{epoch}_metro_stats_metro.csv")
            metro_df.to_csv(get_output_path(f'epoch{epoch}_metro_stats_metro.csv', 'csv'), index=True)

        except Exception as e:
            logger.error(f"Failed to process epoch {epoch}: {str(e)}")

    if max_epoch not in epochs:
        try:
            print(f"Generating stake distribution chart for max epoch: {max_epoch}")
            country_df, continent_df, region_df = calculate_stake_statistics(max_epoch, max_epoch, engine)
            country_df_metro, metro_df = calculate_stake_statistics_metro(max_epoch, max_epoch, engine)
            print(f"Stake distribution chart for max epoch {max_epoch} has been generated.")
        except Exception as e:
            logger.error(f"Failed to generate stake distribution chart for max epoch {max_epoch}: {str(e)}")

    if missing_data_epochs:
        logger.warning(f"Missing epoch_aggregate_data for epochs: {missing_data_epochs}")

    generate_last_ten_epochs_data(end_epoch, engine)
    generate_ten_epoch_validator_rewards(end_epoch, engine)
    generate_ten_epoch_aggregate_data(end_epoch, engine)
    generate_weighted_average_validator_rewards(end_epoch, engine)

    plot_epoch_metrics_with_stake_colors(end_epoch, max_epoch)
    plot_epoch_comparison_charts(start_epoch, end_epoch, max_epoch, engine)
    plot_latency_and_consensus_charts(start_epoch, end_epoch, max_epoch, engine)
    plot_votes_cast_metrics(end_epoch, max_epoch)

    print("Processing complete.")

if __name__ == '__main__':
    if len(sys.argv) > 2:
        try:
            start_epoch = int(sys.argv[1])
            end_epoch = int(sys.argv[2])
            main(start_epoch, end_epoch)
        except ValueError:
            print("Error: Please provide valid integer epochs as command-line arguments (start_epoch end_epoch).")
            main()
    elif len(sys.argv) == 2:
        try:
            end_epoch = int(sys.argv[1])
            main(end_epoch=end_epoch)
        except ValueError:
            print("Error: Please provide a valid integer epoch as a command-line argument.")
            main()
    else:
        main()