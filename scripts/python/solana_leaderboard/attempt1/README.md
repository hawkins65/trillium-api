# apiv2 Project Documentation

This project (`apiv2`) is a collection of scripts designed to process and manage data related to Solana block production, validator information, and various analytics. The workflow is primarily driven by shell scripts that orchestrate Python scripts, Node.js scripts, and SQL updates.

## Workflow Overview

The core workflow is initiated by `00_process_all_automated.sh`, which acts as the main entry point. The process generally follows these steps:

1.  **Data Collection & Preparation:**
    *   `00_process_all_automated.sh` sets up the environment, creates epoch-specific directories, and copies necessary data collection scripts into them.
    *   It then calls `0_process_getslots_data.sh` to begin the main data processing for a given epoch.
    *   `0_process_getslots_data.sh` further orchestrates the execution of scripts like `90_xshin_load_data.sh` (for Xshin data), `90_stakewiz_validators.py` (for Stakewiz validator data), `90_get_block_data.sh` (for block data), and `90_untar_epoch.sh` (for extracting epoch data).
    *   Initial data loading and consolidation are handled by `1_load_consolidated_csv.sh` and `91_load_consolidated_csv.py`.

2.  **Jito MEV Data Processing (Conditional):**
    *   The workflow includes two paths for Jito MEV data processing: `1_no-wait-for-jito-process_data.sh` and `1_wait-for-jito-process_data.sh`.
    *   `1_no-wait-for-jito-process_data.sh` proceeds without waiting for Jito MEV data if it's not immediately available, performing initial updates and data generation.
    *   `1_wait-for-jito-process_data.sh` waits for Jito MEV data (`90_wait-for-jito-kobe-epoch-data.sh`) and then performs more comprehensive updates, including Jito steward data collection (`92-jito-steward-data-collection.py`).

3.  **Validator Aggregate Information & Leaderboard Generation:**
    *   `2_update_validator_aggregate_info.sh` is a critical script that updates various aggregate information for validators. This involves running SQL updates (`92_run_sql_updates.sh`) and Python scripts for inflation rewards and APY calculations (`92_update_validator_aggregate_info.py`, `92_update_ead_inflation_reward.py`, `92_update_vs_inflation_reward.py`, `92_calculate_apy.py`).
    *   `3_build_leaderboard_json.sh` generates leaderboard JSON files, utilizing Python scripts like `93_build_leaderboard_json.py`, `93_build_leaderboard_json-jito-by_count.sh`, `93_vote_latency_json.py`, and `93_chart_votes_cast.py`.

4.  **Data Movement and Cleanup:**
    *   `4_move_json_to_production.sh` and `5_cp_images.sh` are responsible for moving generated JSON data and images to production-ready locations, often involving cache purging (`cloudflare-purge-cache.sh`).
    *   `7_cleanup.sh` performs cleanup tasks after processing.

5.  **Notifications:**
    *   Discord notifications are sent at various stages using scripts like `61_update_discord_channel_trillium-api-data.sh` and `6_update_discord_channel_trillium-api-data.sh`, which in turn call `create_trillium_alert.sh`.

## Input and Output Files

This section details the input and output files for each script and external program within the `apiv2` project.

### Script and Program I/O

*   **`00_process_all_automated.sh`**
    *   **Inputs:**
        *   None (takes epoch number as argument or user input).
    *   **Outputs:**
        *   `$HOME/log/get_slots/${script_name%.*}.log`: Log file for script execution.
        *   `/home/smilax/block-production/get_slots/epoch$epoch_number/`: Directory created for epoch-specific data.
    *   **External Programs/Commands:**
        *   `mkdir -p`: Creates directories.
        *   `cp`: Copies files to the epoch directory.
            *   Copies: `get_epoch_data_csv.py`, `slot_data.py`, `vote_data.py`, `vote_latency.py`, `get_shin_voting.sh`, `rpc_get_block_data.sh`, `cleanup_rundir.sh`
            *   To: `/home/smilax/block-production/get_slots/epoch$epoch_number/`
        *   `bash /home/smilax/block-production/api/0_process_getslots_data.sh`: Executes the next script in the workflow.

*   **`0_process_getslots_data.sh`**
    *   **Inputs:**
        *   None (takes epoch number as argument or user input).
    *   **Outputs:**
        *   `$HOME/log/${script_name%.*}.log`: Log file for script execution.
        *   `epoch${epoch_number}.tar.zst`: Copied to `/mnt/disk3/apiserver/epochs/`.
    *   **External Programs/Commands:**
        *   `mkdir -p`: Creates directories.
        *   `bash 90_xshin_load_data.sh`: Executes Xshin data loading.
        *   `python3 90_stakewiz_validators.py`: Executes Stakewiz validator data fetching.
        *   `bash 90_get_block_data.sh`: Executes block data retrieval.
        *   `bash 90_update_discord_channel_trillium-api-data.sh`: Sends Discord notifications.
        *   `bash 90_untar_epoch.sh`: Untars epoch data.
        *   `bash 1_load_consolidated_csv.sh`: Loads consolidated CSV data.
        *   `bash 1_no-wait-for-jito-process_data.sh`: Processes Jito data (no wait).
        *   `tmux new-session -d -s "jito_process_${epoch_number}" "bash 1_wait-for-jito-process_data.sh $epoch_number"`: Starts Jito data processing (with wait) in a tmux session.
        *   `cp`: Copies `epoch${epoch_number}.tar.zst` to `/mnt/disk3/apiserver/epochs/`.

*   **`90_xshin_load_data.sh`**
    *   **Inputs:**
        *   Epoch number (argument or user input).
    *   **Outputs:**
        *   `90_xshin_all_validators_${epoch}.json`: JSON file containing all Xshin validator data for the epoch.
        *   `90_xshin_all_award_winners_${epoch}.json`: JSON file containing all Xshin award winners for the epoch.
    *   **External Programs/Commands:**
        *   `node 90_xshin.js`: Executes the Node.js script to generate raw JSON data.
        *   `mv`: Renames the generated JSON files.
        *   `python3 90_xshin_load_data.py`: Processes the JSON data and loads it into the database.

*   **`90_stakewiz_validators.py`**
    *   **Inputs:**
        *   `db_config.py`: Database connection parameters.
        *   Stakewiz API (`https://api.stakewiz.com/validators`): Fetches validator data.
    *   **Outputs:**
        *   **PostgreSQL Database:** Inserts/updates records in the `stakewiz_validators` table.
    *   **External Programs/Libraries:**
        *   `psycopg2`: Python library for PostgreSQL interaction.
        *   `requests`: Python library for making HTTP requests to the Stakewiz API.
        *   `uuid`: Python standard library for generating UUIDs.
        *   `datetime`, `pytz`: Python standard libraries for date/time handling.

*   **`90_get_block_data.sh`**
    *   **Inputs:**
        *   Epoch number (argument).
    *   **Outputs:**
        *   Log messages to stdout.
    *   **External Programs/Commands:**
        *   `bash ./rpc_get_block_data.sh`: Executes RPC calls to get block data.
        *   `bash tar_files.sh`: Archives processed files.
        *   `bash copy_tar.sh`: Copies tar archives.

*   **`90_update_discord_channel_trillium-api-data.sh`**
    *   **Inputs:**
        *   Epoch number (argument or user input).
    *   **Outputs:**
        *   Sends messages to a Discord webhook.
    *   **External Programs/Commands:**
        *   `curl`: Used to send POST requests to the Discord webhook URL.
        *   `date`: Used to get current date/time for message content.
        *   `sed`: Used to escape JSON special characters.

*   **`90_untar_epoch.sh`**
    *   **Inputs:**
        *   Epoch number (argument or user input).
        *   `epoch${epoch_number}*.tar.zst`: Compressed tarball containing epoch data.
    *   **Outputs:**
        *   Extracted files from the tarball into the current directory.
    *   **External Programs/Commands:**
        *   `ls`: Lists files to find the tarball.
        *   `zstd`: Decompresses the `.zst` file.
        *   `tar`: Extracts files from the tar archive.

*   **`1_load_consolidated_csv.sh`**
    *   **Inputs:**
        *   Epoch number (argument).
    *   **Outputs:**
        *   None (orchestrates Python script).
    *   **External Programs/Commands:**
        *   `python3 91_load_consolidated_csv.py`: Executes the Python script to load CSV data into the database.

*   **`1_no-wait-for-jito-process_data.sh`**
    *   **Inputs:**
        *   Epoch number (argument or user input).
        *   Jito API (`https://kobe.mainnet.jito.network/api/v1/validators/J1to1yufRnoWn81KYg1XkTWzmKjnYSnmE2VY8DGUJ9Qv`): Checks for existing MEV data.
    *   **Outputs:**
        *   `$HOME/log/${script_name%.*}.log`: Log file for script execution.
        *   `$HOME/log/1_no_wait_full_process_${epoch_number}.flag`: Flag file indicating full processing occurred.
    *   **External Programs/Commands:**
        *   `curl`: Fetches data from Jito API.
        *   `jq`: Parses JSON response from Jito API.
        *   `bash 2_update_validator_aggregate_info.sh`: Updates validator aggregate info.
        *   `bash 3_build_leaderboard_json.sh`: Builds leaderboard JSON.
        *   `python3 93_solana_stakes_export.py`: Exports Solana stakes data (commented out in script).
        *   `python3 93_skip_analysis.py`: Performs skip analysis.
        *   `bash 4_move_json_to_production.sh`: Moves JSON to production.
        *   `bash 5_cp_images.sh`: Copies images.
        *   `bash 61_update_discord_channel_trillium-api-data.sh`: Sends Discord notifications.
        *   `bash 7_cleanup.sh`: Cleans up.

*   **`1_wait-for-jito-process_data.sh`**
    *   **Inputs:**
        *   Epoch number (argument).
        *   `$HOME/log/1_no_wait_full_process_${epoch_number}.flag`: Checks for flag file to skip processing.
    *   **Outputs:**
        *   `$HOME/log/${script_name%.*}.log`: Log file for script execution.
    *   **External Programs/Commands:**
        *   `bash 90_wait-for-jito-kobe-epoch-data.sh`: Waits for Jito Kobe epoch data.
        *   `bash 2_update_validator_aggregate_info.sh`: Updates validator aggregate info.
        *   `python3 92-jito-steward-data-collection.py`: Collects Jito steward data.
        *   `bash 3_build_leaderboard_json.sh`: Builds leaderboard JSON.
        *   `python3 93_solana_stakes_export.py`: Exports Solana stakes data (commented out in script).
        *   `python3 93_skip_analysis.py`: Performs skip analysis.
        *   `bash 4_move_json_to_production.sh`: Moves JSON to production.
        *   `bash 5_cp_images.sh`: Copies images.
        *   `bash 6_update_discord_channel_trillium-api-data.sh`: Sends Discord notifications.
        *   `bash 7_cleanup.sh`: Cleans up.
        *   `bash copy_tar_gdrive_disk3.sh`: Copies tar to GDrive.
        *   `tmux kill-session`: Kills the tmux session.

*   **`rpc_get_block_data.sh`**
    *   **Inputs:**
        *   Epoch number (argument).
    *   **Outputs:**
        *   `slot_data_thread_*.csv`: CSV files containing slot data.
        *   `epoch_votes_thread_*.csv`: CSV files containing vote data.
        *   `solana_rpc_errors.log`: Log file for RPC errors.
        *   `last_slots_to_process.txt`: Records the number of slots processed in the last run.
    *   **External Programs/Commands:**
        *   `python3 get_epoch_data_csv.py`: Fetches and processes block data from RPC.

*   **`tar_files.sh`**
    *   **Inputs:**
        *   Epoch number (argument).
        *   `slot_data_thread_*.csv`: CSV files generated by `get_epoch_data_csv.py`.
        *   `epoch_votes_thread_*.csv`: CSV files generated by `get_epoch_data_csv.py`.
    *   **Outputs:**
        *   `epoch${epoch_number}.tar.zst`: Compressed tarball of processed CSV files.
    *   **External Programs/Commands:**
        *   `tar`: Creates a tar archive.
        *   `zstd`: Compresses the tar archive.
        *   `rm`: Removes original CSV files after archiving.

*   **`copy_tar.sh`**
    *   **Inputs:**
        *   Epoch number (argument).
        *   `epoch${epoch_number}.tar.zst`: Compressed tarball.
    *   **Outputs:**
        *   Copies `epoch${epoch_number}.tar.zst` to `/mnt/disk3/apiserver/epochs/`.
    *   **External Programs/Commands:**
        *   `cp`: Copies the tarball.

*   **`90_xshin.js`**
    *   **Inputs:**
        *   None (likely fetches data from an external API or local files).
    *   **Outputs:**
        *   `all_all_validators.json`: Raw JSON data for all validators.
        *   `all_award_winners.json`: Raw JSON data for award winners.
    *   **External Programs/Libraries:**
        *   Node.js runtime.
        *   Any Node.js modules used within the script (e.g., `axios` for HTTP requests, `fs` for file system operations).

*   **`90_xshin_load_data.py`**
    *   **Inputs:**
        *   Epoch number (argument or user input).
        *   `90_xshin_all_validators_${epoch}.json`: JSON file with validator data.
        *   `90_xshin_all_award_winners_${epoch}.json`: JSON file with award winner data.
        *   `db_config.py`: Database connection parameters.
    *   **Outputs:**
        *   **PostgreSQL Database:** Inserts/updates records in the `xshin_data` table.
        *   `$HOME/log/validator_scores.log`: Log file for script execution.
    *   **External Programs/Libraries:**
        *   `json`: Python standard library for JSON processing.
        *   `psycopg2`: Python library for PostgreSQL interaction.
        *   `logging`: Python standard library for logging.

*   **`db_config.py`**
    *   **Inputs:** None (contains hardcoded database parameters).
    *   **Outputs:** Provides `db_params` dictionary for other Python scripts.
    *   **External Programs/Libraries:** None.

*   **`91_load_consolidated_csv.py`**
    *   **Inputs:**
        *   Epoch number (argument or user input).
        *   `./epoch${epoch_number}/run*/epoch_votes_*.csv`: CSV files containing epoch vote data.
        *   `./epoch${epoch_number}/run*/slot_data_*.csv`: CSV files containing slot data.
        *   `db_config.py`: Database connection parameters.
    *   **Outputs:**
        *   **PostgreSQL Database:**
            *   Creates and populates `temp_epoch_votes` and `temp_validator_data` tables.
            *   Inserts/updates data into `validator_data` and `validator_stats` tables.
            *   Inserts/updates data into `validator_xshin` table from `run0/good.json` and `run0/poor.json`.
        *   `$HOME/log/${script_name}.log`: Log file for slot chain verification.
    *   **External Programs/Libraries:**
        *   `psycopg2`: Python library for PostgreSQL interaction.
        *   `glob`: Python standard library for finding files matching patterns.
        *   `csv`: Python standard library for CSV file handling.
        *   `json`: Python standard library for JSON processing.
        *   `re`: Python standard library for regular expressions.

*   **`2_update_validator_aggregate_info.sh`**
    *   **Inputs:**
        *   Epoch number (argument).
        *   Optional `--skip-previous` flag.
    *   **Outputs:**
        *   None (orchestrates Python script).
    *   **External Programs/Commands:**
        *   `python3 92_update_validator_aggregate_info.py`: Executes the Python script to update validator aggregate information.

*   **`3_build_leaderboard_json.sh`**
    *   **Inputs:**
        *   Epoch number (argument).
    *   **Outputs:**
        *   None (orchestrates Python scripts).
    *   **External Programs/Commands:**
        *   `python3 93_build_leaderboard_json.py`: Builds general leaderboard JSON.
        *   `python3 93_build_leaderboard_json-jito-by_count.sh`: Builds Jito-specific leaderboard JSON.
        *   `python3 93_vote_latency_json.py`: Generates vote latency JSON.
        *   `python3 93_chart_votes_cast.py`: Generates charts for votes cast.
        *   `python3 93_skip_blame.py`: Performs skip blame analysis.
        *   `python3 93_skip_summary.sh`: Generates skip summary.

*   **`93_solana_stakes_export.py`**
    *   **Inputs:**
        *   `db_config.py`: Database connection parameters.
        *   **PostgreSQL Database:** Reads data from `stake_accounts` table.
    *   **Outputs:**
        *   CSV files in `/home/smilax/block-production/api/solana-stakes/` (commented out in script):
            *   `1_total_stake_by_vote_account.csv`
            *   `2a_total_stake_by_staker.csv`
            *   `2b_total_stake_by_withdrawer.csv`
            *   `2c_total_stake_by_custodian.csv`
            *   `3_aggregate_totals.csv`
            *   `4_active_stake_distribution.csv`
            *   `5_zero_active_stake_accounts.csv`
            *   `6a_accounts_per_vote_account.csv`
            *   `6b_accounts_per_custodian.csv`
            *   `7_stake_changes_for_specific_vote_account.csv`
    *   **External Programs/Libraries:**
        *   `psycopg2`: Python library for PostgreSQL interaction.
        *   `csv`: Python standard library for CSV file handling.

*   **`93_skip_analysis.py`**
    *   **Inputs:**
        *   Epoch number (argument or user input).
        *   `db_config.py`: Database connection parameters.
        *   **PostgreSQL Database:** Reads data from `leader_schedule` and `validator_info` tables.
    *   **Outputs:**
        *   `skip_analysis_epoch_${epoch}.json`: JSON file containing skip analysis summary and validator-specific details.
    *   **External Programs/Libraries:**
        *   `psycopg2`: Python library for PostgreSQL interaction.
        *   `json`: Python standard library for JSON processing.
        *   `tqdm`: Python library for progress bars.
        *   `collections`, `decimal`, `logging`, `os`, `sys`, `time`: Python standard libraries.

*   **`4_move_json_to_production.sh`**
    *   **Inputs:**
        *   JSON files generated by `3_build_leaderboard_json.sh` (e.g., `epoch*_validator_rewards.json`, `epoch*_epoch_aggregate_data.json`, `last_ten_epoch_aggregate_data.json`, `ten_epoch_validator_rewards.json`, `ten_epoch_aggregate_data.json`, `recency_weighted_average_validator_rewards.json`, `vote_latency.json`, `vote_latency_*.json`).
    *   **Outputs:**
        *   Copies JSON files to `/home/smilax/block-production/web/public/api/`.
    *   **External Programs/Commands:**
        *   `cp`: Copies files.
        *   `bash cloudflare-purge-cache.sh`: Purges Cloudflare cache.
        *   `bash copy-json-to-web.sh`: Copies JSON to web server.

*   **`5_cp_images.sh`**
    *   **Inputs:**
        *   Image files generated by `93_build_leaderboard_json.py` (e.g., `epoch*_stake_distribution_charts.png`, `stake_distribution_charts.png`, `votes_cast_metrics_chart.png`, `latency_and_consensus_charts.png`, `epoch*_validator_counts_charts-jito*.html`).
    *   **Outputs:**
        *   Copies image files to `/home/smilax/block-production/web/public/images/`.
    *   **External Programs/Commands:**
        *   `cp`: Copies files.
        *   `bash copy-images-to-web.sh`: Copies images to web server.

*   **`61_update_discord_channel_trillium-api-data.sh`**
    *   **Inputs:**
        *   Epoch number (argument).
    *   **Outputs:**
        *   Sends messages to a Discord webhook.
    *   **External Programs/Commands:**
        *   `curl`: Used to send POST requests to the Discord webhook URL.
        *   `date`: Used to get current date/time for message content.
        *   `sed`: Used to escape JSON special characters.
        *   `bash create_trillium_alert.sh`: Creates a Trillium alert.

*   **`7_cleanup.sh`**
    *   **Inputs:** None.
    *   **Outputs:** Cleans up temporary files and directories.
    *   **External Programs/Commands:**
        *   `rm`: Removes files and directories.

*   **`90_wait-for-jito-kobe-epoch-data.sh`**
    *   **Inputs:**
        *   Epoch number (argument).
        *   Jito Kobe API (`https://kobe.mainnet.jito.network/api/v1/validators/tri1cHBy47fPyhCvrCf6FnR7Mz6XdSoSBah2FsZVQeT`): Checks for MEV data.
    *   **Outputs:**
        *   Log messages to stdout.
    *   **External Programs/Commands:**
        *   `curl`: Fetches data from Jito Kobe API.
        *   `jq`: Parses JSON response.
        *   `sleep`: Pauses execution.

*   **`92-jito-steward-data-collection.py`**
    *   **Inputs:**
        *   Epoch number (argument or user input).
        *   `db_config.py`: Database connection parameters.
        *   `rpc_config.py`: RPC endpoint configuration.
        *   `steward-cli`: Jito Steward CLI tool.
    *   **Outputs:**
        *   `jito-steward-state-all-validators-epoch-${epoch}.txt`: Steward state file.
        *   **PostgreSQL Database:** Updates `validator_stats` table with Jito steward data.
        *   `$HOME/log/${script_name}.log`: Log file for script execution.
    *   **External Programs/Libraries:**
        *   `subprocess`: Python standard library for running external commands.
        *   `psycopg2`: Python library for PostgreSQL interaction.
        *   `logging`: Python standard library for logging.

*   **`6_update_discord_channel_trillium-api-data.sh`**
    *   **Inputs:**
        *   Epoch number (argument).
    *   **Outputs:**
        *   Sends messages to a Discord webhook.
    *   **External Programs/Commands:**
        *   `curl`: Used to send POST requests to the Discord webhook URL.
        *   `date`: Used to get current date/time for message content.
        *   `sed`: Used to escape JSON special characters.
        *   `bash create_trillium_alert.sh`: Creates a Trillium alert.

*   **`copy_tar_gdrive_disk3.sh`**
    *   **Inputs:**
        *   Epoch number (argument).
        *   `epoch${epoch_number}.tar.zst`: Compressed tarball.
    *   **Outputs:**
        *   Copies `epoch${epoch_number}.tar.zst` to `/mnt/disk3/apiserver/epochs/`.
    *   **External Programs/Commands:**
        *   `cp`: Copies the tarball.

*   **`92_run_sql_updates.sh`**
    *   **Inputs:**
        *   Epoch number (argument).
        *   SQL files (e.g., `92_update_validator_stats.sql`, `92_update_epoch_aggregate_data.sql`, etc.).
    *   **Outputs:**
        *   Updates PostgreSQL database tables.
    *   **External Programs/Commands:**
        *   `psql`: Executes SQL commands.

*   **`92_update_validator_aggregate_info.py`**
    *   **Inputs:**
        *   Epoch range (user input or default).
        *   `db_config.py`: Database connection parameters.
        *   `rpc_config.py`: RPC endpoint configuration.
        *   `92_validator-info.json`: JSON file containing validator info (generated by `solana validator-info get`).
        *   `92_icon_url_errors.list`: List of identity pubkeys to skip for icon fetching.
        *   `92_gossip.json`: JSON file containing gossip data (generated by `solana gossip`).
        *   GeoIP2 databases (`GeoLite2-City.mmdb`, `GeoLite2-ASN.mmdb`).
        *   Country-region map from `https://trillium.so/pages/country-region.json`.
    *   **Outputs:**
        *   **PostgreSQL Database:** Updates `validator_info`, `leader_schedule`, `validator_stats`, `epoch_aggregate_data` tables.
        *   `/home/smilax/block-production/api/static/images/`: Directory for validator icons.
        *   `92_icon_url_errors.list`: Appends errors during icon fetching.
        *   `92_gossip.json`: Output of `solana gossip` command.
        *   `92_lookup_failures.csv`: CSV file logging GeoIP lookup failures.
        *   `92_validator-info.json`: Output of `solana validator-info get` command.
    *   **External Programs/Libraries:**
        *   `asyncio`, `csv`, `imghdr`, `ipaddress`, `json`, `logging`, `mimetypes`, `os`, `re`, `subprocess`, `sys`, `tempfile`, `threading`, `time`, `traceback`, `collections`, `concurrent.futures`, `datetime`, `decimal`, `io`, `urllib.parse`: Python standard libraries.
        *   `geoip2`: Python library for GeoIP lookups.
        *   `magic`: Python library for file type detection.
        *   `psycopg2`: Python library for PostgreSQL interaction.
        *   `requests`: Python library for HTTP requests.
        *   `urllib3`: Python library for HTTP client utilities.
        *   `solana`: Solana CLI tool.
        *   `validator-history-cli`: Validator history CLI tool.
        *   `curl`: Used for IPWHOIS lookups.

*   **`92_vote_latency_update_ead.sh`**
    *   **Inputs:**
        *   Epoch number (argument).
    *   **Outputs:**
        *   None (orchestrates Python script).
    *   **External Programs/Commands:**
        *   `python3 92_vote_latency_update_ead.py`: Updates vote latency in epoch aggregate data.

*   **`92_update_ead_inflation_reward.py`**
    *   **Inputs:**
        *   Epoch number (argument or user input).
        *   `db_config.py`: Database connection parameters.
        *   **PostgreSQL Database:** Reads `validator_stats` table.
    *   **Outputs:**
        *   **PostgreSQL Database:** Updates `epoch_aggregate_data` table with inflation rewards.
        *   `$HOME/log/${script_name}.log`: Log file for script execution.
    *   **External Programs/Libraries:**
        *   `psycopg2`: Python library for PostgreSQL interaction.
        *   `logging`: Python standard library for logging.
        *   `subprocess`: Python standard library for running external commands (`solana inflation`).
        *   `json`: Python standard library for JSON processing.
        *   `solana`: Solana CLI tool.

*   **`92_update_vs_inflation_reward.py`**
    *   **Inputs:**
        *   Epoch number (argument or user input).
        *   `db_config.py`: Database connection parameters.
        *   `rpc_config.py`: RPC endpoint configuration.
        *   **PostgreSQL Database:** Reads `validator_stats` and `stake_accounts` tables.
    *   **Outputs:**
        *   **PostgreSQL Database:** Updates `validator_stats` table with validator and delegator inflation rewards.
        *   `$HOME/log/${script_name}.log`: Log file for script execution.
    *   **External Programs/Libraries:**
        *   `psycopg2`: Python library for PostgreSQL interaction.
        *   `requests`: Python library for making HTTP requests to RPC.
        *   `logging`: Python standard library for logging.
        *   `concurrent.futures`: Python standard library for parallelism.

*   **`92_calculate_apy.py`**
    *   **Inputs:**
        *   Epoch number (argument or user input).
        *   `db_config.py`: Database connection parameters.
        *   **PostgreSQL Database:** Reads `validator_stats` and `epoch_aggregate_data` tables.
    *   **Outputs:**
        *   **PostgreSQL Database:** Updates `validator_stats` table with calculated APYs.
        *   `$HOME/log/${script_name}.log`: Log file for script execution.
    *   **External Programs/Libraries:**
        *   `psycopg2`: Python library for PostgreSQL interaction.
        *   `decimal`: Python standard library for decimal arithmetic.
        *   `logging`: Python standard library for logging.
        *   `statistics`: Python standard library for statistical functions.
        *   `math`: Python standard library for mathematical functions.

*   **`93_build_leaderboard_json.py`**
    *   **Inputs:**
        *   Epoch number (argument or user input).
        *   `db_config.py`: Database connection parameters.
        *   **PostgreSQL Database:** Reads `validator_stats`, `validator_info`, `votes_table`, `epoch_aggregate_data` tables.
    *   **Outputs:**
        *   `epoch${epoch}_validator_rewards.json`: JSON file with validator rewards data for the epoch.
        *   `epoch${epoch}_epoch_aggregate_data.json`: JSON file with epoch aggregate data.
        *   `last_ten_epoch_aggregate_data.json`: JSON file with aggregate data for the last 10 epochs.
        *   `ten_epoch_validator_rewards.json`: JSON file with average validator rewards over 10 epochs.
        *   `ten_epoch_aggregate_data.json`: JSON file with average aggregate data over 10 epochs.
        *   `recency_weighted_average_validator_rewards.json`: JSON file with recency-weighted average validator rewards.
        *   `epoch${epoch}_stake_distribution_charts.png`: PNG image of stake distribution by country/continent.
        *   `stake_distribution_charts.png`: PNG image of stake distribution (latest epoch).
        *   `epoch${epoch}_country_stats.csv`: CSV file with country stake statistics.
        *   `epoch${epoch}_continent_stats.csv`: CSV file with continent stake statistics.
        *   `epoch${epoch}_region_stats.csv`: CSV file with region stake statistics.
        *   `epoch${epoch}_stake_distribution_charts_metro.png`: PNG image of stake distribution by country/metro.
        *   `stake_distribution_charts_metro.png`: PNG image of stake distribution by country/metro (latest epoch).
        *   `epoch${epoch}_country_stats_metro.csv`: CSV file with country stake statistics (metro).
        *   `epoch${epoch}_metro_stats_metro.csv`: CSV file with metro stake statistics.
        *   `votes_cast_metrics_chart.png`: PNG image of votes cast metrics.
        *   `latency_and_consensus_charts.png`: PNG image of latency and consensus charts.
        *   `epoch_comparison_charts.png`: PNG image of epoch comparison charts.
    *   **External Programs/Libraries:**
        *   `json`, `logging`, `os`, `random`, `re`, `urllib.request`, `sys`: Python standard libraries.
        *   `numpy`: Numerical computing library.
        *   `pandas`: Data manipulation and analysis library.
        *   `plotly`: Interactive graphing library.
        *   `sqlalchemy`: SQL toolkit for database interaction.

*   **`93_build_leaderboard_json-jito-by_count.py`**
    *   **Inputs:**
        *   Epoch number (argument or user input).
        *   `db_config.py`: Database connection parameters.
        *   **PostgreSQL Database:** Reads `validator_stats` table.
        *   `93_continent_colors.json`: JSON file for continent color mapping.
        *   `93_country_colors.json`: JSON file for country color mapping.
    *   **Outputs:**
        *   `epoch${epoch}_validator_counts_charts-jito${rank_suffix}.html`: HTML chart of Jito validator counts by country/continent.
        *   `epoch${epoch}_country_counts-jito${rank_range.replace("-", "_")}.csv`: CSV file with Jito validator country counts.
        *   `epoch${epoch}_continent_counts-jito${rank_range.replace("-", "_")}.csv`: CSV file with Jito validator continent counts.
        *   `93_continent_colors.json`: Updated JSON file for continent color mapping.
        *   `93_country_colors.json`: Updated JSON file for country color mapping.
    *   **External Programs/Libraries:**
        *   `json`, `logging`, `os`, `random`, `sys`: Python standard libraries.
        *   `numpy`: Numerical computing library.
        *   `pandas`: Data manipulation and analysis library.
        *   `plotly`: Interactive graphing library.
        *   `psycopg2`: Python library for PostgreSQL interaction.

*   **`93_vote_latency_json.py`**
    *   **Inputs:**
        *   Epoch number (argument or user input).
        *   `db_config.py`: Database connection parameters.
        *   **PostgreSQL Database:** Reads `validator_xshin`, `votes_table`, `validator_stats` tables.
    *   **Outputs:**
        *   `vote_latency.json`: JSON file with vote latency data (generic).
        *   `vote_latency_${epoch}.json`: JSON file with vote latency data (epoch-specific).
    *   **External Programs/Libraries:**
        *   `json`, `sys`: Python standard libraries.
        *   `psycopg2`: Python library for PostgreSQL interaction.
        *   `decimal`: Python standard library for decimal arithmetic.

*   **`93_chart_votes_cast.py`**
    *   **Inputs:**
        *   `https://api.trillium.so/epoch_data/`: Fetches epoch data from Trillium API.
    *   **Outputs:**
        *   `votes_cast_charts.png`: PNG image of median and total votes cast per epoch.
    *   **External Programs/Libraries:**
        *   `requests`: Python library for making HTTP requests.
        *   `matplotlib.pyplot`: Python library for plotting.

*   **`93_skip_blame.py`**
    *   **Inputs:**
        *   Epoch number (argument or user input).
        *   `db_config.py`: Database connection parameters.
        *   **PostgreSQL Database:** Reads `leader_schedule` and `validator_info` tables.
    *   **Outputs:**
        *   `skip_blame_analysis_epoch_${epoch}.json`: JSON file with skip blame analysis summary and validator details.
        *   `stake_weighted_skip_blame_top_validators_epoch_${epoch}.json`: JSON file with top validators by stake-weighted skip blame score.
        *   `stake_weighted_skip_blame_top_validators_epoch_${epoch}.png`: PNG image of top validators by stake-weighted skip blame score.
        *   `stake_weighted_skip_blame_distribution_low_epoch_${epoch}.png`: PNG image of low skip blame score distribution.
        *   `stake_weighted_skip_blame_distribution_high_epoch_${epoch}.png`: PNG image of high skip blame score distribution.
        *   `skip_blame_historical_${start_epoch}_to_${epoch}.json`: (Optional) JSON file with historical skip blame summaries.
    *   **External Programs/Libraries:**
        *   `matplotlib.pyplot`: Python library for plotting.
        *   `json`, `logging`, `sys`, `time`, `collections`, `re`: Python standard libraries.
        *   `psycopg2`: Python library for PostgreSQL interaction.

*   **`93_skip_summary.sh`**
    *   **Inputs:**
        *   Epoch number (argument).
    *   **Outputs:**
        *   Log messages to stdout.
    *   **External Programs/Commands:**
        *   `python3 93_skip_summary.py`: Executes the Python script to generate a skip summary.

*   **`cloudflare-purge-cache.sh`**
    *   **Inputs:**
        *   Cloudflare API token, Zone ID, Email (environment variables or hardcoded).
        *   URLs to purge (arguments).
    *   **Outputs:**
        *   Purges Cloudflare cache for specified URLs.
    *   **External Programs/Commands:**
        *   `curl`: Makes API calls to Cloudflare.

*   **`copy-json-to-web.sh`**
    *   **Inputs:**
        *   JSON files (arguments).
    *   **Outputs:**
        *   Copies JSON files to `/home/smilax/block-production/web/public/api/`.
    *   **External Programs/Commands:**
        *   `cp`: Copies files.

*   **`copy-images-to-web.sh`**
    *   **Inputs:**
        *   Image files (arguments).
    *   **Outputs:**
        *   Copies image files to `/home/smilax/block-production/web/public/images/`.
    *   **External Programs/Commands:**
        *   `cp`: Copies files.

*   **`create_trillium_alert.sh`**
    *   **Inputs:**
        *   Message content (argument).
    *   **Outputs:**
        *   Sends messages to Discord and Telegram.
    *   **External Programs/Commands:**
        *   `curl`: Sends messages to Discord webhook and Telegram API.
        *   `sed`: Escapes JSON special characters.

*   **`get_epoch_data_csv.py`**
    *   **Inputs:**
        *   Epoch number (argument).
        *   RPC endpoints (from `rpc_config.py`).
    *   **Outputs:**
        *   `slot_data_thread_*.csv`: CSV files with slot data.
        *   `epoch_votes_thread_*.csv`: CSV files with vote data.
        *   `solana_rpc_errors.log`: Log file for RPC errors.
        *   `last_slots_to_process.txt`: Records the number of slots processed.
    *   **External Programs/Libraries:**
        *   `requests`: Python library for making RPC calls.
        *   `csv`: Python standard library for CSV handling.
        *   `json`: Python standard library for JSON processing.
        *   `logging`, `os`, `sys`, `time`, `argparse`, `math`, `glob`, `select`, `re`, `datetime`, `concurrent.futures`: Python standard libraries.

*   **`slot_data.py`**
    *   **Inputs:**
        *   Raw block data (dictionary).
        *   Slot number, epoch number.
    *   **Outputs:**
        *   Formatted slot data (dictionary).
    *   **External Programs/Libraries:**
        *   `math`, `logging`: Python standard libraries.

*   **`vote_data.py`**
    *   **Inputs:**
        *   Raw block data (dictionary).
        *   Slot number, epoch number.
    *   **Outputs:**
        *   Formatted vote data (list of dictionaries).
    *   **External Programs/Libraries:**
        *   `logging`: Python standard library.

*   **`vote_latency.py`**
    *   **Inputs:**
        *   Raw block data (dictionary).
        *   Slot number, epoch number.
    *   **Outputs:**
        *   Formatted vote latency data (list of dictionaries).
    *   **External Programs/Libraries:**
        *   `base58`: Python library for Base58 encoding/decoding.
        *   `struct`: Python standard library for packing/unpacking binary data.
        *   `logging`: Python standard library.

*   **`rpc_config.py`**
    *   **Inputs:** None (contains hardcoded RPC endpoint).
    *   **Outputs:** Provides `RPC_ENDPOINT` for other Python scripts.
    *   **External Programs/Libraries:** None.

*   **SQL Files (`.sql`)**
    *   **General Inputs:**
        *   **PostgreSQL Database:** Reads from and writes to various tables (e.g., `validator_stats`, `epoch_aggregate_data`, `validator_data`).
    *   **General Outputs:**
        *   Updates to PostgreSQL database tables.
    *   **Specific Files:**
        *   **`92_update_validator_stats.sql`**
            *   **Purpose:** Updates statistics for validators.
        *   **`92_update_epoch_aggregate_data.sql`**
            *   **Purpose:** Updates aggregate data for epochs.
        *   **`92_delete_validator_stats.sql`**
            *   **Purpose:** Deletes records from `validator_stats` table.
        *   **`92_delete_validator_data.sql`**
            *   **Purpose:** Deletes records from `validator_data` table.
        *   **`92_delete_system_account.sql`**
            *   **Purpose:** Deletes system account related data.
        *   **`92_recreate_validator_stats_to_inspect.sql`**
            *   **Purpose:** Recreates `validator_stats_to_inspect` table for inspection.
        *   **`92_recreate_validator_data_to_inspect.sql`**
            *   **Purpose:** Recreates `validator_data_to_inspect` table for inspection.
        *   **`92_set-continent-from-unknown.sql`**
            *   **Purpose:** Sets continent for unknown entries.
        *   **`92_set-country.sql`**
            *   **Purpose:** Sets country information.
        *   **`92_set-region.sql`**
            *   **Purpose:** Sets region information.
        *   **`92_update_city_names_special_characters.sql`**
            *   **Purpose:** Updates city names with special characters.
        *   **`92_move_to_vs_low_votes.sql`**
            *   **Purpose:** Moves entries with low votes to `validator_stats`.
        *   **`92_update_mev_to_jito.sql`**
            *   **Purpose:** Updates MEV data related to Jito.

## External Program and Library Dependencies

This project relies on several external programs and libraries to function correctly.

### System-Level Programs

*   **`bash`**: The primary shell interpreter for all `.sh` scripts.
*   **`python3`**: The Python interpreter for all `.py` scripts.
*   **`node`**: The Node.js runtime for `.js` scripts.
*   **`curl`**: Used for making HTTP requests (e.g., to fetch data from APIs).
*   **`jq`**: A lightweight and flexible command-line JSON processor.
*   **`tmux`**: Used for managing persistent terminal sessions (e.g., for `1_wait-for-jito-process_data.sh`).
*   **`zstd`**: A fast compression algorithm, used for decompressing `.zst` files.
*   **`tar`**: Used for archiving and extracting files.
*   **`psql`**: The PostgreSQL interactive terminal, used for executing `.sql` files.
*   **`git`**: Used for version control and managing the project repository.
*   **`dot` (Graphviz)**: Used for generating visual diagrams from `.dot` files (installed via `apt-get`).
*   **`apt-get`**: Debian/Ubuntu package manager, used for installing system dependencies like Graphviz.

### Python Libraries

*   **`psycopg2`**: PostgreSQL adapter for Python.
*   **`requests`**: HTTP library for making web requests.
*   **`pytz`**: World timezone definitions for Python.
*   **`uuid`**: For generating UUIDs.
*   **`numpy`**: Numerical computing library.
*   **`pandas`**: Data manipulation and analysis library.
*   **`plotly`**: Interactive graphing library.
*   **`PIL (Pillow)`**: Python Imaging Library (fork) for image processing.
*   **`sqlalchemy`**: SQL toolkit and Object-Relational Mapper.
*   **`tqdm`**: Progress bar for loops.
*   **`geoip2`**: MaxMind GeoIP2 database reader.
*   **`magic`**: File type identification using `libmagic`.
*   **`urllib3`**: HTTP client library, used by `requests`.
*   **`matplotlib`**: Plotting library.

### Node.js Libraries

*   Specific Node.js package dependencies (from `package.json` or `require()` statements in `.js` files) are not explicitly listed here but would be managed via `npm` or `yarn`. The `90_xshin.js` script is a Node.js script, and any external modules it uses would be dependencies.

### Database

*   **PostgreSQL**: The project heavily relies on a PostgreSQL database for storing and querying validator and epoch-related data. The `db_config.py` file is expected to contain the database connection parameters.
