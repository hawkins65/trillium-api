# Trillium Solana Data Processing Pipeline

A comprehensive data processing pipeline for analyzing Solana blockchain data, including validator performance metrics, voting patterns, block production, and staking rewards.

## Overview

This pipeline processes Solana epoch data to generate comprehensive validator analytics, leaderboards, and API endpoints. It collects data from multiple sources, processes it through various analytical stages, and outputs structured data for web consumption.

## Architecture

### Data Flow

```
Epoch Data Collection → Data Processing → Analysis & Aggregation → Web Publishing
```

1. **Collection Phase**: Gather raw blockchain data from multiple APIs
2. **Processing Phase**: Load, validate, and consolidate data 
3. **Analysis Phase**: Calculate metrics, rankings, and derived values
4. **Publishing Phase**: Generate JSON outputs and deploy to web

### Directory Structure

```
trillium_api/
├── scripts/
│   ├── bash/           # Shell scripts for orchestration
│   ├── python/         # Data processing and analysis
│   ├── nodejs/         # API data fetching
│   ├── sql/           # Database operations
│   └── get_slots/     # Epoch data collection
├── config/            # Database and app configuration
├── data/
│   ├── configs/       # Centralized configuration files
│   ├── epochs/        # Epoch processing data
│   ├── monitoring/    # Monitoring outputs
│   ├── backups/       # Automated backups
│   └── stake_accounts/# Stake analysis data
└── docs/             # Documentation
```

## Configuration Management

The Trillium API uses a **centralized configuration system** for all external integrations:

### Configuration Files (in `data/configs/`)

1. **`notification_webhooks.json`** - Discord, Telegram, PagerDuty webhooks
2. **`urls_and_endpoints.json`** - RPC endpoints, API URLs, web resources
3. **`validator_identities.json`** - Validator pubkeys and groupings
4. **`telegram_config.json`** - Telegram bot settings (template)
5. **`pagerduty_config.json`** - PagerDuty integration (template)

### Key Features

- **No hardcoded values** - All URLs/webhooks in config files
- **Single update point** - Change configs without touching code
- **Config loader** - `999_config_loader.sh` provides helper functions
- **Production ready** - Templates include actual production values

See [Configuration Migration Guide](docs/CONFIGURATION_MIGRATION.md) for details.

## Main Processing Scripts

### Core Orchestration

- **`00_process_all_automated.sh`** - Main entry point, infinite loop processing
- **`0_process_getslots_data.sh`** - Coordinates epoch data collection and initial processing

### Data Collection (90_* scripts)

- **`90_xshin_load_data.sh`** - Fetches Xshin validator data via Node.js API
- **`90_stakewiz_validators.py`** - Collects Stakewiz validator metrics
- **`90_get_block_data.sh`** - Retrieves and processes block production data
- **`90_untar_epoch.sh`** - Extracts compressed epoch archives

### Data Processing (1_* scripts)

- **`1_load_consolidated_csv.sh`** - Loads consolidated slot data from CSV
- **`1_no-wait-for-jito-process_data.sh`** - Immediate processing without Jito data
- **`1_wait-for-jito-process_data.sh`** - Waits for Jito data, runs in background tmux

### Aggregation & Analysis (2_* scripts)

- **`2_update_validator_aggregate_info.sh`** - Main aggregation of all validator data
- **`92_update_validator_aggregate_info.py`** - Core Python aggregation logic
- **`92_calculate_apy.py`** - Annual percentage yield calculations
- **`92_vx-call.py`** - Vote latency analysis
- **`92_slot_duration_statistics.py`** - Block timing analysis

### Output Generation (3_* scripts)

- **`3_build_leaderboard_json.sh`** - Generates leaderboard JSON files
- **`93_build_leaderboard_json-jito-by_count.py`** - Jito-specific leaderboards
- **`93_solana_stakes_export.py`** - Stake distribution exports

### Deployment (4_*, 5_*, 7_* scripts)

- **`4_move_json_to_production.sh`** - Deploys JSON files to web server
- **`5_cp_images.sh`** - Copies validator images and assets
- **`7_cleanup.sh`** - Cleanup operations and maintenance

## Key Data Sources

### External APIs

1. **Jito Kobe API** - `https://kobe.mainnet.jito.network/api/v1/`
   - Validator MEV rewards and performance data
   - Requires jito validator-history-cli binary

2. **Stakewiz API** - `https://api.stakewiz.com/`
   - Comprehensive validator scoring and metrics
   - Geographic and network performance data

3. **Xshin API** - Various endpoints for validator awards and rankings

4. **Solana RPC** - Direct blockchain data via RPC calls
   - Block production data
   - Voting records and credits
   - Stake account information

### Database

- **PostgreSQL** - Primary data store
  - Tables: `validator_data`, `validator_stats`, `epoch_aggregate_data`
  - Configured via `db_config.py`

## Required Dependencies

### System Binaries

- `psql` - PostgreSQL client
- `curl` - HTTP requests  
- `jq` - JSON processing
- `tmux` - Background process management  
- `zstd` - Compression/decompression
- `node` - Node.js runtime
- `python3` - Python 3.x

### Python Packages

See `config/requirements.txt` for complete list:
- `psycopg2` - PostgreSQL adapter
- `requests` - HTTP library
- `pandas` - Data analysis
- `numpy` - Numerical computing

### Node.js Packages  

See `config/package.json` for dependencies

### External CLI Tools

- **Jito Validator History CLI** - Download from Jito Network
  - Used for fetching MEV and Jito-specific validator data
  - Must be in PATH or specified in scripts

## Configuration

### Environment Variables

- `LOG_BASE_DIR` - Base directory for log files (default: `$HOME/log`)
- `LOG_LEVEL` - Logging verbosity (DEBUG, INFO, WARN, ERROR)
- `DATABASE_URL` - PostgreSQL connection string

### Key Configuration Files

- `config/db_config.py` - Database connection parameters
- `config/requirements.txt` - Python dependencies
- `config/package.json` - Node.js dependencies

## Usage

### Basic Operation

```bash
# Process a specific epoch
./scripts/bash/00_process_all_automated.sh 825

# Process current epoch and continue infinitely  
./scripts/bash/00_process_all_automated.sh
```

### Manual Stage Execution

```bash
# Just collect data for epoch 825
./scripts/bash/0_process_getslots_data.sh 825

# Update validator aggregates only
./scripts/bash/2_update_validator_aggregate_info.sh 825

# Generate leaderboards only  
./scripts/bash/3_build_leaderboard_json.sh 825
```

## Monitoring & Notifications

The pipeline includes comprehensive monitoring via:

- **Discord Webhooks** - Real-time status updates and error alerts
- **Telegram Notifications** - Alternative notification channel
- **Structured Logging** - Detailed logs with rotation and retention
- **Process Monitoring** - Health checks and restart capabilities

## Data Outputs

### JSON API Endpoints
- Validator leaderboards with multiple ranking criteria
- Geographic distribution analysis  
- Performance metrics and trends
- Stake pool information
- MEV rewards and Jito data

### Database Tables
- Historical validator performance data
- Aggregated epoch statistics
- Vote latency and timing analysis
- Geographic and network topology data

## Troubleshooting

### Common Issues

1. **Database Connection Failures** - Check `config/db_config.py`
2. **API Timeouts** - External APIs may be temporarily unavailable
3. **Missing Dependencies** - Verify all required binaries are installed
4. **Disk Space** - Large epoch files require significant storage

### Log Files

Logs are stored in `$HOME/log/` with automatic rotation:
- `{script_name}.log` - Current log
- `{script_name}.log.{timestamp}.gz` - Rotated logs

### Error Recovery

Most scripts include error handling and user prompts:
- Press 'Y' or Enter to continue after errors
- Check Discord notifications for detailed error context
- Use tmux sessions to monitor long-running processes

## Contributing

When modifying the pipeline:

1. Follow existing naming conventions (numerical prefixes)
2. Add comprehensive logging and error handling
3. Update documentation for new data sources or outputs
4. Test with sample epoch data before production deployment

## License

[Add appropriate license information]