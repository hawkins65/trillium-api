# Trillium Solana Data Processing Pipeline

A comprehensive, production-ready data processing pipeline for analyzing Solana blockchain data, including validator performance metrics, voting patterns, block production, staking rewards, and real-time monitoring. This system processes millions of data points across epochs to provide detailed validator analytics and leaderboard generation.

![Pipeline Status](https://img.shields.io/badge/Status-Production-brightgreen)
![Database](https://img.shields.io/badge/Database-PostgreSQL-blue)
![Language](https://img.shields.io/badge/Language-Python%20%7C%20Bash%20%7C%20Node.js-orange)

## Overview

This production-grade pipeline processes Solana epoch data to generate comprehensive validator analytics, leaderboards, and API endpoints. It collects data from multiple sources, processes it through various analytical stages, and outputs structured data for web consumption.

### Key Features

- **Real-time Monitoring**: Automated cron jobs monitor validator performance, SOL prices, epoch progress, and MEV activity
- **Comprehensive Analytics**: Processes validator data, stake percentages, block production, and voting patterns
- **Multi-Network Support**: Handles both mainnet and testnet data
- **Centralized Configuration**: Single-point configuration management for all APIs, webhooks, and validator identities
- **Automated Backup System**: Daily database and codebase backups with retention policies
- **Discord Integration**: Real-time notifications for critical events and monitoring alerts
- **Production-Ready Monitoring**: 24/7 automated monitoring with PagerDuty and Telegram integration

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
│   ├── bash/              # Shell scripts for orchestration & monitoring
│   │   ├── 00_*.sh       # Core processing scripts
│   │   ├── 90_*.sh       # Data collection scripts  
│   │   ├── 92_*.sh       # Analysis & aggregation
│   │   ├── 93_*.sh       # Output generation
│   │   └── 999_*.sh      # System monitoring & maintenance
│   ├── python/           # Data processing and analysis
│   ├── nodejs/           # API data fetching
│   ├── sql/             # Database operations
│   ├── get_slots/       # Epoch data collection
│   └── service/         # Systemd service definitions
├── config/              # Database and app configuration
├── data/
│   ├── configs/         # 🔧 Centralized configuration system
│   │   ├── notification_webhooks.json    # Discord/Telegram/PagerDuty
│   │   ├── urls_and_endpoints.json      # RPC endpoints & API URLs
│   │   └── validator_identities.json    # Validator pubkeys & groupings
│   ├── epochs/          # Epoch processing data
│   ├── monitoring/      # Real-time monitoring outputs  
│   ├── backups/         # Automated daily backups
│   ├── stake_accounts/  # Stake analysis data (by epoch)
│   ├── leader_schedules/# Leader schedule data
│   └── db_schema/       # Database schema documentation
├── docs/                # 📚 Comprehensive documentation
│   ├── UNIFIED_LOGGING_GUIDE.md      # Logging framework
│   ├── MONITORING_SCRIPTS.md         # Monitoring documentation
│   └── CONFIGURATION_MIGRATION.md    # Config system guide
└── logs/                # Centralized logging directory
```

## Configuration Management

The Trillium API uses a **centralized configuration system** for all external integrations, eliminating hardcoded values and providing single-point management.

### Production Configuration Status ✅

| Configuration File | Status | Description |
|-------------------|--------|-------------|
| `notification_webhooks.json` | 🟢 **Production Ready** | 10 Discord webhooks, Telegram bot, PagerDuty keys configured |
| `urls_and_endpoints.json` | 🟢 **Production Ready** | Alchemy RPC, QuickNode endpoints, all external APIs configured |
| `validator_identities.json` | 🟢 **Production Ready** | All 6 validator pubkeys (trillium, ofv, laine, cogent, ss, pengu) |
| `92_slot_duration_server_list.json` | 🟢 **Production Ready** | Firedancer WebSocket endpoints configured |
| `telegram_config.json` | 🟡 **Template Available** | Bot token and chat ID configured, ready to enable |
| `pagerduty_config.json` | 🟡 **Template Available** | Integration keys configured for mainnet/testnet |

### Key Features

- **Zero Hardcoded Values** - All URLs, webhooks, and identities in config files
- **Single Update Point** - Change configurations without touching code
- **Production Deployment** - Templates contain actual production values
- **Service Toggle Controls** - Enable/disable Telegram and PagerDuty via config flags
- **Configuration Loader** - `999_config_loader.sh` provides centralized access functions
- **Environment Portability** - Easy migration between environments

### Quick Configuration Setup

```bash
# Deploy all production configurations
cd ~/trillium_api/data/configs
for template in *.template; do 
    cp "$template" "${template%.template}"
done
```

See [Configuration Migration Guide](docs/CONFIGURATION_MIGRATION.md) for detailed setup and usage.

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

## Monitoring & Automation

The pipeline includes a comprehensive **24/7 monitoring system** with automated cron jobs and real-time alerting:

### 🤖 Automated Monitoring (Production Cron Jobs)

| Script | Frequency | Purpose |
|--------|-----------|---------|
| `999_monitor_sol_price.sh` | Every 5 minutes | SOL price monitoring with 5% change alerts |
| `999_epoch_discord_notification.sh` | Every 5 minutes | Epoch progress (90%, 1hr remaining, new epoch) |
| `999_check_all_for_null_mev.sh` | Every 5 minutes | MEV monitoring for 6 validators |
| `999_mainnet_stake_percentage.sh` | Every 5 minutes | Network-wide stake analysis |
| `999_testnet_stake_percentage.sh` | Every 5 minutes | Testnet stake monitoring |
| `90_get_leader_schedule.sh` | Daily at 6:00 AM | Leader schedule collection |
| `999_monitor_version.sh` | Daily at 1:00 AM | SFDP version monitoring |
| `999_backup_psql_all_tables.sh` | Daily at 12:30 AM | Database backups (7-day retention) |

### 📊 Real-time Notifications

- **Discord Integration** - 10 specialized channels for different alert types
- **Telegram Bot** - Alternative notification channel (configurable)  
- **PagerDuty Integration** - Escalation for critical alerts (configurable)
- **Structured Logging** - Unified logging system across all scripts
- **Health Monitoring** - Automatic recovery and alerting for failed processes

## Data Outputs

### 📊 Database Schema (18 Tables)
The system maintains a comprehensive PostgreSQL database with 18 specialized tables:

**Core Validator Data:**
- `validator_data` - Core validator information and identities
- `validator_stats` - Performance statistics and metrics  
- `validator_info` - Extended validator details and metadata
- `validator_stats_slot_duration` - Validator performance with timing data

**Epoch & Performance Tracking:**
- `epoch_aggregate_data` - Aggregated statistics by epoch
- `epoch_votes` - Voting data and patterns by epoch
- `slot_duration` - Block timing and slot duration analysis
- `votes_table` - Detailed voting records and classifications

**Staking & Economics:**
- `stake_accounts` - Staking account details and history
- `stake_pools` - Stake pool information and management
- `leader_schedule` - Validator leader slot assignments

**Geographic & Network Analysis:**
- `city_metro_mapping` - Geographic location mapping
- `country_regions` - Country and region classifications

**External Integrations:**
- `stakewiz_validators` - StakeWiz API validator data
- `validator_xshin` & `xshin_data` - XSHIN protocol integration
- `jito_blacklist` - MEV blacklist management

See [Database Schema Documentation](data/db_schema/DATABASE_SCHEMA_SUMMARY.md) for complete details.

### 🌐 JSON API Endpoints & Outputs
- **Validator Leaderboards** - Multiple ranking criteria (performance, stake, MEV)
- **Geographic Analysis** - Validator distribution by location and ASN
- **Stake Analysis** - Network-wide stake distribution and percentages  
- **Performance Metrics** - Block production, vote credits, uptime statistics
- **MEV Data** - Jito MEV rewards and validator earnings
- **Epoch Tracking** - Historical epoch data and progression analysis

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

## Current System Status

### Production Metrics (Epoch 829)
- **Mainnet Validators Tracked**: 1,117 total validators (1,072 active)
- **Testnet Validators Tracked**: 3,139 total validators (2,328 active)  
- **Active Stake Monitored**: 405M+ SOL on mainnet, 297M+ SOL on testnet
- **Jito Network Coverage**: 97.9% of mainnet stake, 84.8% of validators
- **Database Size**: 18 specialized tables with millions of records
- **Monitoring Coverage**: 24/7 automated monitoring with 10+ cron jobs

### System Health Indicators
- ✅ **Database**: PostgreSQL operational with daily backups
- ✅ **Monitoring**: All 10+ monitoring scripts active
- ✅ **Configuration**: Production configs deployed and operational
- ✅ **Logging**: Unified logging system across all components
- ✅ **Backups**: Automated daily backups with 7-day retention
- ✅ **Notifications**: Discord integration operational, Telegram/PagerDuty ready

## Documentation

### Available Documentation
- 🚀 **[Project Overview](docs/PROJECT_OVERVIEW.md)** - Comprehensive system overview and architecture
- 📖 **[Installation Guide](INSTALLATION.md)** - Complete setup instructions with production checklist
- 💻 **[API Usage Guide](docs/API_USAGE_GUIDE.md)** - Practical examples and usage patterns
- 🔧 **[API Dependencies](API_DEPENDENCIES.md)** - External APIs and binary requirements
- 📊 **[Monitoring Scripts](docs/MONITORING_SCRIPTS.md)** - Detailed 24/7 monitoring documentation  
- ⚙️ **[Configuration Migration](docs/CONFIGURATION_MIGRATION.md)** - Centralized configuration system
- 📝 **[Unified Logging](docs/UNIFIED_LOGGING_GUIDE.md)** - Logging framework and best practices
- 🗄️ **[Database Schema](data/db_schema/DATABASE_SCHEMA_SUMMARY.md)** - Complete 18-table database reference

## Contributing

When modifying the pipeline:

1. **Follow Naming Conventions** - Use numerical prefixes (00_, 90_, 92_, 93_, 999_)
2. **Use Unified Logging** - Source `999_common_log.sh` for consistent logging
3. **Update Configurations** - Use centralized config system, avoid hardcoded values
4. **Add Comprehensive Testing** - Test with sample epoch data before production
5. **Document Changes** - Update relevant documentation and schema files
6. **Follow Security Practices** - Never commit secrets, use `.pgpass` and config files

### Development Workflow
1. **Local Testing** - Test changes in development environment
2. **Configuration Review** - Ensure configs are properly updated
3. **Documentation Update** - Update relevant docs and schema
4. **Production Deployment** - Deploy with monitoring and rollback plan

## License

[Add appropriate license information]