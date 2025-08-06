# Monitoring Scripts Documentation

This document provides detailed information about all monitoring and automation scripts in the Trillium API.

## Script Categories

### 🏗️ Core Data Collection (90-99 prefix)
Scripts that collect essential Solana network data.

### 🔧 System Maintenance (999 prefix)
Scripts for monitoring, notifications, backups, and system health.

## Core Data Collection Scripts

### 90_get_leader_schedule.sh
**Purpose**: Fetches leader schedules for current and next epoch  
**Frequency**: Daily at 6:00 AM  
**Output**: `/home/smilax/trillium_api/data/leader_schedules/epoch{N}-leaderschedule.json`

**Features**:
- Fetches both current and next epoch leader schedules
- Validates JSON output
- Cleans up old schedules (keeps last 10 epochs)
- Uses production Solana RPC endpoint

**Dependencies**: Solana CLI (`/home/smilax/agave/bin/solana`)

### 99_get_epoch_stake_account_details.sh
**Purpose**: Collects stake account details in 10% epoch buckets  
**Frequency**: Every hour (20 minutes past the hour)  
**Output**: `/home/smilax/trillium_api/data/stake_accounts/epoch{N}_stake_{bucket}pct.json`

**Features**:
- Collects stake data in 10% epoch progression buckets (0-10%, 11-20%, etc.)
- Hour-based bucket calculation for even distribution
- Automatic cleanup of old stake files (keeps last 5 epochs)
- JSON validation and error handling

**Dependencies**: Solana CLI

## System Monitoring Scripts

### 999_monitor_version.sh
**Purpose**: Monitors SFDP version across validators  
**Frequency**: Daily at 1:00 AM UTC  
**Output**: Discord notifications, local monitoring data

**Features**:
- Checks validator versions against required SFDP versions
- Monitors both mainnet and testnet
- Performance monitoring with execution metrics
- Discord alerts for version mismatches
- Supports concurrent validator checks

**Dependencies**: Solana CLI, curl, jq

### 999_monitor_sol_price.sh
**Purpose**: Monitors SOL price with configurable alerts  
**Frequency**: Every 5 minutes  
**Output**: Discord notifications on price changes

**Features**:
- CoinMarketCap API integration
- 5% price change threshold alerts
- Hourly price updates
- 24-hour price boundary notifications
- Price history tracking

**Configuration**:
- API Key: CoinMarketCap Pro API
- Discord webhook for price alerts
- Configurable thresholds

### 999_epoch_discord_notification.sh
**Purpose**: Epoch progress notifications  
**Frequency**: Every 5 minutes  
**Output**: Discord notifications for epoch milestones

**Notifications**:
- **90% Complete**: When epoch reaches 90% completion
- **1 Hour Remaining**: Approximately 1 hour left in epoch
- **New Epoch**: When new epoch begins (first 1% of progress)

**Features**:
- Precise epoch progress calculation
- Remaining time estimation
- State tracking to prevent duplicate notifications
- Rich Discord embeds with progress metrics

### 999_check_all_for_null_mev.sh
**Purpose**: Monitors validators for null MEV configuration  
**Frequency**: Every 5 minutes  
**Output**: Discord alerts when null MEV detected

**Features**:
- Monitors 6 validators: trillium, ofv, laine, cogent, ss, pengu
- Uses stakenet-validator-history.sh for MEV checking
- Pre-configured validator identities from centralized config
- Immediate alerts for null MEV detection
- Hourly "all clear" status updates
- Results logging in JSON format

**Validator Identities**:
```bash
["trillium"]="Tri1F8B6YtjkBztGCwBNSLEZib1EAqMUEUM7dTT7ZG3"
["ofv"]="DB7DNWMVQASMFxcjkwdr4w4eg3NmfjWTk2rqFMMbrPLA"
["laine"]="LA1NEzryoih6CQW3gwQqJQffK2mKgnXcjSQZSRpM3wc"
["cogent"]="Cogent51kHgGLHr7zpkpRjGYFXM57LgjHjDdqXd4ypdA"
["ss"]="SSmBEooM7RkmyuXxuKgAhTvhQZ36Z3G2WsmLGJKoQLY"
["pengu"]="peNgUgnzs1jGogUPW8SThXMvzNpzKSNf3om78xVPAYx"
```

### 999_mainnet_stake_percentage.sh & 999_testnet_stake_percentage.sh
**Purpose**: Network-wide stake percentage analysis  
**Frequency**: Every 5 minutes  
**Output**: Hourly Discord reports, web-published rankings

**Features**:
- **Network-wide analysis** of ALL validators (not specific tracking)
- Calls Python script `stake-percentage.py` for comprehensive analysis
- Generates validator stake ranking files
- Publishes results to web via `copy-pages-to-web.sh`
- Python virtual environment activation
- Separates validators by version (0.x.x vs others)

**Process Flow**:
1. Activate Python environment (`/home/smilax/.python_env`)
2. Run `stake-percentage.py mainnet/testnet`
3. Generate `{network}-validators-stake-rank.txt`
4. Copy results to web directory
5. Send Discord notification (hourly)

**Note**: These scripts perform network-wide analysis, not tracking specific validators

### major_minor_version.sh
**Purpose**: Wrapper for major/minor version analysis  
**Frequency**: Every 5 minutes  
**Output**: Version analysis results

**Features**:
- Executes Python-based version analysis
- Standardized logging and error handling
- Integration with existing Python analysis tools

## System Maintenance Scripts

### 999_backup_psql_all_tables.sh
**Purpose**: Automated PostgreSQL database backups  
**Frequency**: Daily at 12:30 AM  
**Output**: `/home/smilax/trillium_api/data/backups/postgresql/`

**Features**:
- Complete database dump with verbose logging
- Automatic gzip compression
- 7-day retention policy
- Backup size reporting
- `.pgpass` authentication support

**Requirements**: PostgreSQL client tools, configured `.pgpass` file

### 999_backup_apiserver.sh
**Purpose**: API server code backup  
**Frequency**: Daily at 12:30 AM  
**Output**: `/home/smilax/trillium_api/data/backups/apiserver/`

**Features**:
- Complete codebase backup (excludes data directory)
- Excludes git files, cache, and virtual environments
- Tar+gzip compression
- 7-day retention policy
- Selective file exclusion for clean backups

### 999_mount_gdrive.sh
**Purpose**: Google Drive mounting with health checks  
**Frequency**: Daily at 10:00 PM + at system reboot  
**Output**: Mounted Google Drive at `/mnt/gdrive`

**Features**:
- Automatic rclone-based Google Drive mounting
- Health checks for existing mounts
- Stale mount detection and recovery
- Daemon mode operation
- Directory structure creation
- Mount verification with timeout

**Requirements**: rclone configured with 'gdrive' remote

## Centralized Configuration System

### Overview
The Trillium API uses a centralized configuration system that eliminates hardcoded values and provides single-point management for all external integrations.

### Configuration Files

#### notification_webhooks.json
**Location**: `/home/smilax/trillium_api/data/configs/notification_webhooks.json`  
**Status**: Production-ready with all Discord webhooks configured

**Contains**:
- **Discord webhooks**: 10 different channels for specific alert types
- **Telegram settings**: Bot tokens and chat IDs (template)
- **PagerDuty config**: Integration keys and service IDs (template)
- **Email settings**: SMTP configuration (template)

#### urls_and_endpoints.json
**Location**: `/home/smilax/trillium_api/data/configs/urls_and_endpoints.json`  
**Status**: Production-ready with all endpoints configured

**Contains**:
- **Solana RPC endpoints**: QuickNode primary/secondary, public endpoints
- **External APIs**: Trillium, StakeWiz, Validators.app, VX Tools, Shinobi
- **Web resources**: Logos, favicons, website URLs

#### validator_identities.json
**Location**: `/home/smilax/trillium_api/data/configs/validator_identities.json`  
**Status**: Production-ready with all validator pubkeys

**Contains**:
- **Validator groups**: Organized by partnership/ownership
- **MEV monitoring list**: Parameters for MEV checking
- **Identity mappings**: Name to pubkey associations

#### telegram_config.json
**Location**: `/home/smilax/trillium_api/data/configs/telegram_config.json`  
**Status**: Template requiring configuration

**Features**:
- Multiple chat configurations for different alert types
- Message formatting options
- Rate limiting settings
- Enable/disable flag

#### pagerduty_config.json
**Location**: `/home/smilax/trillium_api/data/configs/pagerduty_config.json`  
**Status**: Template requiring configuration

**Features**:
- Service configurations by alert type
- Escalation policy settings
- Severity definitions
- Deduplication settings

### Configuration Loader (999_config_loader.sh)

**Purpose**: Centralized functions for loading configuration values

**Key Functions**:
```bash
# Load Discord webhook
WEBHOOK=$(get_discord_webhook "mev_monitoring")

# Load RPC endpoint
RPC=$(get_rpc_url "mainnet" "primary")

# Load API URL
API=$(get_api_url "trillium_validator_rewards")

# Load validator identity
PUBKEY=$(get_validator_identity "trillium")

# Get all MEV parameters
PARAMS=$(get_mev_parameters)

# Check service status
if is_telegram_enabled; then
    # Send Telegram notification
fi
```

**Benefits**:
- No hardcoded values in scripts
- Single point of configuration updates
- Easy environment migration
- Service enable/disable flags
- Configuration validation

## Logging and Monitoring

### Log Locations
- **Script logs**: `~/log/{script_name}.log`
- **Cron output**: All cron jobs redirect to individual log files
- **Application data**: `/home/smilax/trillium_api/data/monitoring/`

### Log Features
- **Unified format**: All scripts use standardized logging functions
- **Error tracking**: Failed operations logged with context
- **Performance metrics**: Execution time and resource usage
- **Automatic rotation**: Old logs cleaned up automatically

### Monitoring Data Storage
```
/home/smilax/trillium_api/data/
├── backups/           # Automated backups
│   ├── postgresql/    # Database backups
│   └── apiserver/     # Code backups
├── configs/           # Configuration files
├── epochs/            # Epoch processing data
├── leader_schedules/  # Leader schedule data
├── monitoring/        # Monitoring script outputs
└── stake_accounts/    # Stake account data
```

## Discord Integration

### Webhook Categories
- **General**: Error notifications and system alerts
- **Public**: Public-facing notifications
- **Price**: SOL price monitoring
- **Version**: SFDP version alerts
- **Epoch**: Epoch progress notifications
- **MEV**: MEV monitoring alerts
- **Stake**: Stake percentage reports

### Notification Features
- Rich embeds with formatted data
- Color-coded alerts (red=error, green=success, blue=info)
- Timestamp and footer branding
- Field-based data presentation
- Automatic rate limiting respect

## Security Considerations

### Authentication
- Database access via `.pgpass` file (chmod 0600)
- API keys stored in configuration files
- Webhook URLs in centralized config

### Access Control
- Scripts run as user-level processes
- Data directory permissions properly set
- Log files readable by owner only

### Data Protection
- Backup files compressed and dated
- Sensitive information excluded from backups
- Regular cleanup of old files

## Troubleshooting

### Common Issues

1. **Permission Denied**:
   ```bash
   chmod +x /home/smilax/trillium_api/scripts/bash/{script_name}.sh
   ```

2. **Database Connection Failed**:
   - Verify `.pgpass` file exists and has correct permissions
   - Test database connectivity manually

3. **Discord Notifications Not Working**:
   - Verify webhook URLs in configuration
   - Test webhook with curl command

4. **Google Drive Mount Failed**:
   - Configure rclone: `rclone config`
   - Test manual mount: `rclone mount gdrive: /mnt/gdrive`

### Log Analysis
```bash
# View recent script execution
tail -f ~/log/{script_name}.log

# Check cron job status
grep CRON /var/log/syslog

# Monitor disk usage
df -h /home/smilax/trillium_api/data/
```

## Performance Optimization

### Resource Usage
- Scripts designed for minimal resource consumption
- Concurrent operations where appropriate
- Timeout settings for external API calls
- Automatic cleanup of temporary files

### Scalability
- Configurable batch sizes
- Adjustable monitoring frequencies
- Modular script architecture
- Easy configuration updates

## Future Enhancements

### Planned Features
- Telegram integration
- PagerDuty escalation
- Email notification fallback
- Prometheus metrics export
- Grafana dashboard integration
- Advanced alerting rules

### Configuration Management
- Environment-based configurations
- Centralized secret management
- Dynamic webhook routing
- Multi-environment support