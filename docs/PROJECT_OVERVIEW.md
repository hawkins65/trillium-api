# Trillium API Project Overview

**Comprehensive Guide to the Solana Validator Data Processing Pipeline**

## What is Trillium API?

The Trillium API is a production-grade, automated data processing pipeline that monitors and analyzes the Solana blockchain network. It tracks validator performance, stake distribution, block production, voting patterns, and MEV (Maximum Extractable Value) rewards across both mainnet and testnet networks.

### Key Capabilities

- **📊 Real-time Monitoring**: 24/7 automated tracking of 1,100+ mainnet and 3,100+ testnet validators
- **🔍 Performance Analysis**: Detailed metrics on block production, vote credits, and uptime
- **💰 Economic Data**: Stake analysis, rewards tracking, and MEV monitoring
- **🌍 Geographic Intelligence**: Validator distribution analysis by location and ASN
- **⚡ Automated Alerts**: Discord, Telegram, and PagerDuty integration for critical events

## System Architecture

### High-Level Data Flow

```
External APIs → Data Collection → Database Storage → Analysis Engine → Outputs
     ↓              ↓                   ↓               ↓            ↓
 Stakewiz API   90_*.sh scripts    PostgreSQL    92_*.sh scripts  JSON APIs
 Jito APIs      NodeJS fetchers    18 tables     Python analytics Web Interface
 Solana RPC     CSV processing     Time-series   SQL queries      Discord Alerts
```

### Core Components

1. **Data Collection Layer** (90_*.sh scripts)
   - Fetches data from Stakewiz, Jito, and Solana RPC APIs
   - Processes epoch data, validator info, and stake accounts
   - Handles leader schedules and block production data

2. **Storage Layer** (PostgreSQL Database)
   - 18 specialized tables for different data types
   - Optimized for time-series data and complex queries
   - Automatic backup and retention management

3. **Analysis Layer** (92_*.sh and Python scripts)
   - Aggregates validator performance metrics
   - Calculates rankings, APY, and geographic distributions
   - Processes voting patterns and MEV data

4. **Monitoring Layer** (999_*.sh scripts)
   - 24/7 cron-based monitoring system
   - Real-time alerts for price changes, epoch events, and validator issues
   - Automated backup and maintenance operations

## Data Sources

### Primary External APIs

| API Service | Purpose | Update Frequency |
|------------|---------|------------------|
| **Stakewiz API** | Validator scoring, geographic data, performance metrics | Hourly |
| **Jito Kobe API** | MEV rewards, Jito-specific validator performance | Real-time |
| **Solana RPC** | Blockchain data, voting records, stake accounts | Real-time |
| **XSHIN API** | Validator awards and special recognitions | Daily |
| **CoinMarketCap** | SOL price data for economic calculations | Every 5 minutes |

### Data Coverage

- **Mainnet**: 1,117 total validators (1,072 active)
- **Testnet**: 3,139 total validators (2,328 active)
- **Stake Monitored**: 405M+ SOL mainnet, 297M+ SOL testnet
- **Historical Data**: Multi-epoch time-series analysis
- **Geographic Coverage**: Global validator distribution tracking

## Key Features

### 🤖 Automated Monitoring System

The system includes 10+ production cron jobs running 24/7:

- **Price Monitoring**: SOL price alerts (5% threshold)
- **Epoch Tracking**: 90% complete, 1-hour remaining, new epoch notifications
- **MEV Monitoring**: Null MEV detection for 6 specific validators
- **Stake Analysis**: Network-wide stake percentage tracking
- **Version Monitoring**: SFDP version compliance checking
- **Health Monitoring**: System backup and maintenance automation

### 🔧 Configuration Management

Centralized configuration system eliminates hardcoded values:

- **10 Discord Webhooks**: Specialized channels for different alert types
- **RPC Endpoints**: Alchemy, QuickNode, and public RPC configurations
- **Validator Identities**: All 6 monitored validator pubkeys
- **Service Controls**: Toggle Telegram, PagerDuty, and other integrations

### 📊 Database Schema

18 specialized PostgreSQL tables organize different aspects of validator data:

**Core Validator Tables:**
- `validator_data` - Basic validator information
- `validator_stats` - Performance metrics and rankings
- `validator_info` - Extended metadata and details

**Performance & Economics:**
- `epoch_aggregate_data` - Epoch-level statistics
- `stake_accounts` - Staking data and history
- `slot_duration` - Block timing analysis

**External Integrations:**
- `stakewiz_validators` - StakeWiz API data
- `jito_blacklist` - MEV blacklist management
- `xshin_data` - XSHIN protocol integration

## Monitoring & Alerting

### Discord Integration (Production Ready)

10 specialized Discord channels provide real-time notifications:

- **General Notifications**: System errors and status updates
- **Price Monitor**: SOL price change alerts (5% threshold)
- **Epoch Events**: Milestone notifications (90%, 1-hour, new epoch)
- **MEV Monitoring**: Null MEV detection for tracked validators
- **Stake Reports**: Network-wide stake analysis
- **Version Alerts**: SFDP version compliance monitoring

### Additional Alert Channels (Ready to Enable)

- **Telegram Bot**: Alternative notification channel
- **PagerDuty**: Escalation for critical system alerts
- **Email Notifications**: Backup alert mechanism

## File Organization

### Script Categories

| Prefix | Purpose | Examples |
|--------|---------|----------|
| `00_*` | Core orchestration | `00_process_all_automated.sh` |
| `90_*` | Data collection | `90_xshin_load_data.sh`, `90_get_leader_schedule.sh` |
| `92_*` | Analysis & aggregation | `92_calculate_apy.py`, `92_slot_duration_statistics.py` |
| `93_*` | Output generation | `93_build_leaderboard_json.py`, `93_solana_stakes_export.py` |
| `999_*` | System monitoring | `999_monitor_sol_price.sh`, `999_backup_psql_all_tables.sh` |

### Configuration Structure

```
data/configs/
├── notification_webhooks.json     # 🟢 Production ready
├── urls_and_endpoints.json        # 🟢 Production ready  
├── validator_identities.json      # 🟢 Production ready
├── 92_slot_duration_server_list.json # 🟢 Production ready
├── telegram_config.json           # 🟡 Template available
└── pagerduty_config.json          # 🟡 Template available
```

## Getting Started

### For New Users

1. **Read the Installation Guide**: [INSTALLATION.md](../INSTALLATION.md)
2. **Review Configuration**: [CONFIGURATION_MIGRATION.md](CONFIGURATION_MIGRATION.md)
3. **Understand Monitoring**: [MONITORING_SCRIPTS.md](MONITORING_SCRIPTS.md)
4. **Setup Logging**: [UNIFIED_LOGGING_GUIDE.md](UNIFIED_LOGGING_GUIDE.md)

### For Developers

1. **Understand Architecture**: Review this document and README.md
2. **Database Schema**: Check [DATABASE_SCHEMA_SUMMARY.md](../data/db_schema/DATABASE_SCHEMA_SUMMARY.md)
3. **API Dependencies**: Review [API_DEPENDENCIES.md](../API_DEPENDENCIES.md)
4. **Development Workflow**: Follow contribution guidelines in README.md

## Production Status

### Current System Health ✅

- **Database**: PostgreSQL operational with daily backups
- **Monitoring**: 10+ automated monitoring scripts active
- **Configuration**: Production configs deployed and validated
- **Logging**: Unified logging system across all components
- **Backups**: Daily automated backups with 7-day retention
- **Notifications**: Discord integration operational

### Performance Metrics

- **Data Volume**: Millions of records across 18 database tables
- **Processing Speed**: Full epoch analysis in minutes
- **Uptime**: 24/7 monitoring with automatic recovery
- **Coverage**: 97.9% of mainnet stake monitored
- **Alert Response**: Real-time notifications for critical events

## Use Cases

### Validator Operators

- **Performance Tracking**: Monitor your validator's ranking and metrics
- **Competitive Analysis**: Compare performance against network averages
- **Economic Insights**: Track rewards, stake changes, and MEV earnings
- **Operational Alerts**: Get notified of performance issues or network events

### Researchers & Analysts

- **Network Analysis**: Study validator distribution and decentralization
- **Economic Research**: Analyze stake flows, rewards, and validator economics
- **Performance Studies**: Research block production efficiency and voting patterns
- **Geographic Analysis**: Understand global validator distribution

### Solana Ecosystem

- **Network Health Monitoring**: Track overall network performance
- **Validator Discovery**: Find high-performing validators for delegation
- **Economic Data**: Access comprehensive stake and rewards data
- **Alert Systems**: Monitor critical network events and changes

## Support & Documentation

### Available Resources

- 📖 **Installation Guide**: Complete setup instructions
- 🔧 **Configuration Guide**: Centralized configuration management
- 📊 **Monitoring Documentation**: Detailed monitoring script guide
- 🗄️ **Database Documentation**: Complete schema reference
- 📝 **Logging Guide**: Unified logging system documentation
- 🔗 **API Reference**: External dependencies and integration guide

### Getting Help

1. **Check Documentation**: Review relevant documentation files
2. **Examine Log Files**: Check `~/log/` for detailed error messages
3. **Test Components**: Use individual scripts to isolate issues
4. **Monitor Resources**: Check disk space, memory, and database performance
5. **Review Configuration**: Ensure all config files are properly deployed

## Future Roadmap

### Planned Enhancements

- **API Expansion**: RESTful API endpoints for external access
- **Dashboard Interface**: Web-based monitoring and visualization
- **Advanced Analytics**: Machine learning for validator performance prediction
- **Multi-Network Support**: Extension to other Solana networks
- **Enhanced Alerting**: More sophisticated alert rules and escalation

### Integration Opportunities

- **Grafana Dashboards**: Visual monitoring and alerting
- **Prometheus Metrics**: Time-series metrics export
- **API Gateway**: Rate-limited public API access
- **Mobile Notifications**: Push notifications for critical alerts
- **Third-party Integrations**: Slack, Microsoft Teams, and other platforms

---

This overview provides a comprehensive introduction to the Trillium API system. For specific implementation details, refer to the individual documentation files and source code.