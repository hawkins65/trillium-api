# Trillium API Installation Guide

## Prerequisites

### System Requirements

- Ubuntu/Debian Linux (recommended)
- Minimum 16GB RAM
- 500GB+ available disk space for epoch data
- PostgreSQL 12+ 
- Python 3.8+
- Node.js 16+

### Required System Packages

**Note: The setup script can automatically install these packages with sudo prompts.**

For manual installation:
```bash
# Update system
sudo apt update && sudo apt upgrade -y

# Install core dependencies
sudo apt install -y \
    postgresql-client \
    python3 python3-pip python3-venv python3-dev \
    nodejs npm \
    curl jq tmux \
    zstd gzip tar \
    git build-essential \
    libpq-dev bc
```

**PostgreSQL Server Setup:** Use an AI tool to install PostgreSQL v16 server with v17 client (see Database Setup section below).

## Database Setup

### PostgreSQL Installation and Configuration

**Recommended: Use an AI Tool for PostgreSQL Setup**

Instead of manual installation, we recommend using an AI assistant (ChatGPT, Claude, etc.) to help with PostgreSQL setup. Use this prompt:

```
Help me install PostgreSQL v16 server with v17 client on Ubuntu. 
I need to:
1. Install PostgreSQL v16 server and v17 client
2. Create a database named 'sol_blocks'
3. Create a user named 'smilax' with full permissions on the sol_blocks database
4. Ensure the service starts automatically
5. Provide the exact commands and configuration steps
```

The AI will provide current, tested commands for your specific Ubuntu version and handle any version-specific installation requirements.

**Alternative Manual Setup** (if you prefer):
```bash
# Start PostgreSQL service (after installation)
sudo systemctl start postgresql
sudo systemctl enable postgresql

# Create database and user
sudo -u postgres psql
CREATE DATABASE sol_blocks;
CREATE USER smilax WITH ENCRYPTED PASSWORD 'your_secure_password';
GRANT ALL PRIVILEGES ON DATABASE sol_blocks TO smilax;
\q
```

3. **Configure database connection**:

First, create a `.pgpass` file to securely store your database password:
```bash
# Create .pgpass file in your home directory (update with your actual values)
echo "localhost:5432:sol_blocks:your_username:your_secure_password" > ~/.pgpass

# Set secure permissions (required for PostgreSQL to use this file)
chmod 0600 ~/.pgpass
```

Then, copy the database configuration template (contains production-ready settings):
```bash
cd ~/trillium_api
cp config/db_config.py.template config/db_config.py
```

Edit `config/db_config.py` if needed (template contains production values):
```python
db_params = {
    "host": "localhost",
    "port": "5432", 
    "database": "sol_blocks",            # Standard production database name
    "user": "smilax",                    # Update to match your username
    "sslmode": "disable"
    # Password is automatically read from ~/.pgpass
}
```

**Note**: The template already contains production-ready values. You may only need to update the `user` field.

**Security Note**: The password is **not** stored in the code. It's read from the `~/.pgpass` file, which:
- Is excluded from git commits
- Has secure file permissions (0600)
- Follows PostgreSQL's recommended security practices

### Database Schema Setup

Run the database initialization scripts (if available) or create tables as needed based on the Python scripts' requirements.

## Python Environment Setup

### Create Virtual Environment

```bash
cd ~/trillium_api
python3 -m venv venv
source venv/bin/activate
```

### Install Python Dependencies

```bash
pip install -r config/requirements.txt
```

If `requirements.txt` is incomplete, install core packages:
```bash
pip install psycopg2-binary requests pandas numpy pytz uuid
```

## Node.js Setup

### Install Dependencies

```bash
cd ~/trillium_api
npm install --prefix scripts/nodejs
```

If `package.json` is minimal, install required packages:
```bash
cd scripts/nodejs
npm init -y
npm install axios dotenv
```

## WebSocket Server Configuration

### Configure Solana RPC WebSocket Endpoints

1. **Copy the WebSocket server list template (contains production endpoints)**:
```bash
cd ~/trillium_api
cp data/configs/92_slot_duration_server_list.json.template data/configs/92_slot_duration_server_list.json
```

2. **Review/modify the server configuration**:
The template contains production Firedancer WebSocket endpoints. Edit `data/configs/92_slot_duration_server_list.json` if you want to use different RPC providers:
```json
{
  "servers": [
    {
      "name": "your_rpc_server_1",
      "endpoint": "wss://your-rpc-provider.com/websocket",
      "location": "Your Server Location",
      "group": 1,
      "continent": "North America"
    }
  ]
}
```

**Required Fields**:
- `name`: Unique identifier for logging and monitoring
- `endpoint`: WebSocket URL (`ws://` or `wss://`)
- `location`: Human-readable location description
- `group`: Server group number for organization
- `continent`: Continent name for geographic analysis

**Notes**:
- Multiple servers can be configured for redundancy
- WebSocket endpoints are used for real-time slot duration monitoring
- Both secure (`wss://`) and non-secure (`ws://`) connections are supported

## External Binary Installation

### Jito Validator History CLI

Follow the official stakenet installation documentation:
**https://github.com/jito-foundation/stakenet/blob/master/README.md**

After installation, ensure the binary is available at one of these locations:
- `/usr/local/bin/validator-history`
- `~/stakenet/target/release/validator-history-cli`

**Verify installation**:
```bash
validator-history --version
# or
~/stakenet/target/release/validator-history-cli --version
```

### Solana CLI (Optional but Recommended)

Follow the official Agave installation documentation:
**https://github.com/anza-xyz/agave/blob/master/docs/src/cli/install.md#build-from-source**

After installation, ensure the binary is available at `~/agave/bin/solana` and add to your PATH:
```bash
export PATH="$HOME/agave/bin:$PATH"
echo 'export PATH="$HOME/agave/bin:$PATH"' >> ~/.bashrc
```

## Directory Structure Setup

### Create Required Directories

```bash
mkdir -p ~/log/get_slots
mkdir -p ~/api
mkdir -p ~/.config/solana
# Note: get_slots functionality is integrated in scripts/get_slots/
mkdir -p ~/epochs
```

### Set Permissions

```bash
chmod +x ~/trillium_api/scripts/bash/*.sh
chmod +x ~/trillium_api/scripts/get_slots/*.sh
```

## Configuration

### Environment Variables

Add to `~/.bashrc`:
```bash
export LOG_BASE_DIR="$HOME/log"
export LOG_LEVEL="INFO"
export TRILLIUM_API_ROOT="$HOME/trillium_api"
export PATH="$TRILLIUM_API_ROOT/scripts/bash:$PATH"
```

Reload:
```bash
source ~/.bashrc
```

### Notification and API Configuration

The Trillium API uses a centralized configuration system for all external integrations:

#### 1. **Deploy All Configuration Files**

```bash
cd ~/trillium_api/data/configs

# Deploy all configuration templates (production-ready values included)
for template in *.template; do 
    cp "$template" "${template%.template}"
done

# Verify all configs are deployed
ls -la *.json
```

#### 2. **Configuration Files Overview**

| File | Purpose | Status |
|------|---------|--------|
| `notification_webhooks.json` | Discord webhook URLs for all channels | ✅ Production-ready |
| `urls_and_endpoints.json` | RPC endpoints (including Alchemy) and API URLs | ✅ Production-ready |
| `validator_identities.json` | Identity & vote pubkeys for all validators | ✅ Production-ready |
| `telegram_config.json` | Telegram bot token and chat IDs | ✅ Production-ready |
| `pagerduty_config.json` | PagerDuty integration keys (mainnet/testnet) | ✅ Production-ready |

#### 3. **Discord Webhooks Configuration**

The `notification_webhooks.json` includes webhooks for:
- **general_notifications**: System alerts and errors
- **public_notifications**: Public-facing announcements  
- **sol_price_monitor**: SOL price change alerts
- **version_monitor**: SFDP version monitoring
- **slot_progression**: Slot processing updates
- **wss_monitoring**: WebSocket monitoring alerts
- **epoch_notifications**: Epoch progress (90%, 1hr, new)
- **mev_monitoring**: NULL MEV detection alerts
- **mainnet_stake**: Mainnet stake analysis
- **testnet_stake**: Testnet stake analysis

#### 4. **URLs and Endpoints Configuration**

The `urls_and_endpoints.json` includes:
- **Solana RPC**: 
  - Alchemy mainnet endpoint (97zE6cvElUYwOp_zVSqMXt5H7dYSYxtW)
  - Primary/secondary QuickNode endpoints
  - Public Solana endpoints
- **External APIs**: Trillium, StakeWiz, Validators.app, VX Tools, Shinobi, CoinMarketCap
- **Web Resources**: Logos, favicons, avatar URLs

#### 5. **Validator Identities Configuration**

The `validator_identities.json` includes:
- **Identity Pubkeys**: Validator identity addresses
- **Vote Pubkeys**: All 6 validator vote account addresses configured
- **Validator Groups**: Organized by partnership/ownership

Example structure:
```json
{
  "identity_pubkeys": {
    "trillium_and_partners": {
      "trillium": "Tri1F8B6YtjkBztGCwBNSLEZib1EAqMUEUM7dTT7ZG3",
      "ofv": "DB7DNWMVQASMFxcjkwdr4w4eg3NmfjWTk2rqFMMbrPLA",
      ...
    }
  },
  "vote_pubkeys": {
    "trillium": "tri1cHBy47fPyhCvrCf6FnR7Mz6XdSoSBah2FsZVQeT",
    "ofv": "oRAnGeU5h8h2UkvbfnE5cjXnnAa4rBoaxmS4kbFymSe",
    ...
  }
}
```

#### 6. **Using the Configuration Loader**

All monitoring scripts can use the centralized configuration loader:

```bash
# In your script, source the config loader
source /home/smilax/trillium_api/scripts/bash/999_config_loader.sh

# Load configurations
DISCORD_WEBHOOK=$(get_discord_webhook mev_monitoring)
MAINNET_RPC=$(get_rpc_url mainnet primary)
TRILLIUM_API=$(get_api_url trillium_validator_rewards)
VALIDATOR_PUBKEY=$(get_validator_identity trillium)
```

Available functions:
- `get_discord_webhook(type)` - Load Discord webhook by type
- `get_rpc_url(network, type)` - Load Solana RPC endpoint
- `get_api_url(name)` - Load external API URL
- `get_validator_identity(name)` - Load validator pubkey
- `get_validator_vote_pubkey(name)` - Load validator vote account pubkey
- `get_mev_parameters()` - Get all MEV monitoring parameters
- `is_telegram_enabled()` - Check if Telegram is configured
- `is_pagerduty_enabled()` - Check if PagerDuty is configured

### API Keys and Credentials

Create `.env` file in project root if needed:
```bash
# External API keys (if required)
STAKEWIZ_API_KEY=your_key_here
JITO_API_KEY=your_key_here
```

## Network Configuration

### Firewall (if applicable)

```bash
# Allow PostgreSQL (local only)
sudo ufw allow from 127.0.0.1 to any port 5432

# Allow outbound HTTPS for API calls
sudo ufw allow out 443
sudo ufw allow out 80
```

### DNS Resolution

Ensure external APIs are accessible:
```bash
curl -I https://api.stakewiz.com/validators
curl -I https://kobe.mainnet.jito.network/api/v1/validators
```

## Testing Installation

### Database Connectivity Test

```bash
cd ~/trillium_api
python3 -c "
import sys
sys.path.append('config')
from db_config import db_params
import psycopg2
try:
    conn = psycopg2.connect(**db_params)
    print('Database connection successful')
    conn.close()
except Exception as e:
    print(f'Database connection failed: {e}')
"
```

### API Connectivity Test

```bash
# Test Stakewiz API
curl -s "https://api.stakewiz.com/validators" | jq '. | length' || echo "Stakewiz API unavailable"

# Test Jito API  
curl -s "https://kobe.mainnet.jito.network/api/v1/validators" | jq '. | length' || echo "Jito API unavailable"
```

### Script Execution Test

```bash
cd ~/trillium_api/scripts/bash
./999_common_log.sh
echo "If no errors, logging framework is working"
```

## Service Setup (Optional)

### Systemd Service for Automated Processing

Create `/etc/systemd/system/trillium-api.service`:
```ini
[Unit]
Description=Trillium API Data Processing
After=network.target postgresql.service

[Service]
Type=simple
User=your_username
WorkingDirectory=/home/your_username/trillium_api
ExecStart=/home/your_username/trillium_api/scripts/bash/00_process_all_automated.sh
Restart=always
RestartSec=30
Environment=LOG_LEVEL=INFO

[Install]
WantedBy=multi-user.target
```

Enable and start:
```bash
sudo systemctl enable trillium-api.service
sudo systemctl start trillium-api.service
```

## Monitoring Setup

### Log Rotation

Create `/etc/logrotate.d/trillium-api`:
```
/home/your_username/log/*.log {
    daily
    rotate 7
    compress
    delaycompress
    missingok
    notifempty
    create 644 your_username your_username
}
```

### Health Check Script

Create monitoring script to check process health and disk usage.

## Troubleshooting

### Common Installation Issues

1. **Permission Denied**: Check file permissions and ownership
2. **Module Not Found**: Verify Python virtual environment activation
3. **Database Connection**: Check PostgreSQL service status and credentials
4. **API Timeouts**: Verify network connectivity and firewall rules
5. **Disk Space**: Monitor available space in log and data directories

### Log File Locations

- Application logs: `~/log/`
- PostgreSQL logs: `/var/log/postgresql/`
- System logs: `/var/log/syslog`

### Getting Help

- Check log files for detailed error messages
- Verify all dependencies are installed correctly
- Test individual components before running full pipeline
- Monitor system resources during operation

## Automated Monitoring and Cron Jobs

The trillium_api includes comprehensive monitoring scripts that can be set up as cron jobs:

### Core Monitoring Scripts

| Script | Purpose | Frequency | Description |
|--------|---------|-----------|-------------|
| `90_get_leader_schedule.sh` | Leader Schedule | Daily (6am) | Fetches leader schedules for current and next epoch |
| `99_get_epoch_stake_account_details.sh` | Stake Accounts | Hourly | Collects stake account details in 10% epoch buckets |
| `999_monitor_version.sh` | Version Monitor | Daily (1am) | Monitors SFDP version across validators |
| `999_monitor_sol_price.sh` | Price Monitor | Every 5min | Monitors SOL price with Discord alerts |
| `999_epoch_discord_notification.sh` | Epoch Events | Every 5min | Notifications for 90% complete, 1hr remaining, new epoch |
| `999_check_all_for_null_mev.sh` | MEV Monitor | Every 5min | Checks SS/Trillium validators for null MEV |
| `999_mainnet_stake_percentage.sh` | Mainnet Stake | Every 5min | Monitors mainnet stake percentage |
| `999_testnet_stake_percentage.sh` | Testnet Stake | Every 5min | Monitors testnet stake percentage |
| `major_minor_version.sh` | Version Check | Every 5min | Runs major/minor version analysis |

### System Maintenance Scripts

| Script | Purpose | Frequency | Description |
|--------|---------|-----------|-------------|
| `999_backup_psql_all_tables.sh` | Database Backup | Daily (12:30am) | Compressed PostgreSQL backups (7-day retention) |
| `999_backup_apiserver.sh` | Code Backup | Daily (12:30am) | API server code backup (excludes data) |
| `999_mount_gdrive.sh` | Google Drive | Daily (10pm) + @reboot | Mounts Google Drive with health checks |

### Setting Up Cron Jobs

1. **Install the complete crontab**:
```bash
# Copy the provided crontab configuration
crontab -e
```

2. **Add this complete crontab**:
```bash
# get the leaderschedule every day at 6am
0 6 * * * /home/smilax/trillium_api/scripts/bash/90_get_leader_schedule.sh >> /home/smilax/log/90_get_leader_schedule.log 2>&1

# get the stake accounts 10 times per epoch (at 0-10%,11-20%, etc.)
20 * * * * /home/smilax/trillium_api/scripts/bash/99_get_epoch_stake_account_details.sh >> /home/smilax/log/99_get_epoch_stake_account_details.log 2>&1

# run these backups every night at 12:30 AM 
30 0 * * * /home/smilax/trillium_api/scripts/bash/999_backup_psql_all_tables.sh >> /home/smilax/log/999_backup_psql_all_tables.log 2>&1
30 0 * * * /home/smilax/trillium_api/scripts/bash/999_backup_apiserver.sh >> /home/smilax/log/999_backup_apiserver.log 2>&1

# mount google drive every day at 10:00pm and at reboot
0 22 * * * /home/smilax/trillium_api/scripts/bash/999_mount_gdrive.sh >> /home/smilax/log/999_mount_gdrive.log 2>&1
@reboot /home/smilax/trillium_api/scripts/bash/999_mount_gdrive.sh >> /home/smilax/log/999_mount_gdrive.log 2>&1

# monitor SFDP version once per day at 1am UTC and update discord 
0 1 * * * /home/smilax/trillium_api/scripts/bash/999_monitor_version.sh >> /home/smilax/log/999_monitor_version.log 2>&1

# run solana price check every 5 minutes and update discord if 5% change or once per hour
*/5 * * * * /home/smilax/trillium_api/scripts/bash/999_monitor_sol_price.sh >> /home/smilax/log/999_monitor_sol_price.log 2>&1

# run check for null mev on all SS and Trillium validators every 5 minutes and update discord
*/5 * * * * /home/smilax/trillium_api/scripts/bash/999_check_all_for_null_mev.sh >> /home/smilax/log/999_check_all_for_null_mev.log 2>&1

# run epoch_notification every 5 minutes and update discord
# for Solana epoch events (90% complete, 1 hour remaining, and new epoch start)
*/5 * * * * /home/smilax/trillium_api/scripts/bash/999_epoch_discord_notification.sh >> /home/smilax/log/999_epoch_discord_notification.log 2>&1

# run the testnet and mainnet stake percentage program every 5 minutes
*/5 * * * * /home/smilax/trillium_api/scripts/bash/999_testnet_stake_percentage.sh >> /home/smilax/log/999_testnet_stake_percentage.log 2>&1
*/5 * * * * /home/smilax/trillium_api/scripts/bash/999_mainnet_stake_percentage.sh >> /home/smilax/log/999_mainnet_stake_percentage.log 2>&1
*/5 * * * * /home/smilax/trillium_api/scripts/bash/major_minor_version.sh >> /home/smilax/log/major_minor_version.log 2>&1
```

3. **Create log directory**:
```bash
mkdir -p ~/log
```

### Configuration Requirements

**Before enabling cron jobs, configure:**

1. **rclone for Google Drive** (required for `999_mount_gdrive.sh`):
```bash
rclone config  # Configure gdrive remote
```

2. **Validator pubkeys** (update in monitoring scripts as needed):
   - MEV monitoring script
   - Stake percentage scripts

3. **Notification webhooks** (already configured in production template):
   - Discord webhooks are pre-configured
   - Update Telegram/PagerDuty settings if needed

### Monitoring Script Features

✅ **Standardized logging** with unified log format  
✅ **Error handling** with Discord notifications  
✅ **Automatic cleanup** of old files/logs  
✅ **Health checks** and stale connection detection  
✅ **Graceful failure handling** with retry logic  
✅ **Production-ready configuration** deployed  

## Next Steps

After successful installation:

1. Run setup script: `./scripts/bash/setup_production_environment.sh`
2. Run a test epoch processing: `./scripts/bash/0_process_getslots_data.sh 825`
3. Enable cron jobs for automated monitoring
4. Monitor logs during first execution
5. Set up reverse proxy for web API endpoints (if applicable)

## Backup and Recovery

### Database Backup

```bash
# Create backup
pg_dump -h localhost -U trillium_user solana_data > backup_$(date +%Y%m%d).sql

# Restore backup
psql -h localhost -U trillium_user solana_data < backup_20241201.sql
```

### Configuration Backup

```bash
# Backup configuration
tar -czf config_backup_$(date +%Y%m%d).tar.gz config/ scripts/bash/999_discord_notify.sh
```

## Security Considerations

- Use strong database passwords
- Restrict database access to localhost only
- Keep API keys secure and rotate regularly
- Monitor log files for sensitive information
- Regular security updates for system packages
- Consider using SSL/TLS for database connections in production