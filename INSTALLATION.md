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

```bash
# Update system
sudo apt update && sudo apt upgrade -y

# Install core dependencies
sudo apt install -y \
    postgresql postgresql-contrib \
    python3 python3-pip python3-venv \
    nodejs npm \
    curl jq tmux \
    zstd gzip tar \
    git build-essential

# Install PostgreSQL development headers
sudo apt install -y libpq-dev python3-dev
```

## Database Setup

### PostgreSQL Configuration

1. **Start PostgreSQL service**:
```bash
sudo systemctl start postgresql
sudo systemctl enable postgresql
```

2. **Create database and user**:
```bash
sudo -u postgres psql

-- In PostgreSQL prompt:
CREATE DATABASE solana_data;
CREATE USER trillium_user WITH ENCRYPTED PASSWORD 'your_secure_password';
GRANT ALL PRIVILEGES ON DATABASE solana_data TO trillium_user;
\q
```

3. **Configure database connection**:
Edit `config/db_config.py`:
```python
db_params = {
    'host': 'localhost',
    'database': 'solana_data',
    'user': 'trillium_user',
    'password': 'your_secure_password',
    'port': 5432
}
```

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

## External Binary Installation

### Jito Validator History CLI

1. **Download from GitHub releases**:
```bash
# Check latest release at: https://github.com/jito-foundation/validator-history
cd /tmp
wget https://github.com/jito-foundation/validator-history/releases/latest/download/validator-history-linux-x86_64.tar.gz
tar -xzf validator-history-linux-x86_64.tar.gz
sudo mv validator-history /usr/local/bin/
sudo chmod +x /usr/local/bin/validator-history
```

2. **Verify installation**:
```bash
validator-history --version
```

### Solana CLI (Optional but Recommended)

```bash
sh -c "$(curl -sSfL https://release.solana.com/stable/install)"
export PATH="$HOME/.local/share/solana/install/active_release/bin:$PATH"
echo 'export PATH="$HOME/.local/share/solana/install/active_release/bin:$PATH"' >> ~/.bashrc
```

## Directory Structure Setup

### Create Required Directories

```bash
mkdir -p ~/log/get_slots
mkdir -p ~/api
mkdir -p ~/.config/solana
mkdir -p ~/block-production/get_slots
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

### Discord Webhook (Optional)

Edit webhook URL in `scripts/bash/999_discord_notify.sh`:
```bash
DISCORD_WEBHOOK_URL="your_discord_webhook_url_here"
```

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

## Next Steps

After successful installation:

1. Run a test epoch processing: `./scripts/bash/0_process_getslots_data.sh 825`
2. Monitor logs during first execution
3. Set up monitoring and alerting
4. Configure automated backups for database and critical data
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