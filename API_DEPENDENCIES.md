# External API Dependencies and Binary Requirements

## External APIs

### 1. Jito Kobe API
- **Base URL**: `https://kobe.mainnet.jito.network/api/v1/`
- **Purpose**: MEV rewards, validator performance data, Jito-specific metrics
- **Key Endpoints**:
  - `/validators` - List all validators with Jito data
  - `/validators/{vote_account}` - Individual validator details
  - `/validators/{vote_account}/mev_rewards` - MEV reward history
- **Authentication**: Public API, no key required
- **Rate Limits**: Unknown, but should implement backoff/retry logic
- **Used By**: 
  - `1_wait-for-jito-process_data.sh`
  - `92-jito-steward-data-collection.py`
  - `92_update_validator_aggregate_info.py`

### 2. Stakewiz API
- **Base URL**: `https://api.stakewiz.com/`
- **Purpose**: Comprehensive validator scoring, geographic data, performance metrics
- **Key Endpoints**:
  - `/validators` - All validators with scoring data
  - `/validator/{identity}` - Individual validator details
- **Authentication**: Public API, no key required
- **Rate Limits**: Reasonable, includes retry logic in scripts
- **Data Format**: JSON with extensive validator metadata
- **Used By**:
  - `90_stakewiz_validators.py`

### 3. Xshin API
- **Base URL**: Various endpoints (configured in Node.js scripts)
- **Purpose**: Validator awards, rankings, special recognitions
- **Authentication**: May require API key (check Node.js scripts)
- **Used By**:
  - `90_xshin.js`
  - `90_xshin_load_data.py`

### 4. Solana RPC Endpoints
- **Purpose**: Direct blockchain data access
- **Endpoints**: Various public and private RPC nodes
- **Key Methods**:
  - `getEpochInfo`
  - `getBlockProduction`
  - `getVoteAccounts`
  - `getBlock`
  - `getSlot`
- **Used By**:
  - Most Python scripts for direct blockchain queries
  - `rpc_get_block_data.sh`

### 5. CoinGecko API
- **Base URL**: `https://api.coingecko.com/api/v3/`
- **Purpose**: SOL price data for APY calculations
- **Key Endpoints**:
  - `/simple/price?ids=solana&vs_currencies=usd`
- **Authentication**: Free tier, no key required
- **Used By**:
  - `92_calculate_apy.py`
  - Price monitoring scripts

### 6. GitHub API
- **Base URL**: `https://api.github.com/`
- **Purpose**: Version checking for Solana validator clients
- **Key Endpoints**:
  - `/repos/solana-labs/solana/releases/latest`
  - `/repos/jito-foundation/jito-solana/releases/latest`
- **Authentication**: Public API, but consider rate limits
- **Used By**:
  - Version monitoring and client analysis scripts

### 7. Dune Analytics API
- **Base URL**: `https://api.dune.com/api/v1/`
- **Purpose**: Additional blockchain analytics data
- **Authentication**: Requires API key
- **Used By**: Some analysis scripts (optional)

## Required Binary Dependencies

### Core System Binaries

#### PostgreSQL Client
- **Binary**: `psql`
- **Installation**: `sudo apt install postgresql-client`
- **Purpose**: Database operations, SQL script execution
- **Version**: 12+ recommended
- **Used By**: Most database-related scripts

#### Python 3
- **Binary**: `python3`
- **Installation**: `sudo apt install python3 python3-pip`
- **Purpose**: Core data processing and analysis
- **Version**: 3.8+ required
- **Used By**: All `.py` scripts

#### Node.js
- **Binary**: `node`, `npm`
- **Installation**: `sudo apt install nodejs npm`
- **Purpose**: API data fetching, JavaScript-based processing
- **Version**: 16+ recommended
- **Used By**: `90_xshin.js` and related scripts

#### Curl
- **Binary**: `curl`
- **Installation**: `sudo apt install curl`
- **Purpose**: HTTP requests, API calls, file downloads
- **Used By**: Most scripts for API communication

#### jq
- **Binary**: `jq`
- **Installation**: `sudo apt install jq`
- **Purpose**: JSON parsing and manipulation in bash scripts
- **Used By**: Scripts processing JSON API responses

#### tmux
- **Binary**: `tmux`
- **Installation**: `sudo apt install tmux`
- **Purpose**: Background process management, session persistence
- **Used By**: `0_process_getslots_data.sh` for Jito processing

### Compression Tools

#### Zstandard
- **Binary**: `zstd`
- **Installation**: `sudo apt install zstd`
- **Purpose**: Compress/decompress epoch data archives
- **Used By**: `90_untar_epoch.sh`, archive processing scripts

#### Gzip
- **Binary**: `gzip`, `gunzip`
- **Installation**: Usually pre-installed
- **Purpose**: Log file compression, legacy archive handling
- **Used By**: Log rotation, cleanup scripts

#### Tar
- **Binary**: `tar`
- **Installation**: Usually pre-installed
- **Purpose**: Archive creation and extraction
- **Used By**: `tar_files.sh`, `copy_tar.sh`

### Mathematical Tools

#### bc (Basic Calculator)
- **Binary**: `bc`
- **Installation**: `sudo apt install bc`
- **Purpose**: Floating-point arithmetic in bash scripts
- **Used By**: Timing calculations, mathematical operations in scripts

### Specialized Solana Tools

#### Jito Validator History CLI
- **Binary**: `validator-history`
- **Installation**: Download from https://github.com/jito-foundation/validator-history/releases
- **Purpose**: Fetch historical Jito validator data, MEV rewards
- **Version**: Latest stable release
- **Installation Steps**:
  ```bash
  cd /tmp
  wget https://github.com/jito-foundation/validator-history/releases/latest/download/validator-history-linux-x86_64.tar.gz
  tar -xzf validator-history-linux-x86_64.tar.gz
  sudo mv validator-history /usr/local/bin/
  sudo chmod +x /usr/local/bin/validator-history
  ```
- **Used By**: 
  - `1_wait-for-jito-process_data.sh`
  - Jito data collection scripts

#### Solana CLI (Optional)
- **Binary**: `solana`
- **Installation**: `sh -c "$(curl -sSfL https://release.solana.com/stable/install)"`
- **Purpose**: Direct Solana blockchain interaction, RPC calls
- **Version**: Latest stable
- **Used By**: Some optional blockchain interaction scripts

### Monitoring and Notification Tools

#### Discord/Telegram Bots
- **Purpose**: Real-time notifications and alerting
- **Requirements**: 
  - Discord webhook URL
  - Telegram bot token (if using Telegram notifications)
- **Configuration**: Set webhook URLs in notification scripts

## Network Requirements

### Firewall Configuration
```bash
# Allow outbound HTTPS for API calls
sudo ufw allow out 443
sudo ufw allow out 80

# Allow PostgreSQL (localhost only)
sudo ufw allow from 127.0.0.1 to any port 5432
```

### DNS Resolution
Ensure these domains are accessible:
- `kobe.mainnet.jito.network`
- `api.stakewiz.com`
- `api.coingecko.com`
- `api.github.com`
- `discord.com` (for notifications)

### Network Performance
- Stable internet connection required
- Bandwidth: ~100MB per epoch for full data collection
- Latency: <500ms to major APIs for optimal performance

## Environment Variables

### Required
```bash
export LOG_BASE_DIR="$HOME/log"
export LOG_LEVEL="INFO"
```

### Optional
```bash
export STAKEWIZ_API_KEY="your_key_if_required"
export JITO_API_KEY="your_key_if_required"
export DUNE_API_KEY="your_dune_key"
export DISCORD_WEBHOOK_URL="your_webhook_url"
export TELEGRAM_BOT_TOKEN="your_bot_token"
```

## Dependency Health Checks

### API Availability Check Script
```bash
#!/bin/bash
# Check all external APIs

echo "Testing API endpoints..."

# Jito Kobe API
curl -sf "https://kobe.mainnet.jito.network/api/v1/validators" > /dev/null && echo "✅ Jito Kobe API" || echo "❌ Jito Kobe API"

# Stakewiz API  
curl -sf "https://api.stakewiz.com/validators" > /dev/null && echo "✅ Stakewiz API" || echo "❌ Stakewiz API"

# CoinGecko API
curl -sf "https://api.coingecko.com/api/v3/simple/price?ids=solana&vs_currencies=usd" > /dev/null && echo "✅ CoinGecko API" || echo "❌ CoinGecko API"

# GitHub API
curl -sf "https://api.github.com/repos/solana-labs/solana/releases/latest" > /dev/null && echo "✅ GitHub API" || echo "❌ GitHub API"
```

### Binary Check Script
```bash
#!/bin/bash
# Check all required binaries

for binary in psql python3 node curl jq tmux zstd bc validator-history; do
    if command -v $binary > /dev/null 2>&1; then
        echo "✅ $binary"
    else
        echo "❌ $binary - not found"
    fi
done
```

## Rate Limiting and Error Handling

### Retry Logic
Most scripts implement exponential backoff for API failures:
```bash
retry_count=0
max_retries=3
while [ $retry_count -lt $max_retries ]; do
    if api_call; then
        break
    else
        retry_count=$((retry_count + 1))
        sleep $((retry_count * 30))
    fi
done
```

### Circuit Breaker Pattern
For critical APIs, implement circuit breaker to prevent cascade failures:
- Open circuit after 5 consecutive failures
- Half-open after 5 minutes
- Close circuit after successful request

## Monitoring External Dependencies

### API Response Time Monitoring
- Track response times for each API
- Alert when response times exceed thresholds
- Log API availability metrics

### Dependency Version Tracking
- Monitor for updates to external APIs
- Track changes in response schemas
- Maintain compatibility matrices

## Backup Strategies

### API Data Caching
- Cache API responses locally when possible
- Implement cache invalidation strategies
- Use cached data during API outages

### Offline Mode
- Gracefully degrade functionality when APIs unavailable
- Process local data only during extended outages
- Queue requests for replay when APIs return

## Security Considerations

### API Key Management
- Store API keys securely (environment variables, not code)
- Rotate keys regularly
- Monitor for key usage anomalies

### Network Security
- Use HTTPS for all API communications
- Validate SSL certificates
- Implement request signing where supported
- Monitor for suspicious network activity