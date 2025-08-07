# API Usage and Examples Guide

**Comprehensive guide for using the Trillium API data processing pipeline**

## Overview

This guide provides practical examples for running the Trillium API pipeline, from basic epoch processing to advanced monitoring and analysis tasks.

## Core Processing Commands

### Basic Epoch Processing

```bash
# Process the current epoch automatically
./scripts/bash/00_process_all_automated.sh

# Process a specific epoch (e.g., epoch 825)
./scripts/bash/00_process_all_automated.sh 825

# Process epoch data collection only
./scripts/bash/0_process_getslots_data.sh 825
```

### Individual Processing Stages

```bash
# Stage 1: Data Collection
./scripts/bash/90_xshin_load_data.sh                    # Fetch Xshin validator data
./scripts/bash/90_get_leader_schedule.sh                # Get leader schedules
python3 scripts/python/90_stakewiz_validators.py       # Collect Stakewiz data

# Stage 2: Data Processing  
./scripts/bash/1_load_consolidated_csv.sh 825           # Load CSV data
./scripts/bash/1_wait-for-jito-process_data.sh 825     # Process with Jito data

# Stage 3: Analysis & Aggregation
./scripts/bash/2_update_validator_aggregate_info.sh 825 # Main aggregation
python3 scripts/python/92_calculate_apy.py             # Calculate APY
python3 scripts/python/92_slot_duration_statistics.py  # Analyze block timing

# Stage 4: Output Generation
./scripts/bash/3_build_leaderboard_json.sh 825         # Generate leaderboards
python3 scripts/python/93_solana_stakes_export.py     # Export stake data

# Stage 5: Publishing
./scripts/bash/4_move_json_to_production.sh 825        # Deploy to web
./scripts/bash/5_cp_images.sh                          # Copy validator images
```

## Database Operations

### Direct Database Queries

```bash
# Connect to database
psql -h localhost -U smilax sol_blocks

# View validator performance for current epoch
SELECT 
    identity_pubkey,
    name,
    vote_credits,
    blocks_produced,
    stake_amount
FROM validator_stats 
WHERE epoch = 829 
ORDER BY vote_credits DESC 
LIMIT 10;

# Check stake distribution
SELECT 
    version,
    SUM(stake) as total_stake,
    COUNT(*) as validator_count
FROM validator_data 
GROUP BY version 
ORDER BY total_stake DESC;
```

### Database Maintenance

```bash
# Manual backup
./scripts/bash/999_backup_psql_all_tables.sh

# Check database size
psql -h localhost -U smilax sol_blocks -c "
SELECT 
    schemaname,
    tablename,
    pg_size_pretty(pg_total_relation_size(tablename::regclass)) as size
FROM pg_tables 
WHERE schemaname = 'public'
ORDER BY pg_total_relation_size(tablename::regclass) DESC;"
```

## Configuration Management

### Loading Configuration Values

```bash
# Source the config loader
source ./scripts/bash/999_config_loader.sh

# Load Discord webhook
WEBHOOK=$(get_discord_webhook "mev_monitoring")
echo "MEV monitoring webhook: $WEBHOOK"

# Load RPC endpoint
RPC_URL=$(get_rpc_url "mainnet" "primary")
echo "Primary RPC: $RPC_URL"

# Load validator identity
VALIDATOR_PUBKEY=$(get_validator_identity "trillium")
echo "Trillium pubkey: $VALIDATOR_PUBKEY"

# Load all MEV monitoring parameters
MEV_PARAMS=$(get_mev_parameters)
echo "MEV monitoring validators: $MEV_PARAMS"
```

### Configuration File Examples

```bash
# Check current configuration status
cd data/configs
ls -la *.json

# Deploy all configuration templates
for template in *.template; do 
    cp "$template" "${template%.template}"
done

# Test configuration loading
./scripts/bash/999_config_loader.sh
```

## Monitoring and Alerts

### Manual Monitoring Runs

```bash
# Check SOL price and send alert if changed significantly
./scripts/bash/999_monitor_sol_price.sh

# Check epoch progress and send milestone notifications
./scripts/bash/999_epoch_discord_notification.sh

# Monitor validators for null MEV
./scripts/bash/999_check_all_for_null_mev.sh

# Check network stake percentages
./scripts/bash/999_mainnet_stake_percentage.sh
./scripts/bash/999_testnet_stake_percentage.sh

# Monitor SFDP version compliance
./scripts/bash/999_monitor_version.sh
```

### Setting Up Automated Monitoring

```bash
# Install the complete production crontab
crontab -e

# Add these entries for full production monitoring:
# Every 5 minutes - Price monitoring
*/5 * * * * /home/smilax/trillium_api/scripts/bash/999_monitor_sol_price.sh >> /home/smilax/log/999_monitor_sol_price.log 2>&1

# Every 5 minutes - Epoch notifications  
*/5 * * * * /home/smilax/trillium_api/scripts/bash/999_epoch_discord_notification.sh >> /home/smilax/log/999_epoch_discord_notification.log 2>&1

# Every 5 minutes - MEV monitoring
*/5 * * * * /home/smilax/trillium_api/scripts/bash/999_check_all_for_null_mev.sh >> /home/smilax/log/999_check_all_for_null_mev.log 2>&1

# Daily at 6:00 AM - Leader schedules
0 6 * * * /home/smilax/trillium_api/scripts/bash/90_get_leader_schedule.sh >> /home/smilax/log/90_get_leader_schedule.log 2>&1

# Daily at 12:30 AM - Backups
30 0 * * * /home/smilax/trillium_api/scripts/bash/999_backup_psql_all_tables.sh >> /home/smilax/log/999_backup_psql_all_tables.log 2>&1
```

## Python Scripts Usage

### Validator Analysis

```bash
# Calculate APY for all validators
cd scripts/python
python3 92_calculate_apy.py

# Update validator aggregate information
python3 92_update_validator_aggregate_info.py

# Analyze slot duration statistics
python3 92_slot_duration_statistics.py

# Generate stake export
python3 93_solana_stakes_export.py mainnet
```

### Custom Analysis Examples

```python
#!/usr/bin/env python3
import sys
sys.path.append('../../config')
from db_config import db_params
import psycopg2
import pandas as pd

# Connect to database
conn = psycopg2.connect(**db_params)

# Get top 10 validators by vote credits
query = """
SELECT 
    v.name,
    v.identity_pubkey,
    vs.vote_credits,
    vs.blocks_produced,
    vs.stake_amount
FROM validator_stats vs
JOIN validator_data v ON vs.identity_pubkey = v.identity_pubkey  
WHERE vs.epoch = 829
ORDER BY vs.vote_credits DESC
LIMIT 10;
"""

df = pd.read_sql(query, conn)
print(df)

# Calculate validator performance metrics
performance_query = """
SELECT 
    epoch,
    AVG(vote_credits) as avg_vote_credits,
    AVG(blocks_produced) as avg_blocks_produced,
    COUNT(*) as total_validators
FROM validator_stats
WHERE epoch >= 820
GROUP BY epoch
ORDER BY epoch;
"""

performance_df = pd.read_sql(performance_query, conn)
print(performance_df)

conn.close()
```

## Data Analysis Workflows

### Epoch Comparison Analysis

```bash
# Compare validator performance across epochs
psql -h localhost -U smilax sol_blocks << EOF
WITH epoch_comparison AS (
    SELECT 
        identity_pubkey,
        epoch,
        vote_credits,
        blocks_produced,
        ROW_NUMBER() OVER (PARTITION BY identity_pubkey ORDER BY epoch DESC) as epoch_rank
    FROM validator_stats
    WHERE epoch IN (827, 828, 829)
)
SELECT 
    e1.identity_pubkey,
    e1.vote_credits as current_vote_credits,
    e2.vote_credits as previous_vote_credits,
    (e1.vote_credits - e2.vote_credits) as vote_credit_change
FROM epoch_comparison e1
JOIN epoch_comparison e2 ON e1.identity_pubkey = e2.identity_pubkey
WHERE e1.epoch = 829 AND e2.epoch = 828
ORDER BY vote_credit_change DESC
LIMIT 10;
EOF
```

### Stake Analysis Workflow

```bash
# Get current stake distribution
python3 << EOF
import sys
sys.path.append('config')
from db_config import db_params
import psycopg2

conn = psycopg2.connect(**db_params)
cursor = conn.cursor()

# Stake distribution by validator version
cursor.execute("""
    SELECT 
        version,
        SUM(stake) as total_stake,
        COUNT(*) as validator_count,
        SUM(stake) * 100.0 / (SELECT SUM(stake) FROM validator_data WHERE epoch = 829) as stake_percentage
    FROM validator_data
    WHERE epoch = 829
    GROUP BY version
    ORDER BY total_stake DESC;
""")

print("Stake Distribution by Version:")
print("Version\t\tStake (SOL)\tValidators\tStake %")
for row in cursor.fetchall():
    print(f"{row[0]:<15}\t{row[1]:>12.0f}\t{row[2]:>10}\t{row[3]:>7.2f}%")

conn.close()
EOF
```

### Geographic Analysis

```bash
# Analyze validator distribution by location
psql -h localhost -U smilax sol_blocks << EOF
SELECT 
    country,
    COUNT(*) as validator_count,
    SUM(stake) as total_stake,
    AVG(vote_credits) as avg_vote_credits
FROM validator_data vd
LEFT JOIN country_regions cr ON vd.country = cr.country
WHERE epoch = 829
GROUP BY country
HAVING COUNT(*) > 5
ORDER BY total_stake DESC;
EOF
```

## Troubleshooting Common Issues

### Database Connection Issues

```bash
# Test database connectivity
python3 -c "
import sys
sys.path.append('config')
from db_config import db_params
import psycopg2
try:
    conn = psycopg2.connect(**db_params)
    print('✅ Database connection successful')
    cursor = conn.cursor()
    cursor.execute('SELECT COUNT(*) FROM validator_data;')
    count = cursor.fetchone()[0]
    print(f'✅ Found {count} validator records')
    conn.close()
except Exception as e:
    print(f'❌ Database connection failed: {e}')
"

# Check .pgpass file
ls -la ~/.pgpass
cat ~/.pgpass
```

### API Connectivity Testing

```bash
# Test external API connections
echo "Testing Stakewiz API..."
curl -s "https://api.stakewiz.com/validators" | jq '. | length' || echo "❌ Stakewiz API unavailable"

echo "Testing Jito API..."
curl -s "https://kobe.mainnet.jito.network/api/v1/validators" | jq '. | length' || echo "❌ Jito API unavailable"

echo "Testing CoinMarketCap API..."
curl -s "https://api.coingecko.com/api/v3/simple/price?ids=solana&vs_currencies=usd" | jq '.solana.usd' || echo "❌ CoinGecko API unavailable"
```

### Log Analysis

```bash
# Check recent log entries
tail -f ~/log/00_process_all_automated.log

# Find errors in logs
grep -i error ~/log/*.log | tail -20

# Monitor specific script execution
tail -f ~/log/999_monitor_sol_price.log
```

### Performance Monitoring

```bash
# Check disk usage
df -h /home/smilax/trillium_api/data/

# Monitor database size
psql -h localhost -U smilax sol_blocks -c "
SELECT pg_size_pretty(pg_database_size('sol_blocks')) as database_size;"

# Check system resources
top -p $(pgrep -f "python3.*92_")
```

## Advanced Usage

### Custom Webhook Testing

```bash
# Test Discord webhook manually
source ./scripts/bash/999_config_loader.sh
WEBHOOK=$(get_discord_webhook "general_notifications")

curl -H "Content-Type: application/json" \
     -d '{
       "embeds": [{
         "title": "Test Notification",
         "description": "Testing Trillium API webhook integration",
         "color": 3447003,
         "timestamp": "'$(date -u +%Y-%m-%dT%H:%M:%S.%3NZ)'"
       }]
     }' \
     "$WEBHOOK"
```

### Custom Monitoring Script

```bash
#!/bin/bash
# Custom validator monitoring script

source /home/smilax/trillium_api/scripts/bash/999_common_log.sh
source /home/smilax/trillium_api/scripts/bash/999_config_loader.sh

init_logging

# Monitor specific validator
VALIDATOR_PUBKEY=$(get_validator_identity "trillium")
DISCORD_WEBHOOK=$(get_discord_webhook "general_notifications")

# Check validator status
VOTE_CREDITS=$(psql -h localhost -U smilax sol_blocks -t -c "
SELECT vote_credits 
FROM validator_stats 
WHERE identity_pubkey = '$VALIDATOR_PUBKEY' 
AND epoch = (SELECT MAX(epoch) FROM validator_stats);
")

log_info "Validator $VALIDATOR_PUBKEY has $VOTE_CREDITS vote credits"

# Send alert if vote credits are low
if [ "$VOTE_CREDITS" -lt 1000 ]; then
    log_warn "Low vote credits detected: $VOTE_CREDITS"
    # Send Discord notification
    curl -H "Content-Type: application/json" \
         -d "{\"content\": \"⚠️ Low vote credits for validator: $VOTE_CREDITS\"}" \
         "$DISCORD_WEBHOOK"
fi

cleanup_logging
```

## Integration Examples

### Grafana Dashboard Data

```sql
-- Query for Grafana time-series visualization
SELECT 
    epoch * 432000 as time_sec,  -- Convert epoch to approximate timestamp
    version,
    SUM(stake) as total_stake
FROM validator_data
WHERE epoch >= 820
GROUP BY epoch, version
ORDER BY epoch, total_stake DESC;
```

### Prometheus Metrics Export

```python
#!/usr/bin/env python3
# Export metrics in Prometheus format

import sys
sys.path.append('config')
from db_config import db_params
import psycopg2

conn = psycopg2.connect(**db_params)
cursor = conn.cursor()

# Get current epoch metrics
cursor.execute("""
    SELECT 
        COUNT(*) as total_validators,
        SUM(stake) as total_stake,
        AVG(vote_credits) as avg_vote_credits
    FROM validator_data
    WHERE epoch = (SELECT MAX(epoch) FROM validator_data);
""")

total_validators, total_stake, avg_vote_credits = cursor.fetchone()

# Output Prometheus metrics
print(f"# HELP solana_total_validators Total number of validators")
print(f"# TYPE solana_total_validators gauge")
print(f"solana_total_validators {total_validators}")

print(f"# HELP solana_total_stake Total stake amount")
print(f"# TYPE solana_total_stake gauge")  
print(f"solana_total_stake {total_stake}")

print(f"# HELP solana_avg_vote_credits Average vote credits")
print(f"# TYPE solana_avg_vote_credits gauge")
print(f"solana_avg_vote_credits {avg_vote_credits}")

conn.close()
```

---

This guide provides comprehensive examples for using the Trillium API system. For more specific use cases or troubleshooting, refer to the individual documentation files and examine the source code of the relevant scripts.