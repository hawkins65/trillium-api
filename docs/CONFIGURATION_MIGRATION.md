# Configuration Migration Guide

This guide helps you migrate from hardcoded values to the centralized configuration system.

## Overview of Changes

### 1. Centralized Configuration Files
All external URLs, webhooks, and identities are now managed through JSON configuration files in `/home/smilax/trillium_api/data/configs/`.

### 2. Key Changes Made

#### MEV Monitoring Script (`999_check_all_for_null_mev.sh`)
- **Before**: Generic placeholder for validator lists
- **After**: Pre-configured with actual validator identities:
  - trillium, ofv, laine, cogent, ss, pengu
  - Uses original logic from `/home/smilax/block-production/api/check-all-for-null-mev.sh`

#### Stake Percentage Scripts
- **Before**: Individual validator tracking logic
- **After**: Network-wide analysis matching original scripts
  - Calls Python `stake-percentage.py` script
  - Publishes results to web
  - No specific validator tracking

#### Discord Webhooks
- **Before**: Hardcoded in each script
- **After**: Centralized in `notification_webhooks.json`
  - 10 different webhook URLs for specific channels
  - Single point of update

## Migration Steps

### 1. Deploy Configuration Files

```bash
cd ~/trillium_api/data/configs

# Deploy all templates
for template in *.template; do 
    cp "$template" "${template%.template}"
done
```

### 2. Update Existing Scripts

If you have custom scripts, update them to use the configuration loader:

```bash
# Old approach (hardcoded)
DISCORD_WEBHOOK="https://discord.com/api/webhooks/1234567890/abcdef"

# New approach (centralized)
source /home/smilax/trillium_api/scripts/bash/999_config_loader.sh
DISCORD_WEBHOOK=$(get_discord_webhook "mev_monitoring")
```

### 3. Configuration File Reference

#### notification_webhooks.json
```json
{
  "discord": {
    "general_notifications": "webhook_url",
    "public_notifications": "webhook_url",
    "sol_price_monitor": "webhook_url",
    "version_monitor": "webhook_url",
    "slot_progression": "webhook_url",
    "wss_monitoring": "webhook_url",
    "epoch_notifications": "webhook_url",
    "mev_monitoring": "webhook_url",
    "mainnet_stake": "webhook_url",
    "testnet_stake": "webhook_url"
  }
}
```

#### urls_and_endpoints.json
```json
{
  "solana_rpc": {
    "mainnet_primary": "quicknode_url",
    "mainnet_secondary": "quicknode_url",
    "mainnet_public": "https://api.mainnet-beta.solana.com",
    "testnet_primary": "quicknode_url",
    "testnet_public": "https://api.testnet.solana.com"
  },
  "external_apis": {
    "trillium_validator_rewards": "url",
    "stakewiz_validators": "url",
    // ... more APIs
  }
}
```

#### validator_identities.json
```json
{
  "validator_groups": {
    "trillium_and_partners": {
      "trillium": "Tri1F8B6YtjkBztGCwBNSLEZib1EAqMUEUM7dTT7ZG3",
      "ofv": "DB7DNWMVQASMFxcjkwdr4w4eg3NmfjWTk2rqFMMbrPLA",
      // ... more validators
    }
  },
  "mev_monitoring_parameters": [
    "trillium", "ofv", "laine", "cogent", "ss", "pengu"
  ]
}
```

## Using the Configuration Loader

### Available Functions

```bash
# Load Discord webhook by type
get_discord_webhook "webhook_type"

# Load RPC URL
get_rpc_url "network" "type"  # e.g., "mainnet" "primary"
                              # Now includes "mainnet" "alchemy" (production endpoint)

# Load external API URL
get_api_url "api_name"

# Load validator identity pubkey
get_validator_identity "validator_name"

# Load validator vote pubkey (NEW)
get_validator_vote_pubkey "validator_name"  # All 6 validators configured

# Get MEV monitoring parameters
get_mev_parameters

# Check service status
is_telegram_enabled
is_pagerduty_enabled

# Get service config
get_telegram_config "config_key"
get_pagerduty_config "config_key"
```

### New Features Added

1. **Telegram Integration** ✅ - Bot token (6487508986:AAHzmaPqTxl95g9S3CsQ6b0EQ5s20egM4yg) and chat ID (6375790507) configured
2. **PagerDuty Integration** ✅ - Mainnet (4ce4a8e4c8294705c03e342e6e3c475c) and testnet integration keys configured
3. **Alchemy RPC** ✅ - Primary RPC endpoint (97zE6cvElUYwOp_zVSqMXt5H7dYSYxtW) configured
4. **Vote Account Pubkeys** ✅ - All 6 validator vote pubkeys (trillium, ofv, laine, cogent, ss, pengu) configured
```

### Example Script Update

```bash
#!/bin/bash

# Source required files
source /home/smilax/trillium_api/scripts/bash/999_common_log.sh
source /home/smilax/trillium_api/scripts/bash/999_config_loader.sh

# Initialize
init_logging

# Load configurations
DISCORD_WEBHOOK=$(get_discord_webhook "mev_monitoring")
MAINNET_RPC=$(get_rpc_url "mainnet" "primary")
VALIDATOR_PUBKEY=$(get_validator_identity "trillium")

# Use in your script
log_info "Using RPC: $MAINNET_RPC"
log_info "Monitoring validator: $VALIDATOR_PUBKEY"

# Send Discord notification
curl -H "Content-Type: application/json" \
     -d "{\"content\": \"Alert message\"}" \
     "$DISCORD_WEBHOOK"
```

## Benefits of Migration

1. **Single Point of Updates**: Change webhooks/URLs in one place
2. **Environment Portability**: Easy to deploy to new servers
3. **No Hardcoded Secrets**: All sensitive data in config files
4. **Service Toggle**: Enable/disable Telegram/PagerDuty without code changes
5. **Validation**: Config loader validates files exist
6. **Consistency**: All scripts use same configuration source

## Troubleshooting

### Missing Configuration File
```bash
ERROR: Webhook configuration file not found: /home/smilax/trillium_api/data/configs/notification_webhooks.json
```
**Solution**: Deploy the configuration templates as shown above.

### Invalid Configuration Key
```bash
ERROR: Webhook type 'invalid_type' not found in configuration
```
**Solution**: Check available keys in the configuration file.

### Testing Configuration
```bash
# Test the configuration loader
/home/smilax/trillium_api/scripts/bash/999_config_loader.sh
```

## Future Enhancements

- Encrypted configuration for sensitive data
- Environment-specific configs (dev/staging/prod)
- Dynamic configuration reloading
- Web UI for configuration management
- Automated configuration backup