#!/bin/bash

# Source path initialization
source "$(dirname "$0")/000_init_paths.sh" || {
    echo "❌ Failed to source path initialization script" >&2
    exit 1
}

# Configuration Loader Functions
# Provides centralized configuration loading for all monitoring scripts

# Configuration file paths
readonly CONFIG_DIR="/home/smilax/trillium_api/data/configs"
readonly WEBHOOKS_CONFIG="$CONFIG_DIR/notification_webhooks.json"
readonly URLS_CONFIG="$CONFIG_DIR/urls_and_endpoints.json"
readonly VALIDATOR_CONFIG="$CONFIG_DIR/validator_identities.json"
readonly TELEGRAM_CONFIG="$CONFIG_DIR/telegram_config.json"
readonly PAGERDUTY_CONFIG="$CONFIG_DIR/pagerduty_config.json"

# Function to load Discord webhook URL by type
get_discord_webhook() {
    local webhook_type="$1"
    
    if [ ! -f "$WEBHOOKS_CONFIG" ]; then
        echo "ERROR: Webhook configuration file not found: $WEBHOOKS_CONFIG" >&2
        return 1
    fi
    
    local webhook_url=$(jq -r ".discord.${webhook_type} // empty" "$WEBHOOKS_CONFIG" 2>/dev/null)
    
    if [ -z "$webhook_url" ] || [ "$webhook_url" = "null" ]; then
        echo "ERROR: Webhook type '${webhook_type}' not found in configuration" >&2
        return 1
    fi
    
    echo "$webhook_url"
}

# Function to load RPC URL by type and network
get_rpc_url() {
    local network="$1"  # mainnet/testnet
    local type="$2"     # primary/secondary/public
    
    if [ ! -f "$URLS_CONFIG" ]; then
        echo "ERROR: URLs configuration file not found: $URLS_CONFIG" >&2
        return 1
    fi
    
    local rpc_url=$(jq -r ".solana_rpc.${network}_${type} // empty" "$URLS_CONFIG" 2>/dev/null)
    
    if [ -z "$rpc_url" ] || [ "$rpc_url" = "null" ]; then
        echo "ERROR: RPC URL for '${network}_${type}' not found in configuration" >&2
        return 1
    fi
    
    echo "$rpc_url"
}

# Function to load external API URL by name
get_api_url() {
    local api_name="$1"
    
    if [ ! -f "$URLS_CONFIG" ]; then
        echo "ERROR: URLs configuration file not found: $URLS_CONFIG" >&2
        return 1
    fi
    
    local api_url=$(jq -r ".external_apis.${api_name} // empty" "$URLS_CONFIG" 2>/dev/null)
    
    if [ -z "$api_url" ] || [ "$api_url" = "null" ]; then
        echo "ERROR: API URL for '${api_name}' not found in configuration" >&2
        return 1
    fi
    
    echo "$api_url"
}

# Function to load validator identity by name
get_validator_identity() {
    local validator_name="$1"
    
    if [ ! -f "$VALIDATOR_CONFIG" ]; then
        echo "ERROR: Validator configuration file not found: $VALIDATOR_CONFIG" >&2
        return 1
    fi
    
    # First try the new structure
    local validator_pubkey=$(jq -r ".identity_pubkeys.trillium_and_partners.${validator_name} // empty" "$VALIDATOR_CONFIG" 2>/dev/null)
    
    # Fallback to old structure for compatibility
    if [ -z "$validator_pubkey" ] || [ "$validator_pubkey" = "null" ]; then
        validator_pubkey=$(jq -r ".validator_groups.trillium_and_partners.${validator_name} // empty" "$VALIDATOR_CONFIG" 2>/dev/null)
    fi
    
    if [ -z "$validator_pubkey" ] || [ "$validator_pubkey" = "null" ]; then
        echo "ERROR: Validator '${validator_name}' not found in configuration" >&2
        return 1
    fi
    
    echo "$validator_pubkey"
}

# Function to load validator vote pubkey by name
get_validator_vote_pubkey() {
    local validator_name="$1"
    
    if [ ! -f "$VALIDATOR_CONFIG" ]; then
        echo "ERROR: Validator configuration file not found: $VALIDATOR_CONFIG" >&2
        return 1
    fi
    
    local vote_pubkey=$(jq -r ".vote_pubkeys.${validator_name} // empty" "$VALIDATOR_CONFIG" 2>/dev/null)
    
    if [ -z "$vote_pubkey" ] || [ "$vote_pubkey" = "null" ]; then
        echo "ERROR: Vote pubkey for '${validator_name}' not found in configuration" >&2
        return 1
    fi
    
    echo "$vote_pubkey"
}

# Function to load all MEV monitoring parameters
get_mev_parameters() {
    if [ ! -f "$VALIDATOR_CONFIG" ]; then
        echo "ERROR: Validator configuration file not found: $VALIDATOR_CONFIG" >&2
        return 1
    fi
    
    jq -r '.mev_monitoring_parameters[]' "$VALIDATOR_CONFIG" 2>/dev/null
}

# Function to check if Telegram is enabled
is_telegram_enabled() {
    if [ ! -f "$TELEGRAM_CONFIG" ]; then
        return 1
    fi
    
    local enabled=$(jq -r '.telegram.enabled // false' "$TELEGRAM_CONFIG" 2>/dev/null)
    [ "$enabled" = "true" ]
}

# Function to get Telegram configuration
get_telegram_config() {
    local config_key="$1"
    
    if [ ! -f "$TELEGRAM_CONFIG" ]; then
        echo "ERROR: Telegram configuration file not found: $TELEGRAM_CONFIG" >&2
        return 1
    fi
    
    jq -r ".telegram.${config_key} // empty" "$TELEGRAM_CONFIG" 2>/dev/null
}

# Function to check if PagerDuty is enabled
is_pagerduty_enabled() {
    if [ ! -f "$PAGERDUTY_CONFIG" ]; then
        return 1
    fi
    
    local enabled=$(jq -r '.pagerduty.enabled // false' "$PAGERDUTY_CONFIG" 2>/dev/null)
    [ "$enabled" = "true" ]
}

# Function to get PagerDuty configuration
get_pagerduty_config() {
    local config_key="$1"
    
    if [ ! -f "$PAGERDUTY_CONFIG" ]; then
        echo "ERROR: PagerDuty configuration file not found: $PAGERDUTY_CONFIG" >&2
        return 1
    fi
    
    jq -r ".pagerduty.${config_key} // empty" "$PAGERDUTY_CONFIG" 2>/dev/null
}

# Function to validate all configuration files exist
validate_configs() {
    local missing_configs=()
    
    [ ! -f "$WEBHOOKS_CONFIG" ] && missing_configs+=("notification_webhooks.json")
    [ ! -f "$URLS_CONFIG" ] && missing_configs+=("urls_and_endpoints.json") 
    [ ! -f "$VALIDATOR_CONFIG" ] && missing_configs+=("validator_identities.json")
    
    if [ ${#missing_configs[@]} -gt 0 ]; then
        echo "ERROR: Missing configuration files: ${missing_configs[*]}" >&2
        echo "Please ensure all configuration templates are deployed." >&2
        return 1
    fi
    
    return 0
}

# Example usage functions (for testing)
test_config_loader() {
    echo "Testing configuration loader..."
    
    echo "Discord webhooks:"
    echo "  General: $(get_discord_webhook general_notifications)"
    echo "  Price: $(get_discord_webhook sol_price_monitor)"
    
    echo "RPC URLs:"
    echo "  Mainnet Primary: $(get_rpc_url mainnet primary)"
    echo "  Testnet Public: $(get_rpc_url testnet public)"
    
    echo "API URLs:"
    echo "  Trillium Rewards: $(get_api_url trillium_validator_rewards)"
    
    echo "Validator Identities:"
    echo "  Trillium: $(get_validator_identity trillium)"
    echo "  SuperStake: $(get_validator_identity ss)"
    
    echo "MEV Parameters:"
    get_mev_parameters
}

# If script is run directly, run tests
if [[ "${BASH_SOURCE[0]}" == "${0}" ]]; then
    test_config_loader
fi