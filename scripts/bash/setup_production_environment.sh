#!/bin/bash

# Source path initialization
source "$(dirname "$0")/000_init_paths.sh" || {
    echo "❌ Failed to source path initialization script" >&2
    exit 1
}
# setup_production_environment.sh
# 
# Comprehensive setup and validation script for Trillium Solana Data Processing Pipeline
# This script ensures all dependencies are installed, configurations are set up,
# and file permissions are correctly configured for production deployment.
#
# Usage: ./setup_production_environment.sh [--check-only]
#        --check-only: Only validate environment without making changes

set -euo pipefail

# Script configuration
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
LOG_FILE="$PROJECT_ROOT/setup_environment.log"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Configuration
CHECK_ONLY=false
if [[ "${1:-}" == "--check-only" ]]; then
    CHECK_ONLY=true
fi

# Logging function
log() {
    local level="$1"
    shift
    local timestamp=$(date '+%Y-%m-%d %H:%M:%S')
    echo "[$timestamp] [$level] $*" | tee -a "$LOG_FILE"
}

# Print colored output
print_status() {
    local status="$1"
    local message="$2"
    case "$status" in
        "OK")    echo -e "${GREEN}✅ $message${NC}" ;;
        "WARN")  echo -e "${YELLOW}⚠️  $message${NC}" ;;
        "ERROR") echo -e "${RED}❌ $message${NC}" ;;
        "INFO")  echo -e "${BLUE}ℹ️  $message${NC}" ;;
    esac
}

# Check if command exists
command_exists() {
    command -v "$1" >/dev/null 2>&1
}

# Check if running as root
check_user() {
    if [[ $EUID -eq 0 ]]; then
        print_status "ERROR" "This script should not be run as root for security reasons"
        exit 1
    fi
    print_status "OK" "Running as non-root user: $(whoami)"
}

# Validate and optionally install system dependencies
check_system_dependencies() {
    print_status "INFO" "Checking system dependencies..."
    
    local missing_deps=()
    local system_deps=(
        "psql:postgresql-client"
        "python3:python3"
        "pip3:python3-pip"
        "node:nodejs"
        "npm:npm"
        "curl:curl"
        "jq:jq"
        "tmux:tmux"
        "zstd:zstd"
        "tar:tar"
        "gzip:gzip"
        "bc:bc"
        "git:git"
        "build-essential:build-essential"
        "libpq-dev:libpq-dev"
        "python3-dev:python3-dev"
        "python3-venv:python3-venv"
    )
    
    for dep_info in "${system_deps[@]}"; do
        local cmd="${dep_info%%:*}"
        local package="${dep_info##*:}"
        
        # Special handling for some packages that don't have direct commands
        case "$package" in
            "build-essential"|"libpq-dev"|"python3-dev"|"python3-venv")
                if dpkg-query -W -f='${Status}' "$package" 2>/dev/null | grep -q "install ok installed"; then
                    print_status "OK" "$package is installed"
                else
                    print_status "WARN" "$package is missing"
                    missing_deps+=("$package")
                fi
                ;;
            *)
                if command_exists "$cmd"; then
                    print_status "OK" "$cmd is installed"
                else
                    print_status "WARN" "$cmd is missing"
                    missing_deps+=("$package")
                fi
                ;;
        esac
    done
    
    if [[ ${#missing_deps[@]} -gt 0 ]]; then
        print_status "WARN" "Missing system dependencies: ${missing_deps[*]}"
        
        if [[ "$CHECK_ONLY" == "false" ]]; then
            print_status "INFO" "Attempting to install missing system dependencies..."
            echo "The following packages will be installed: ${missing_deps[*]}"
            echo "This requires sudo privileges."
            
            if sudo apt update && sudo apt install -y "${missing_deps[@]}"; then
                print_status "OK" "Successfully installed missing system dependencies"
                
                # Verify installation
                local failed_installs=()
                for dep_info in "${system_deps[@]}"; do
                    local cmd="${dep_info%%:*}"
                    local package="${dep_info##*:}"
                    
                    case "$package" in
                        "build-essential"|"libpq-dev"|"python3-dev"|"python3-venv")
                            if ! dpkg-query -W -f='${Status}' "$package" 2>/dev/null | grep -q "install ok installed"; then
                                failed_installs+=("$package")
                            fi
                            ;;
                        *)
                            if ! command_exists "$cmd"; then
                                failed_installs+=("$package")
                            fi
                            ;;
                    esac
                done
                
                if [[ ${#failed_installs[@]} -gt 0 ]]; then
                    print_status "ERROR" "Failed to install: ${failed_installs[*]}"
                    return 1
                fi
            else
                print_status "ERROR" "Failed to install system dependencies"
                echo "Please run manually: sudo apt update && sudo apt install ${missing_deps[*]}"
                return 1
            fi
        else
            print_status "ERROR" "Missing system dependencies. Install with:"
            echo "sudo apt update && sudo apt install ${missing_deps[*]}"
            return 1
        fi
    fi
    
    return 0
}

# Check Solana CLI installation
check_solana_cli() {
    print_status "INFO" "Checking Solana CLI..."
    
    local solana_paths=(
        "/home/$(whoami)/agave/bin/solana"
        "/home/smilax/agave/bin/solana"
        "$(which solana 2>/dev/null || echo "")"
    )
    
    for solana_path in "${solana_paths[@]}"; do
        if [[ -x "$solana_path" ]]; then
            print_status "OK" "Solana CLI found: $solana_path"
            local version=$("$solana_path" --version 2>/dev/null || echo "unknown")
            print_status "INFO" "Solana version: $version"
            return 0
        fi
    done
    
    print_status "ERROR" "Solana CLI not found."
    echo "Please follow the official Agave installation instructions at:"
    echo "https://github.com/anza-xyz/agave/blob/master/docs/src/cli/install.md#build-from-source"
    echo ""
    echo "After installation, ensure the binary is available at: ~/agave/bin/solana"
    echo "Add to your PATH: export PATH=\"\$HOME/agave/bin:\$PATH\""
    return 1
}

# Check validator-history CLI
check_validator_history() {
    print_status "INFO" "Checking validator-history CLI..."
    
    local validator_history_paths=(
        "/usr/local/bin/validator-history"
        "/home/$(whoami)/stakenet/target/release/validator-history-cli"
        "/home/smilax/stakenet/target/release/validator-history-cli"
        "$(which validator-history 2>/dev/null || echo "")"
        "$(which validator-history-cli 2>/dev/null || echo "")"
    )
    
    for vh_path in "${validator_history_paths[@]}"; do
        if [[ -x "$vh_path" ]]; then
            print_status "OK" "validator-history CLI found: $vh_path"
            local version=$("$vh_path" --version 2>/dev/null || echo "unknown")
            print_status "INFO" "validator-history version: $version"
            return 0
        fi
    done
    
    print_status "ERROR" "validator-history CLI not found."
    echo "Please follow the official stakenet installation instructions at:"
    echo "https://github.com/jito-foundation/stakenet/blob/master/README.md"
    echo ""
    echo "After installation, ensure the binary is available at one of these locations:"
    echo "  - /usr/local/bin/validator-history"
    echo "  - ~/stakenet/target/release/validator-history-cli"
    return 1
}

# Check PostgreSQL connection and .pgpass
check_postgresql() {
    print_status "INFO" "Checking PostgreSQL configuration..."
    
    # Check if .pgpass exists
    local pgpass_file="$HOME/.pgpass"
    if [[ ! -f "$pgpass_file" ]]; then
        print_status "ERROR" ".pgpass file not found at $pgpass_file"
        echo ""
        echo "PostgreSQL Database Setup Required:"
        echo "1. Use an AI tool (ChatGPT/Claude) to help you install:"
        echo "   - PostgreSQL v16 server with v17 client"
        echo "   - Create 'sol_blocks' database"
        echo "   - Create 'smilax' user with appropriate permissions"
        echo ""
        echo "2. After database setup, create .pgpass file:"
        echo "   echo 'localhost:5432:sol_blocks:smilax:your_password' > ~/.pgpass"
        echo "   chmod 0600 ~/.pgpass"
        echo ""
        echo "Example AI prompt:"
        echo "'Help me install PostgreSQL v16 server with v17 client on Ubuntu."
        echo "Create database named sol_blocks and user named smilax with full"
        echo "permissions on that database.'"
        return 1
    fi
    
    # Check .pgpass permissions
    local pgpass_perms=$(stat -c "%a" "$pgpass_file")
    if [[ "$pgpass_perms" != "600" ]]; then
        if [[ "$CHECK_ONLY" == "false" ]]; then
            chmod 0600 "$pgpass_file"
            print_status "OK" "Fixed .pgpass permissions to 0600"
        else
            print_status "WARN" ".pgpass permissions are $pgpass_perms, should be 0600"
        fi
    else
        print_status "OK" ".pgpass file permissions are correct (0600)"
    fi
    
    # Test database connection
    if [[ -f "$PROJECT_ROOT/config/db_config.py" ]]; then
        print_status "INFO" "Testing database connection..."
        cd "$PROJECT_ROOT"
        
        local test_result=$(python3 -c "
import sys
sys.path.append('config')
try:
    from db_config import db_params
    import psycopg2
    conn = psycopg2.connect(**db_params)
    conn.close()
    print('SUCCESS')
except Exception as e:
    print(f'ERROR: {e}')
" 2>/dev/null)
        
        if [[ "$test_result" == "SUCCESS" ]]; then
            print_status "OK" "Database connection successful"
        else
            print_status "ERROR" "Database connection failed: $test_result"
            return 1
        fi
    else
        print_status "WARN" "Database config not found - will be created from template"
    fi
    
    return 0
}

# Check Python environment and dependencies
check_python_environment() {
    print_status "INFO" "Checking Python environment..."
    
    # Check Python version
    local python_version=$(python3 --version 2>&1 | cut -d' ' -f2)
    print_status "INFO" "Python version: $python_version"
    
    # Check if requirements.txt exists
    if [[ ! -f "$PROJECT_ROOT/config/requirements.txt" ]]; then
        print_status "ERROR" "requirements.txt not found at $PROJECT_ROOT/config/requirements.txt"
        return 1
    fi
    
    # Check if we're in a virtual environment
    if [[ -n "$VIRTUAL_ENV" ]]; then
        print_status "OK" "Already in virtual environment: $VIRTUAL_ENV"
    elif [[ -f "$PROJECT_ROOT/venv/bin/activate" ]]; then
        print_status "OK" "Virtual environment found"
        if [[ "$CHECK_ONLY" == "false" ]]; then
            print_status "INFO" "Activating virtual environment..."
            source "$PROJECT_ROOT/venv/bin/activate"
        fi
    else
        if [[ "$CHECK_ONLY" == "false" ]]; then
            print_status "INFO" "Creating virtual environment..."
            cd "$PROJECT_ROOT"
            python3 -m venv venv
            source venv/bin/activate
            print_status "OK" "Virtual environment created and activated"
        else
            print_status "ERROR" "No virtual environment found. Please create one first:"
            echo "cd $PROJECT_ROOT && python3 -m venv venv && source venv/bin/activate"
            return 1
        fi
    fi
    
    # Install/check Python dependencies
    if [[ "$CHECK_ONLY" == "false" ]]; then
        print_status "INFO" "Installing Python dependencies..."
        cd "$PROJECT_ROOT"
        
        # Upgrade pip first
        pip install --upgrade pip
        
        # Try psycopg2-binary first (precompiled), fallback to psycopg2 if needed
        print_status "INFO" "Installing PostgreSQL adapter..."
        if pip install psycopg2-binary; then
            print_status "OK" "psycopg2-binary installed successfully"
            # Remove psycopg2 from requirements to avoid conflict
            grep -v '^psycopg2$' config/requirements.txt > /tmp/requirements_filtered.txt
            pip install -r /tmp/requirements_filtered.txt
            rm /tmp/requirements_filtered.txt
        else
            print_status "WARN" "psycopg2-binary failed, trying psycopg2 from source..."
            pip install -r config/requirements.txt
        fi
        
        print_status "OK" "Python dependencies installed"
    fi
    
    return 0
}

# Check Node.js environment
check_nodejs_environment() {
    print_status "INFO" "Checking Node.js environment..."
    
    local node_version=$(node --version 2>/dev/null || echo "not found")
    local npm_version=$(npm --version 2>/dev/null || echo "not found")
    
    print_status "INFO" "Node.js version: $node_version"
    print_status "INFO" "npm version: $npm_version"
    
    if [[ -f "$PROJECT_ROOT/config/package.json" ]]; then
        if [[ "$CHECK_ONLY" == "false" ]]; then
            print_status "INFO" "Installing Node.js dependencies..."
            cd "$PROJECT_ROOT/config"
            npm install
            print_status "OK" "Node.js dependencies installed"
        fi
    else
        print_status "WARN" "package.json not found"
    fi
    
    return 0
}

# Setup configuration files
setup_configuration() {
    print_status "INFO" "Setting up configuration files..."
    
    cd "$PROJECT_ROOT"
    
    # Setup database configuration
    if [[ ! -f "config/db_config.py" ]]; then
        if [[ -f "config/db_config.py.template" ]]; then
            cp "config/db_config.py.template" "config/db_config.py"
            print_status "OK" "Created config/db_config.py from template"
        else
            print_status "ERROR" "db_config.py.template not found"
            return 1
        fi
    else
        print_status "OK" "config/db_config.py already exists"
    fi
    
    # Setup WebSocket server configuration
    mkdir -p "data/configs"
    if [[ ! -f "data/configs/92_slot_duration_server_list.json" ]]; then
        if [[ -f "data/configs/92_slot_duration_server_list.json.template" ]]; then
            cp "data/configs/92_slot_duration_server_list.json.template" "data/configs/92_slot_duration_server_list.json"
            print_status "OK" "Created WebSocket server configuration from template"
        else
            print_status "ERROR" "92_slot_duration_server_list.json.template not found"
            return 1
        fi
    else
        print_status "OK" "WebSocket server configuration already exists"
    fi
    
    return 0
}

# Set proper file ownership and permissions
setup_permissions() {
    print_status "INFO" "Setting up file permissions..."
    
    local current_user=$(whoami)
    
    # Set ownership of project directory
    if [[ "$CHECK_ONLY" == "false" ]]; then
        # Set directory permissions
        find "$PROJECT_ROOT" -type d -exec chmod 755 {} \;
        
        # Set script permissions
        find "$PROJECT_ROOT/scripts/bash" -name "*.sh" -exec chmod +x {} \;
        
        # Set Python script permissions
        find "$PROJECT_ROOT/scripts/python" -name "*.py" -exec chmod 644 {} \;
        
        # Set data directory permissions
        mkdir -p "$PROJECT_ROOT/data"/{exports,monitoring,charts,configs,reports,analysis,temp,logs}
        chmod -R 755 "$PROJECT_ROOT/data"
        
        print_status "OK" "File permissions set correctly"
    fi
    
    # Check critical file permissions
    local critical_files=(
        "$PROJECT_ROOT/config/db_config.py:644"
        "$PROJECT_ROOT/data/configs/92_slot_duration_server_list.json:644"
    )
    
    for file_perm in "${critical_files[@]}"; do
        local file="${file_perm%%:*}"
        local expected_perm="${file_perm##*:}"
        
        if [[ -f "$file" ]]; then
            local actual_perm=$(stat -c "%a" "$file")
            if [[ "$actual_perm" == "$expected_perm" ]]; then
                print_status "OK" "$(basename "$file") permissions correct ($actual_perm)"
            else
                if [[ "$CHECK_ONLY" == "false" ]]; then
                    chmod "$expected_perm" "$file"
                    print_status "OK" "Fixed $(basename "$file") permissions to $expected_perm"
                else
                    print_status "WARN" "$(basename "$file") permissions: $actual_perm (should be $expected_perm)"
                fi
            fi
        fi
    done
    
    return 0
}

# Test external API connectivity
test_api_connectivity() {
    print_status "INFO" "Testing external API connectivity..."
    
    local apis=(
        "https://api.stakewiz.com/validators:Stakewiz API"
        "https://kobe.mainnet.jito.network/api/v1/validators:Jito Kobe API"
        "https://api.coingecko.com/api/v3/simple/price?ids=solana&vs_currencies=usd:CoinGecko API"
        "https://api.github.com/repos/solana-labs/solana/releases/latest:GitHub API"
        "https://trillium.so/pages/country-region.json:Trillium Country-Region Data"
    )
    
    for api_info in "${apis[@]}"; do
        local url="${api_info%:*}"
        local name="${api_info##*:}"
        
        if curl -s --connect-timeout 5 --max-time 10 "$url" >/dev/null 2>&1; then
            print_status "OK" "$name accessible"
        else
            print_status "WARN" "$name not accessible (check network/firewall)"
        fi
    done
    
    return 0
}

# Main execution
main() {
    echo "=============================================="
    echo "Trillium Solana Data Pipeline Setup Script"
    echo "=============================================="
    echo "Project Root: $PROJECT_ROOT"
    echo "Log File: $LOG_FILE"
    echo "Mode: $([ "$CHECK_ONLY" == "true" ] && echo "Check Only" || echo "Setup & Check")"
    echo "=============================================="
    
    log "INFO" "Starting environment setup/validation"
    
    # Initialize log file
    echo "Setup started at $(date)" > "$LOG_FILE"
    
    local exit_code=0
    
    # Run all checks
    check_user || exit_code=1
    check_system_dependencies || exit_code=1
    check_solana_cli || exit_code=1
    check_validator_history || exit_code=1
    check_postgresql || exit_code=1
    check_python_environment || exit_code=1
    check_nodejs_environment || exit_code=1
    
    # Setup configurations and permissions (if not check-only mode)
    if [[ "$CHECK_ONLY" == "false" ]]; then
        setup_configuration || exit_code=1
        setup_permissions || exit_code=1
    fi
    
    # Test connectivity
    test_api_connectivity
    
    echo "=============================================="
    if [[ $exit_code -eq 0 ]]; then
        print_status "OK" "Environment setup/validation completed successfully!"
        if [[ "$CHECK_ONLY" == "false" ]]; then
            echo ""
            echo "Next steps:"
            echo "1. Ensure your ~/.pgpass file contains the correct database password"
            echo "2. Review config/db_config.py and update username if needed"
            echo "3. Review data/configs/92_slot_duration_server_list.json if using different RPC providers"
            echo "4. Run: source venv/bin/activate (to activate Python virtual environment)"
        fi
    else
        print_status "ERROR" "Environment setup/validation failed. Check errors above."
    fi
    echo "=============================================="
    
    log "INFO" "Setup completed with exit code: $exit_code"
    
    exit $exit_code
}

# Run main function
main "$@"