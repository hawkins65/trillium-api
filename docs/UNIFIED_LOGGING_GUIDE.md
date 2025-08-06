# Trillium API Unified Logging System

## Overview
This unified logging system provides consistent logging across all script types (Python, Bash, JavaScript, SQL) with color-coded output and standardized formatting.

## Log Format
```
[TIMESTAMP] [LEVEL] [SCRIPT_TYPE:SCRIPT_NAME] [PID] - MESSAGE
```

## Color Scheme by Script Type
- 🐍 **Python**: Blue/Cyan (`\033[36m`)
- 🐚 **Bash**: Green (`\033[32m`) 
- 🟨 **JavaScript**: Yellow (`\033[33m`)
- 🔍 **SQL**: Purple/Magenta (`\033[35m`)
- ⚙️ **System**: Gray (`\033[90m`)

## Log Levels (with colors)
- **DEBUG**: White (`\033[37m`)
- **INFO**: Green (`\033[32m`)
- **WARN**: Yellow (`\033[33m`)
- **ERROR**: Red (`\033[31m`)
- **CRITICAL**: Red background (`\033[41m`)

## Usage by Script Type

### Python Scripts
```python
#!/usr/bin/env python3
import sys
import os
import importlib.util

# Load logging configuration
spec = importlib.util.spec_from_file_location("logging_config", "999_logging_config.py")
logging_config = importlib.util.module_from_spec(spec)
spec.loader.exec_module(logging_config)
setup_logging = logging_config.setup_logging

# Setup logger
logger = setup_logging(os.path.basename(__file__).replace('.py', ''))

# Use logger
logger.info("🚀 Script started")
logger.debug("Debug information")
logger.warning("Warning message") 
logger.error("Error occurred")
logger.critical("Critical issue")
```

### Bash Scripts
```bash
#!/bin/bash

# Source common logging
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/999_common_log.sh"

# Initialize logging
init_logging

# Use logging functions
log_info "🚀 Script started"
log_debug "Debug information"
log_warn "Warning message"
log_error "Error occurred"

# Cleanup at end
cleanup_logging
```

### JavaScript/Node.js Scripts
```javascript
#!/usr/bin/env node
const { setupLogging } = require('./999_logging_config');

// Setup logger
const logger = setupLogging(require('path').basename(__filename, '.js'));

// Use logger
logger.info('🚀 Script started');
logger.debug('Debug information');
logger.warn('Warning message');
logger.error('Error occurred');
logger.critical('Critical issue');
```

### SQL Scripts (via Python Wrapper)
```bash
# Execute SQL with logging
python 999_sql_logging_wrapper.py my_script.sql "Description of what this SQL does"
```

## Advanced Features

### Execution Time Logging
```python
# Python
import time
start_time = time.time()
# ... do work ...
end_time = time.time()
logger.info(f"⏱️ Operation completed in {end_time - start_time:.2f}s")
```

```bash
# Bash
start_time=$(date +%s.%N)
# ... do work ...
log_execution_time "INFO" "Operation name" "$start_time"
```

```javascript
// JavaScript
const startTime = Date.now();
// ... do work ...
logger.logExecutionTime('Operation name', startTime);
```

### Context Logging
```python
# Python
logger.info("[EPOCH_123] Processing validator data")
```

```bash
# Bash
log_context "INFO" "EPOCH_123" "Processing validator data"
```

```javascript
// JavaScript
logger.logContext('INFO', 'EPOCH_123', 'Processing validator data');
```

### System Resource Logging
```bash
# Bash only
log_system_stats "DEBUG"  # Logs CPU and memory usage
```

## Configuration

### Environment Variables
```bash
export LOG_BASE_DIR="$HOME/log"           # Default: ~/log
export LOG_LEVEL="INFO"                   # DEBUG, INFO, WARN, ERROR
export LOG_RETENTION_DAYS="7"             # Days to keep log files
export MAX_LOG_SIZE="100M"                # Max size before rotation
```

### Directory Structure
```
~/log/
├── script_name_log_2025-08-02_17-20-50.log
├── another_script_log_2025-08-02_17-21-15.log
└── rotated_logs/
    ├── old_script.log.20250801_120000.gz
    └── ...
```

## File Features

### Log File Rotation
- Files are automatically rotated when they exceed `MAX_LOG_SIZE`
- Rotated files are compressed with gzip
- Old log files are cleaned up after `LOG_RETENTION_DAYS`

### Dual Output
- **Console**: Colored output with emojis for easy reading
- **File**: Plain text format for machine processing and long-term storage

## Example Output

### Console (with colors)
```
[2025-08-02 17:20:50] [INFO] [🐚BASH:data_processor] [PID:12345] - 🚀 Processing epoch 825
[2025-08-02 17:20:51] [WARN] [🐍PYTHON:validator_check] [PID:12346] - ⚠️ Found 3 offline validators
[2025-08-02 17:20:52] [ERROR] [🟨JAVASCRIPT:api_server] [PID:12347] - ❌ Database connection failed
```

### Log File (plain text)
```
[2025-08-02 17:20:50] [INFO] [BASH:data_processor] [PID:12345] - 🚀 Processing epoch 825
[2025-08-02 17:20:51] [WARN] [PYTHON:validator_check] [PID:12346] - ⚠️ Found 3 offline validators 
[2025-08-02 17:20:52] [ERROR] [JAVASCRIPT:api_server] [PID:12347] - ❌ Database connection failed
```

## Migration Guide

### Updating Existing Scripts

1. **Python Scripts**: Replace existing logging setup with unified version
2. **Bash Scripts**: Source `999_common_log.sh` and replace `echo` with `log_info`
3. **JavaScript Scripts**: Use the new `999_logging_config.js`
4. **SQL Scripts**: Execute via the Python wrapper for logging

### Gradual Migration
- Start with new scripts using the unified system
- Update critical/frequently-used scripts first
- Maintain backward compatibility during transition

## Testing
Use the provided test scripts to verify logging configuration:
```bash
# Test Python logging
cd scripts/python && python test_unified_logging.py

# Test Bash logging  
cd scripts/bash && ./test_unified_logging.sh

# Test JavaScript logging
cd scripts/nodejs && node 999_logging_config.js
```

## Benefits
1. **Consistency**: Same format across all script types
2. **Readability**: Color coding and emojis for quick visual parsing
3. **Traceability**: Process IDs and timestamps for debugging
4. **Maintainability**: Centralized logging configuration
5. **Performance**: Log rotation and cleanup to manage disk space
6. **Flexibility**: Configurable via environment variables