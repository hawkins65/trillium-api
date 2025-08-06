#!/usr/bin/env node
/**
 * Unified logging configuration for Node.js scripts
 * Matches the Python and Bash logging formats with consistent colors
 */

const fs = require('fs');
const path = require('path');

// Color codes for different script types and levels
const SCRIPT_COLORS = {
    javascript: '\x1b[33m',  // Yellow
    python: '\x1b[36m',      // Cyan
    bash: '\x1b[32m',        // Green
    sql: '\x1b[35m',         // Magenta
    system: '\x1b[90m',      // Dark Gray
    reset: '\x1b[0m'         // Reset
};

const LEVEL_COLORS = {
    DEBUG: '\x1b[37m',       // White
    INFO: '\x1b[32m',        // Green
    WARN: '\x1b[33m',        // Yellow
    ERROR: '\x1b[31m',       // Red
    CRITICAL: '\x1b[41m',    // Red background
    reset: '\x1b[0m'
};

const LOG_LEVELS = {
    DEBUG: 0,
    INFO: 1,
    WARN: 2,
    ERROR: 3,
    CRITICAL: 4
};

class TrilliumLogger {
    constructor(scriptName, options = {}) {
        this.scriptName = scriptName || path.basename(process.argv[1], '.js');
        this.logDir = options.logDir || path.join(process.env.HOME, 'log');
        this.logLevel = options.logLevel || 'INFO';
        this.scriptType = 'JAVASCRIPT';
        this.pid = process.pid;
        
        // Ensure log directory exists
        if (!fs.existsSync(this.logDir)) {
            fs.mkdirSync(this.logDir, { recursive: true });
        }
        
        // Setup log file
        const timestamp = new Date().toISOString().replace(/[:.]/g, '-').slice(0, 19);
        this.logFile = path.join(this.logDir, `${this.scriptName}_log_${timestamp}.log`);
    }
    
    shouldLog(level) {
        const currentPriority = LOG_LEVELS[this.logLevel] || 1;
        const messagePriority = LOG_LEVELS[level] || 1;
        return messagePriority >= currentPriority;
    }
    
    formatTimestamp() {
        return new Date().toISOString().replace('T', ' ').slice(0, 19);
    }
    
    formatLogEntry(level, message, caller = '') {
        const timestamp = this.formatTimestamp();
        const callerInfo = caller ? ` [${caller}]` : '';
        
        // Plain format for file logging
        const plainEntry = `[${timestamp}] [${level}] [${this.scriptType}:${this.scriptName}] [PID:${this.pid}]${callerInfo} - ${message}`;
        
        // Colored format for console
        const levelColor = LEVEL_COLORS[level] || LEVEL_COLORS.reset;
        const scriptColor = SCRIPT_COLORS.javascript;
        const resetColor = SCRIPT_COLORS.reset;
        
        const coloredEntry = `[${timestamp}] [${levelColor}${level}${LEVEL_COLORS.reset}] [${scriptColor}🟨${this.scriptType}:${this.scriptName}${resetColor}] [PID:${this.pid}]${callerInfo} - ${message}`;
        
        return { plainEntry, coloredEntry };
    }
    
    log(level, message, caller = '') {
        if (!this.shouldLog(level)) {
            return;
        }
        
        const { plainEntry, coloredEntry } = this.formatLogEntry(level, message, caller);
        
        // Console output with colors (to stderr)
        if (process.stdout.isTTY) {
            process.stderr.write(coloredEntry + '\n');
        } else {
            process.stderr.write(plainEntry + '\n');
        }
        
        // File output without colors
        try {
            fs.appendFileSync(this.logFile, plainEntry + '\n');
        } catch (err) {
            // Fallback - at least show the message
            process.stderr.write(`[LOG ERROR] Could not write to log file: ${err.message}\n`);
        }
    }
    
    debug(message, caller) {
        this.log('DEBUG', message, caller);
    }
    
    info(message, caller) {
        this.log('INFO', message, caller);
    }
    
    warn(message, caller) {
        this.log('WARN', message, caller);
    }
    
    error(message, caller) {
        this.log('ERROR', message, caller);
    }
    
    critical(message, caller) {
        this.log('CRITICAL', message, caller);
    }
    
    // Execution time logging
    logExecutionTime(description, startTime, level = 'INFO') {
        const endTime = Date.now();
        const duration = ((endTime - startTime) / 1000).toFixed(2);
        this.log(level, `⏱️ ${description} completed in ${duration}s`);
    }
    
    // Context logging
    logContext(level, context, message) {
        this.log(level, `[${context}] ${message}`);
    }
}

// Factory function
function setupLogging(scriptName, options = {}) {
    return new TrilliumLogger(scriptName, options);
}

// Export for use in other modules
module.exports = {
    TrilliumLogger,
    setupLogging,
    SCRIPT_COLORS,
    LEVEL_COLORS,
    LOG_LEVELS
};

// CLI usage example
if (require.main === module) {
    const logger = setupLogging('test_script');
    
    logger.info('🚀 Testing JavaScript logging configuration');
    logger.debug('This is a debug message');
    logger.warn('This is a warning message');
    logger.error('This is an error message');
    logger.critical('This is a critical message');
    
    const startTime = Date.now();
    setTimeout(() => {
        logger.logExecutionTime('Test operation', startTime);
        logger.info('🏁 Logging test completed');
    }, 100);
}