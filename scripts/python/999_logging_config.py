import logging
import os
import sys
from datetime import datetime

# Color codes for different script types
SCRIPT_COLORS = {
    'python': '\033[36m',      # Cyan
    'bash': '\033[32m',        # Green
    'javascript': '\033[33m',  # Yellow
    'sql': '\033[35m',         # Magenta
    'system': '\033[90m',      # Dark Gray
    'reset': '\033[0m'         # Reset
}

# Log level colors
LEVEL_COLORS = {
    'DEBUG': '\033[37m',       # White
    'INFO': '\033[32m',        # Green
    'WARNING': '\033[33m',     # Yellow
    'ERROR': '\033[31m',       # Red
    'CRITICAL': '\033[41m',    # Red background
    'reset': '\033[0m'
}

def get_script_type(script_name):
    """Determine script type from filename extension or name pattern."""
    if script_name.endswith('.py') or 'python' in script_name.lower():
        return 'python'
    elif script_name.endswith('.sh') or 'bash' in script_name.lower():
        return 'bash'
    elif script_name.endswith('.js') or 'node' in script_name.lower():
        return 'javascript'
    elif script_name.endswith('.sql'):
        return 'sql'
    else:
        return 'system'

def setup_logging(script_name, log_dir=None, level=logging.INFO):
    """
    Sets up a standardized logger for a script.

    Args:
        script_name (str): The name of the script (e.g., os.path.basename(__file__).replace('.py', '')).
        log_dir (str, optional): The directory where log files will be stored.
                                 Defaults to '~/log'.
        level (int, optional): The logging level (e.g., logging.INFO, logging.DEBUG).
                               Defaults to logging.INFO.
    Returns:
        logging.Logger: The configured logger instance.
    """
    if log_dir is None:
        log_dir = os.path.expanduser('~/log')

    os.makedirs(log_dir, exist_ok=True)

    # Remove all existing handlers to prevent duplicate logs
    for handler in logging.root.handlers[:]:
        logging.root.removeHandler(handler)
    for handler in logging.getLogger().handlers[:]:
        logging.getLogger().removeHandler(handler)

    logger = logging.getLogger(script_name)
    logger.setLevel(level)

    # File handler
    now = datetime.now()
    formatted_time = now.strftime('%Y-%m-%d_%H-%M-%S')
    file_handler = logging.FileHandler(os.path.join(log_dir, f"{script_name}_log_{formatted_time}.log"))
    file_handler.setLevel(level)
    file_formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    file_handler.setFormatter(file_formatter)
    logger.addHandler(file_handler)

    # Console handler with colors
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(logging.INFO) # Console always INFO or higher
    
    # Custom formatter with script type and colors
    script_type = get_script_type(script_name)
    script_color = SCRIPT_COLORS.get(script_type, SCRIPT_COLORS['system'])
    reset_color = SCRIPT_COLORS['reset']
    
    class ColoredFormatter(logging.Formatter):
        def format(self, record):
            # Add script type to record
            record.script_type = script_type.upper()
            record.script_name = script_name
            record.pid = os.getpid()
            
            # Color the level
            level_color = LEVEL_COLORS.get(record.levelname, LEVEL_COLORS['reset'])
            colored_level = f"{level_color}{record.levelname}{LEVEL_COLORS['reset']}"
            
            # Color the script info
            colored_script = f"{script_color}🐍{record.script_type}:{record.script_name}{reset_color}"
            
            # Create formatted message
            formatted = f"[{record.asctime}] [{colored_level}] [{colored_script}] [PID:{record.pid}] - {record.getMessage()}"
            return formatted
    
    console_formatter = ColoredFormatter('%(asctime)s')
    console_handler.setFormatter(console_formatter)
    logger.addHandler(console_handler)

    return logger
