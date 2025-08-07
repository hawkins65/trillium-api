import os
import sys
import json
from decimal import Decimal
from pathlib import Path

# Import from parent directory
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import importlib
logging_config = importlib.import_module('999_logging_config')
setup_logging = logging_config.setup_logging

# Initialize logger
logger = setup_logging('utils')

# Define base directories using environment variables or defaults
TRILLIUM_BASE = os.environ.get('TRILLIUM_BASE', '/home/smilax/trillium_api')
TRILLIUM_DATA = os.environ.get('TRILLIUM_DATA', os.path.join(TRILLIUM_BASE, 'data'))
TRILLIUM_DATA_JSON = os.environ.get('TRILLIUM_DATA_JSON', os.path.join(TRILLIUM_DATA, 'json'))
TRILLIUM_DATA_EPOCHS = os.environ.get('TRILLIUM_DATA_EPOCHS', os.path.join(TRILLIUM_DATA, 'epochs'))

# Output directories for leaderboard data
# Use environment variables if set by the bash script, otherwise use defaults
LEADERBOARD_OUTPUT_BASE = os.environ.get('TRILLIUM_LEADERBOARD_DIR', os.path.join(TRILLIUM_DATA, 'leaderboard'))
LEADERBOARD_JSON_DIR = os.environ.get('TRILLIUM_LEADERBOARD_JSON', os.path.join(LEADERBOARD_OUTPUT_BASE, 'json'))
LEADERBOARD_CSV_DIR = os.environ.get('TRILLIUM_LEADERBOARD_CSV', os.path.join(LEADERBOARD_OUTPUT_BASE, 'csv'))
LEADERBOARD_HTML_DIR = os.environ.get('TRILLIUM_LEADERBOARD_HTML', os.path.join(LEADERBOARD_OUTPUT_BASE, 'html'))

def get_output_directory(output_type='json'):
    """
    Get the standard output directory for a given type and ensure it exists.
    
    Args:
        output_type: Type of output ('json', 'csv', 'html')
    
    Returns:
        Path to the output directory
    """
    directory_map = {
        'json': LEADERBOARD_JSON_DIR,
        'csv': LEADERBOARD_CSV_DIR,
        'html': LEADERBOARD_HTML_DIR
    }
    
    output_dir = directory_map.get(output_type, LEADERBOARD_OUTPUT_BASE)
    ensure_directory(output_dir)
    return output_dir

def get_output_path(filename, output_type='json'):
    """
    Get the full path for an output file in the standard directory structure.
    
    Args:
        filename: Name of the file
        output_type: Type of output ('json', 'csv', 'html')
    
    Returns:
        Full path to the output file
    """
    output_dir = get_output_directory(output_type)
    return os.path.join(output_dir, filename)

# Client type mapping
CLIENT_TYPE_MAP = {
    0: 'Solana Labs',
    1: 'Jito Labs',
    2: 'Firedancer',
    3: 'Agave',
    4: 'Paladin',
    None: 'Unknown',
}

def ensure_directory(directory):
    os.makedirs(directory, exist_ok=True)
    return directory

def format_elapsed_time(seconds):
    days = seconds // (24 * 3600)
    seconds = seconds % (24 * 3600)
    hours = seconds // 3600
    seconds %= 3600
    minutes = seconds // 60
    return f"{int(days):02}:{int(hours):02}:{int(minutes):02}"

def decimal_default(obj):
    if isinstance(obj, Decimal):
        return float(obj)
    raise TypeError

def format_lamports_to_sol(lamports, precision=7):
    if lamports is None:
        return None
    sol_amount = Decimal(lamports) / Decimal('1000000000')
    if precision == 0:
        return int(sol_amount)
    return float(f"{sol_amount:.{precision}f}")

def format_number(number, precision):
    if number is None:
        return None
    return float(f"{number:.{precision}f}") if precision > 0 else int(number)

def add_trillium_attribution(data):
    if isinstance(data, dict):
        data["_Trillium_Attribution"] = "Fueled By Trillium | Solana"
        return data
    elif isinstance(data, list):
        return [add_trillium_attribution(item) for item in data]
    else:
        return data

def save_chart_html(fig, chart_title, output_filename):
    # If output_filename is a relative path starting with ./, convert to standard path
    if output_filename.startswith('./html/'):
        filename_only = os.path.basename(output_filename)
        output_filename = get_output_path(filename_only, 'html')
    elif not os.path.isabs(output_filename):
        # If it's just a filename without path, put it in the standard html directory
        output_filename = get_output_path(output_filename, 'html')
    
    # Ensure the directory exists
    ensure_directory(os.path.dirname(output_filename))
    
    html_chart = fig.to_html(full_html=False, include_plotlyjs='cdn')
    html_full = f"""
    <!DOCTYPE html>
    <html lang="en">
    <head>
        <meta charset="utf-8">
        <title>{chart_title}</title>
        <link rel="icon" href="https://trillium.so/pages/trillium-logo.gif" type="image/gif">
        <style>
            body {{
                margin: 0;
                padding: 0;
                background: #ffffff;
                font-family: Arial, sans-serif;
                text-align: center;
            }}
            .logo {{
                margin-top: 30px;
            }}
        </style>
    </head>
    <body>
        {html_chart}
        <div class="logo">
            <img src="https://trillium.so/images/fueled-by-trillium.png" height="30">
        </div>
    </body>
    </html>
    """
    with open(output_filename, "w") as f:
        f.write(html_full)
    logger.info(f"HTML file created - {output_filename}")