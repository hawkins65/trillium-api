#!/usr/bin/env python3
"""
Utility module to handle output paths for leaderboard files.
Uses environment variables set by the bash script to determine output directories.
"""

import os
from pathlib import Path

def get_output_path(filename: str, file_type: str = None) -> str:
    """
    Get the appropriate output path for a file based on environment variables.
    
    Args:
        filename: Name of the file to create
        file_type: Type of file ('json', 'csv', 'html', 'log'). If None, inferred from extension
    
    Returns:
        Full path where the file should be created
    """
    # Determine file type from extension if not provided
    if file_type is None:
        ext = Path(filename).suffix.lower()
        file_type_map = {
            '.json': 'json',
            '.csv': 'csv',
            '.html': 'html',
            '.txt': 'log',
            '.log': 'log'
        }
        file_type = file_type_map.get(ext, 'log')
    
    # Get the appropriate directory from environment variables
    env_var_map = {
        'json': 'TRILLIUM_LEADERBOARD_JSON',
        'csv': 'TRILLIUM_LEADERBOARD_CSV',
        'html': 'TRILLIUM_LEADERBOARD_HTML',
        'log': 'TRILLIUM_LEADERBOARD_LOGS',
        'logs': 'TRILLIUM_LEADERBOARD_LOGS'
    }
    
    env_var = env_var_map.get(file_type, 'TRILLIUM_LEADERBOARD_LOGS')
    output_dir = os.environ.get(env_var)
    
    # Fallback to current directory if environment variable not set
    if not output_dir:
        # Try to use default structure
        base_dir = os.environ.get('TRILLIUM_DATA', '/home/smilax/trillium_api/data')
        if file_type == 'log' or file_type == 'logs':
            output_dir = os.path.join(base_dir, 'logs')
        else:
            output_dir = os.path.join(base_dir, 'leaderboard', file_type)
    
    # Create directory if it doesn't exist
    Path(output_dir).mkdir(parents=True, exist_ok=True)
    
    # Return full path
    return os.path.join(output_dir, filename)

def get_json_path(filename: str) -> str:
    """Convenience function for JSON files."""
    return get_output_path(filename, 'json')

def get_csv_path(filename: str) -> str:
    """Convenience function for CSV files."""
    return get_output_path(filename, 'csv')

def get_html_path(filename: str) -> str:
    """Convenience function for HTML files."""
    return get_output_path(filename, 'html')

def get_log_path(filename: str) -> str:
    """Convenience function for log files."""
    return get_output_path(filename, 'log')