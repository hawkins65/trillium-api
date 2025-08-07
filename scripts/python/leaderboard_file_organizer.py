#!/usr/bin/env python3
"""
Wrapper functions to help organize leaderboard output files into appropriate directories.
This module provides utilities to redirect file outputs to organized directory structure.
"""

import os
import sys
import argparse
import shutil
from pathlib import Path
from typing import Optional, Dict, Any

# Default output directories based on file type
DEFAULT_OUTPUT_DIRS = {
    '.json': 'json',
    '.csv': 'csv',
    '.html': 'html',
    '.txt': 'logs',
    '.log': 'logs',
}

class FileOrganizer:
    """Manages file output organization for leaderboard generation."""
    
    def __init__(self, base_dir: Optional[str] = None):
        """
        Initialize the file organizer.
        
        Args:
            base_dir: Base directory for organized outputs. If None, uses TRILLIUM_LEADERBOARD_DIR env var
        """
        if base_dir is None:
            base_dir = os.environ.get('TRILLIUM_LEADERBOARD_DIR', '/home/smilax/trillium_api/data/leaderboard')
        
        self.base_dir = Path(base_dir)
        self.ensure_directories()
    
    def ensure_directories(self):
        """Create necessary subdirectories if they don't exist."""
        for subdir in ['json', 'csv', 'html', 'logs']:
            dir_path = self.base_dir / subdir
            dir_path.mkdir(parents=True, exist_ok=True)
    
    def get_output_path(self, filename: str, file_type: Optional[str] = None) -> Path:
        """
        Get the appropriate output path for a file based on its type.
        
        Args:
            filename: Name of the file
            file_type: Optional explicit file type. If None, inferred from extension
        
        Returns:
            Path object for the output file location
        """
        if file_type is None:
            # Infer from extension
            ext = Path(filename).suffix.lower()
            subdir = DEFAULT_OUTPUT_DIRS.get(ext, 'logs')
        else:
            subdir = file_type
        
        return self.base_dir / subdir / filename
    
    def organize_existing_files(self, source_dir: str = '.'):
        """
        Organize existing files in a directory by moving them to appropriate subdirectories.
        
        Args:
            source_dir: Directory containing files to organize
        """
        source_path = Path(source_dir)
        
        # Pattern mappings for special files
        special_patterns = {
            '2_response_file.*': 'logs',
            '*_log_*.log': 'logs',
        }
        
        moved_files = []
        
        for file_path in source_path.iterdir():
            if file_path.is_file():
                # Check special patterns first
                moved = False
                for pattern, target_dir in special_patterns.items():
                    if file_path.match(pattern):
                        dest_path = self.base_dir / target_dir / file_path.name
                        shutil.move(str(file_path), str(dest_path))
                        moved_files.append((file_path.name, target_dir))
                        moved = True
                        break
                
                # If not a special pattern, use extension
                if not moved:
                    ext = file_path.suffix.lower()
                    if ext in DEFAULT_OUTPUT_DIRS:
                        target_dir = DEFAULT_OUTPUT_DIRS[ext]
                        dest_path = self.base_dir / target_dir / file_path.name
                        if file_path != dest_path:  # Don't move if already in place
                            shutil.move(str(file_path), str(dest_path))
                            moved_files.append((file_path.name, target_dir))
        
        return moved_files

def add_output_dir_argument(parser: argparse.ArgumentParser):
    """
    Add output directory argument to an argument parser.
    
    Args:
        parser: ArgumentParser instance to add the argument to
    """
    parser.add_argument(
        '--output-dir',
        type=str,
        help='Base directory for organized output files',
        default=os.environ.get('TRILLIUM_LEADERBOARD_DIR')
    )

def setup_output_redirect(args: argparse.Namespace) -> FileOrganizer:
    """
    Set up file output redirection based on parsed arguments.
    
    Args:
        args: Parsed command line arguments
    
    Returns:
        FileOrganizer instance
    """
    return FileOrganizer(base_dir=args.output_dir if hasattr(args, 'output_dir') else None)

def wrap_file_write(organizer: FileOrganizer):
    """
    Create a wrapper function for file writing that redirects to organized directories.
    
    Args:
        organizer: FileOrganizer instance
    
    Returns:
        Wrapper function for open()
    """
    original_open = open
    
    def wrapped_open(filename, mode='r', *args, **kwargs):
        # Only redirect write modes for files in current directory
        if any(m in mode for m in ['w', 'a', 'x']) and not os.path.dirname(filename):
            # Get organized path
            organized_path = organizer.get_output_path(filename)
            return original_open(organized_path, mode, *args, **kwargs)
        else:
            return original_open(filename, mode, *args, **kwargs)
    
    return wrapped_open

# CLI for organizing existing files
if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Organize leaderboard output files')
    parser.add_argument('source_dir', nargs='?', default='.', help='Source directory to organize')
    add_output_dir_argument(parser)
    
    args = parser.parse_args()
    
    organizer = FileOrganizer(base_dir=args.output_dir)
    moved_files = organizer.organize_existing_files(args.source_dir)
    
    if moved_files:
        print(f"Organized {len(moved_files)} files:")
        for filename, target_dir in moved_files:
            print(f"  {filename} -> {target_dir}/")
    else:
        print("No files to organize.")