#!/usr/bin/env python3
"""
Test script for unified logging configuration
"""

import sys
import os
import time

# Setup logging
import importlib.util
script_dir = os.path.dirname(os.path.abspath(__file__))
logging_config_path = os.path.join(script_dir, "999_logging_config.py")
spec = importlib.util.spec_from_file_location("logging_config", logging_config_path)
logging_config = importlib.util.module_from_spec(spec)
spec.loader.exec_module(logging_config)
setup_logging = logging_config.setup_logging

def main():
    logger = setup_logging(os.path.basename(__file__).replace('.py', ''))
    
    logger.info("🚀 Testing Python unified logging configuration")
    logger.debug("This is a debug message")
    logger.warning("This is a warning message") 
    logger.error("This is an error message")
    logger.critical("This is a critical message")
    
    # Test execution timing
    start_time = time.time()
    time.sleep(0.1)  # Simulate work
    end_time = time.time()
    
    duration = end_time - start_time
    logger.info(f"⏱️ Test operation completed in {duration:.2f}s")
    logger.info("🏁 Python logging test completed")

if __name__ == "__main__":
    main()