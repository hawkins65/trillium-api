#!/usr/bin/env python3
"""
One-off script to generate slot duration histograms for all epochs with valid data
Usage: python3 generate_all_histograms.py
"""
import os
import sys
import subprocess
import psycopg2
from psycopg2.extras import RealDictCursor
import importlib.util

# Setup unified logging
script_dir = os.path.dirname(os.path.abspath(__file__))
logging_config_path = os.path.join(script_dir, "scripts/python/999_logging_config.py")
spec = importlib.util.spec_from_file_location("logging_config", logging_config_path)
logging_config = importlib.util.module_from_spec(spec)
spec.loader.exec_module(logging_config)
logger = logging_config.setup_logging(os.path.basename(__file__).replace('.py', ''))

# Database connection
sys.path.append('scripts/python')
from db_config import db_params

def get_valid_epochs():
    """Get all epochs that have valid slot duration data"""
    try:
        conn = psycopg2.connect(**db_params)
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        
        # Query to find epochs with meaningful slot duration data
        cursor.execute("""
            SELECT 
                epoch,
                COUNT(*) as validator_count,
                COUNT(slot_duration_mean) as with_duration,
                AVG(slot_duration_mean) as avg_duration_ns,
                MIN(slot_duration_mean) as min_duration_ns,
                MAX(slot_duration_mean) as max_duration_ns
            FROM validator_stats_slot_duration 
            WHERE slot_duration_mean IS NOT NULL 
                AND slot_duration_mean > 0
            GROUP BY epoch
            HAVING COUNT(slot_duration_mean) >= 10  -- At least 10 validators with data
            ORDER BY epoch
        """)
        
        epochs_data = cursor.fetchall()
        cursor.close()
        conn.close()
        
        return epochs_data
        
    except Exception as e:
        logger.error(f"Error querying valid epochs: {e}")
        return []

def run_histogram_script(epoch):
    """Run the histogram script for a specific epoch"""
    script_path = os.path.join(script_dir, "scripts/python/93_plot_slot_duration_histogram.py")
    
    try:
        logger.info(f"🎨 Generating histogram for epoch {epoch}")
        result = subprocess.run([
            sys.executable, script_path, str(epoch)
        ], capture_output=True, text=True, timeout=300)  # 5 minute timeout
        
        if result.returncode == 0:
            logger.info(f"✅ Successfully generated histogram for epoch {epoch}")
            # Extract output file path from stdout if available
            for line in result.stdout.split('\n'):
                if 'Histogram saved to' in line:
                    logger.info(f"📁 {line.strip()}")
            return True
        else:
            logger.error(f"❌ Failed to generate histogram for epoch {epoch}")
            logger.error(f"Error output: {result.stderr}")
            return False
            
    except subprocess.TimeoutExpired:
        logger.error(f"⏰ Timeout generating histogram for epoch {epoch}")
        return False
    except Exception as e:
        logger.error(f"❌ Unexpected error generating histogram for epoch {epoch}: {e}")
        return False

def main():
    logger.info("🚀 Starting batch histogram generation for all valid epochs")
    
    # Get all valid epochs
    epochs_data = get_valid_epochs()
    
    if not epochs_data:
        logger.error("❌ No valid epochs found with slot duration data")
        sys.exit(1)
    
    logger.info(f"📊 Found {len(epochs_data)} epochs with valid slot duration data")
    
    # Show summary of what we're about to process
    logger.info("📋 Epochs to process:")
    for epoch_data in epochs_data:
        epoch = epoch_data['epoch']
        validator_count = epoch_data['validator_count']
        with_duration = epoch_data['with_duration']
        avg_duration_ms = epoch_data['avg_duration_ns'] / 1_000_000  # Convert ns to ms
        min_duration_ms = epoch_data['min_duration_ns'] / 1_000_000
        max_duration_ms = epoch_data['max_duration_ns'] / 1_000_000
        
        logger.info(f"  Epoch {epoch}: {with_duration}/{validator_count} validators, "
                   f"avg {avg_duration_ms:.1f}ms (range: {min_duration_ms:.1f}-{max_duration_ms:.1f}ms)")
    
    # Confirm before proceeding
    try:
        response = input(f"\nGenerate histograms for all {len(epochs_data)} epochs? (y/n) [y]: ").strip().lower()
        if response and response != 'y':
            logger.info("❌ Operation cancelled by user")
            sys.exit(0)
    except KeyboardInterrupt:
        logger.info("\n❌ Operation cancelled by user")
        sys.exit(0)
    
    # Process each epoch
    successful = 0
    failed = 0
    
    for i, epoch_data in enumerate(epochs_data, 1):
        epoch = epoch_data['epoch']
        logger.info(f"🔄 Processing epoch {epoch} ({i}/{len(epochs_data)})")
        
        if run_histogram_script(epoch):
            successful += 1
        else:
            failed += 1
            
        # Brief pause between epochs to avoid overwhelming the system
        import time
        time.sleep(1)
    
    # Summary
    logger.info(f"\n🎉 Batch histogram generation completed!")
    logger.info(f"✅ Successful: {successful}")
    logger.info(f"❌ Failed: {failed}")
    logger.info(f"📊 Total processed: {successful + failed}/{len(epochs_data)}")
    
    if successful > 0:
        logger.info(f"📁 HTML files should be in the output directory configured in output_paths.py")

if __name__ == "__main__":
    main()