#!/usr/bin/env python3
"""
SQL Script Logging Wrapper
Executes SQL scripts with unified logging format
"""

import sys
import os
import subprocess
import time
from datetime import datetime

# Add parent directory to path for logging config
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from python.logging_config import setup_logging

def execute_sql_with_logging(sql_file, db_params=None, description=None):
    """
    Execute SQL file with standardized logging
    
    Args:
        sql_file (str): Path to SQL file
        db_params (dict): Database connection parameters
        description (str): Description of what the SQL does
    """
    
    # Setup logging for SQL wrapper
    logger = setup_logging(f"sql_wrapper_{os.path.basename(sql_file)}")
    
    start_time = time.time()
    script_name = os.path.basename(sql_file)
    
    logger.info(f"🔍 Executing SQL script: {script_name}")
    if description:
        logger.info(f"📝 Description: {description}")
    
    try:
        # Read SQL file
        with open(sql_file, 'r') as f:
            sql_content = f.read()
        
        logger.debug(f"📄 SQL file size: {len(sql_content)} characters")
        
        # If db_params provided, execute using psycopg2
        if db_params:
            import psycopg2
            
            conn = psycopg2.connect(**db_params)
            cursor = conn.cursor()
            
            logger.info("🔗 Database connection established")
            
            # Execute SQL
            cursor.execute(sql_content)
            
            # Get row count if applicable
            try:
                row_count = cursor.rowcount
                if row_count >= 0:
                    logger.info(f"📊 Rows affected: {row_count}")
            except:
                pass
                
            conn.commit()
            cursor.close()
            conn.close()
            
            logger.info("✅ SQL execution completed successfully")
            
        else:
            logger.info("ℹ️ No database parameters provided - SQL file read only")
            
    except FileNotFoundError:
        logger.error(f"❌ SQL file not found: {sql_file}")
        return False
    except Exception as e:
        logger.error(f"❌ SQL execution failed: {str(e)}")
        return False
    finally:
        end_time = time.time()
        duration = end_time - start_time
        logger.info(f"⏱️ SQL script execution completed in {duration:.2f}s")
    
    return True

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python 999_sql_logging_wrapper.py <sql_file> [description]")
        sys.exit(1)
    
    sql_file = sys.argv[1]
    description = sys.argv[2] if len(sys.argv) > 2 else None
    
    # Import db_params if available
    try:
        sys.path.append(os.path.join(os.path.dirname(__file__)))
        from db_config import db_params
        success = execute_sql_with_logging(sql_file, db_params, description)
    except ImportError:
        success = execute_sql_with_logging(sql_file, None, description)
    
    sys.exit(0 if success else 1)