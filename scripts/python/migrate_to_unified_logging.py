#!/usr/bin/env python3
"""
Batch migration script to update Python scripts to use unified logging
"""

import os
import re
import glob
import importlib.util

# Setup unified logging for this migration script
script_dir = os.path.dirname(os.path.abspath(__file__))
logging_config_path = os.path.join(script_dir, "999_logging_config.py")
spec = importlib.util.spec_from_file_location("logging_config", logging_config_path)
logging_config = importlib.util.module_from_spec(spec)
spec.loader.exec_module(logging_config)
logger = logging_config.setup_logging(os.path.basename(__file__).replace('.py', ''))

def migrate_python_script(file_path):
    """Migrate a single Python script to use unified logging"""
    
    try:
        with open(file_path, 'r') as f:
            content = f.read()
        
        # Skip if already migrated
        if '999_logging_config' in content:
            logger.info(f"⏭️ Already migrated: {file_path}")
            return False
        
        # Skip if no logging imports found
        if 'import logging' not in content and 'from logging' not in content:
            logger.info(f"⏭️ No logging found: {file_path}")
            return False
            
        logger.info(f"🔄 Migrating: {file_path}")
        
        # Replace import logging with unified logging setup
        logging_import_pattern = r'^import logging\s*$'
        unified_logging_setup = '''import importlib.util

# Setup unified logging
script_dir = os.path.dirname(os.path.abspath(__file__))
logging_config_path = os.path.join(script_dir, "999_logging_config.py")
spec = importlib.util.spec_from_file_location("logging_config", logging_config_path)
logging_config = importlib.util.module_from_spec(spec)
spec.loader.exec_module(logging_config)
logger = logging_config.setup_logging(os.path.basename(__file__).replace('.py', ''))'''
        
        content = re.sub(logging_import_pattern, unified_logging_setup, content, flags=re.MULTILINE)
        
        # Replace common logging calls
        replacements = [
            (r'logging\.info\(', 'logger.info('),
            (r'logging\.error\(', 'logger.error('),
            (r'logging\.warning\(', 'logger.warning('),
            (r'logging\.debug\(', 'logger.debug('),
            (r'logging\.critical\(', 'logger.critical('),
            (r'logger = logging\.getLogger\([^)]*\)', '# Logger setup moved to unified configuration'),
            (r'logging\.basicConfig\([^)]*\)', '# Logging config moved to unified configuration'),
        ]
        
        for pattern, replacement in replacements:
            content = re.sub(pattern, replacement, content)
        
        # Write back the modified content
        with open(file_path, 'w') as f:
            f.write(content)
        
        logger.info(f"✅ Successfully migrated: {file_path}")
        return True
        
    except Exception as e:
        logger.error(f"❌ Failed to migrate {file_path}: {e}")
        return False

def main():
    """Main migration function"""
    
    logger.info("🚀 Starting Python script logging migration")
    
    # Find all Python scripts
    script_patterns = [
        "/home/smilax/trillium_api/scripts/python/*.py",
        "/home/smilax/trillium_api/scripts/get_slots/*.py"
    ]
    
    all_scripts = []
    for pattern in script_patterns:
        all_scripts.extend(glob.glob(pattern))
    
    # Exclude this migration script and already updated scripts
    exclude_files = [
        "migrate_to_unified_logging.py",
        "999_logging_config.py", 
        "999_sql_logging_wrapper.py",
        "test_unified_logging.py"
    ]
    
    scripts_to_migrate = [
        script for script in all_scripts 
        if not any(exclude in script for exclude in exclude_files)
    ]
    
    logger.info(f"📊 Found {len(scripts_to_migrate)} scripts to potentially migrate")
    
    migrated_count = 0
    skipped_count = 0
    
    for script_path in scripts_to_migrate:
        if migrate_python_script(script_path):
            migrated_count += 1
        else:
            skipped_count += 1
    
    logger.info(f"🎉 Migration complete! Migrated: {migrated_count}, Skipped: {skipped_count}")

if __name__ == "__main__":
    main()