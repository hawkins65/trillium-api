#!/usr/bin/env python3
"""
Test script to process run0 files (JSON validator performance data) for a given epoch
Usage: python3 test_run0_processor.py <epoch_number>
"""
import os
import sys
import json
import psycopg2
import importlib.util
from typing import Dict, List, Any
from pprint import pprint

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

def load_run0_json_files(epoch_number: int) -> Dict[str, Any]:
    """Load good.json and poor.json from run0 directory for the given epoch"""
    base_directory = os.path.join(
        os.environ.get('TRILLIUM_DATA_EPOCHS', '/home/smilax/trillium_api/data/epochs'),
        f"epoch{epoch_number}",
        "run0"
    )
    
    if not os.path.exists(base_directory):
        logger.error(f"Run0 directory not found: {base_directory}")
        return {}
        
    logger.info(f"Processing run0 directory: {base_directory}")
    
    # Check what files exist
    files = os.listdir(base_directory)
    logger.info(f"Files in run0: {files}")
    
    data = {}
    
    # Load good.json
    good_file = os.path.join(base_directory, "good.json")
    if os.path.exists(good_file):
        try:
            with open(good_file, 'r') as f:
                data['good'] = json.load(f)
            logger.info(f"Loaded good.json - {len(data['good']['voters'])} good validators")
        except Exception as e:
            logger.error(f"Error loading good.json: {e}")
    else:
        logger.warning(f"good.json not found in {base_directory}")
    
    # Load poor.json
    poor_file = os.path.join(base_directory, "poor.json")
    if os.path.exists(poor_file):
        try:
            with open(poor_file, 'r') as f:
                data['poor'] = json.load(f)
            logger.info(f"Loaded poor.json - {len(data['poor']['voters'])} poor validators")
        except Exception as e:
            logger.error(f"Error loading poor.json: {e}")
    else:
        logger.warning(f"poor.json not found in {base_directory}")
        
    return data

def analyze_run0_data(data: Dict[str, Any], epoch_number: int):
    """Analyze the structure and content of run0 data"""
    logger.info(f"=== Analysis of run0 data for epoch {epoch_number} ===")
    
    if not data:
        logger.error("No data loaded!")
        return
    
    for category in ['good', 'poor']:
        if category not in data:
            logger.warning(f"No {category} data found")
            continue
            
        category_data = data[category]
        logger.info(f"\n--- {category.upper()} VALIDATORS ---")
        
        # Show metadata
        logger.info(f"Timestamp: {category_data.get('timestamp')}")
        logger.info(f"Data epochs: {category_data.get('data_epochs')}")
        logger.info(f"Cluster averages:")
        logger.info(f"  - Vote latency (vl): {category_data.get('cluster_average_vl'):.4f} ± {category_data.get('cluster_stddev_vl', 0):.4f}")
        logger.info(f"  - Last vote latency (llv): {category_data.get('cluster_average_llv'):.4f} ± {category_data.get('cluster_stddev_llv', 0):.4f}")
        logger.info(f"  - Credits vs votes (cv): {category_data.get('cluster_average_cv'):.4f} ± {category_data.get('cluster_stddev_cv', 0):.4f}")
        
        # Show thresholds
        logger.info(f"Thresholds:")
        logger.info(f"  - VL: {category_data.get('threshold_vl')}")
        logger.info(f"  - LLV: {category_data.get('threshold_llv')}")
        logger.info(f"  - CV: {category_data.get('threshold_cv')}")
        
        voters = category_data.get('voters', [])
        logger.info(f"Number of voters: {len(voters)}")
        
        if voters:
            # Show first few validators as examples
            logger.info(f"\nFirst 3 {category} validators:")
            for i, voter in enumerate(voters[:3]):
                logger.info(f"  {i+1}. Vote: {voter['vote_pubkey'][:8]}...")
                logger.info(f"     Identity: {voter['identity_pubkey'][:8]}...")
                logger.info(f"     Stake: {voter['stake']:,} lamports")
                
                # Handle None values in display
                vl_str = f"{voter['average_vl']:.4f}" if voter['average_vl'] is not None else "None"
                llv_str = f"{voter['average_llv']:.4f}" if voter['average_llv'] is not None else "None"
                cv_str = f"{voter['average_cv']:.4f}" if voter['average_cv'] is not None else "None"
                
                logger.info(f"     VL: {vl_str}, LLV: {llv_str}, CV: {cv_str}")
                logger.info(f"     Foundation staked: {voter['is_foundation_staked']}")
                
            # Show statistics
            total_stake = sum(v['stake'] for v in voters)
            foundation_count = sum(1 for v in voters if v['is_foundation_staked'])
            logger.info(f"\nStatistics for {category}:")
            logger.info(f"  - Total stake: {total_stake:,} lamports")
            logger.info(f"  - Foundation staked validators: {foundation_count}/{len(voters)} ({100*foundation_count/len(voters):.1f}%)")
            
            # Performance metrics ranges (filter out None values)
            vl_values = [v['average_vl'] for v in voters if v['average_vl'] is not None]
            llv_values = [v['average_llv'] for v in voters if v['average_llv'] is not None]
            cv_values = [v['average_cv'] for v in voters if v['average_cv'] is not None]
            
            if vl_values:
                logger.info(f"  - Vote latency range: {min(vl_values):.4f} - {max(vl_values):.4f} ({len(vl_values)}/{len(voters)} have values)")
            else:
                logger.info(f"  - Vote latency range: No valid values found")
                
            if llv_values:
                logger.info(f"  - Last vote latency range: {min(llv_values):.4f} - {max(llv_values):.4f} ({len(llv_values)}/{len(voters)} have values)")
            else:
                logger.info(f"  - Last vote latency range: No valid values found")
                
            if cv_values:
                logger.info(f"  - Credits vs votes range: {min(cv_values):.4f} - {max(cv_values):.4f} ({len(cv_values)}/{len(voters)} have values)")
            else:
                logger.info(f"  - Credits vs votes range: No valid values found")
                
            # Count None values
            none_vl = sum(1 for v in voters if v['average_vl'] is None)
            none_llv = sum(1 for v in voters if v['average_llv'] is None)
            none_cv = sum(1 for v in voters if v['average_cv'] is None)
            
            if none_vl > 0 or none_llv > 0 or none_cv > 0:
                logger.info(f"  - None values: VL={none_vl}, LLV={none_llv}, CV={none_cv}")

def update_production_database(data: Dict[str, Any], epoch_number: int):
    """Update the production validator_xshin table with run0 data"""
    logger.info(f"\n=== Updating Production Database for Epoch {epoch_number} ===")
    
    if not data:
        logger.error("No data to process")
        return
        
    try:
        conn = psycopg2.connect(**db_params)
        cursor = conn.cursor()
        
        # Prepare data for insertion (same format as the main script)
        all_data = []
        processed_files = []
        
        for category in ['good', 'poor']:
            if category not in data:
                continue
                
            processed_files.append(f"{category}.json")
            voters = data[category].get('voters', [])
            logger.info(f"Processing {len(voters)} {category} validators...")
            
            none_values_count = 0
            
            for voter in voters:
                # Handle None values by converting to NULL-compatible values
                average_vl = voter.get('average_vl')
                average_llv = voter.get('average_llv')
                average_cv = voter.get('average_cv')
                
                # Count None values for logging
                if average_vl is None or average_llv is None or average_cv is None:
                    none_values_count += 1
                
                all_data.append((
                    epoch_number,
                    voter['vote_pubkey'],
                    voter['identity_pubkey'],
                    voter['stake'],
                    average_vl,  # Can be None, will become NULL in PostgreSQL
                    average_llv,
                    average_cv,
                    voter.get('vo', False),
                    voter['is_foundation_staked']
                ))
            
            logger.info(f"📈 Processed {len(voters)} {category} validators ({none_values_count} with None performance values)")
        
        # Insert all data using the same upsert logic as the production script
        if all_data:
            logger.info(f"💾 Inserting {len(all_data)} validator records into validator_xshin table...")
            cursor.executemany("""
                INSERT INTO validator_xshin (
                    epoch, vote_account_pubkey, identity_pubkey, stake, 
                    average_vl, average_llv, average_cv, vo, is_foundation_staked
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (epoch, identity_pubkey) DO UPDATE SET
                    vote_account_pubkey = EXCLUDED.vote_account_pubkey,
                    stake = EXCLUDED.stake,
                    average_vl = EXCLUDED.average_vl,
                    average_llv = EXCLUDED.average_llv,
                    average_cv = EXCLUDED.average_cv,
                    vo = EXCLUDED.vo,
                    is_foundation_staked = EXCLUDED.is_foundation_staked
            """, all_data)
            conn.commit()
            
            logger.info(f"✅ Successfully processed run0 JSON files:")
            logger.info(f"   📊 Files processed: {', '.join(processed_files)}")
            logger.info(f"   📈 Total validators: {len(all_data)}")
            logger.info(f"   💾 Upserted into validator_xshin table for epoch {epoch_number}")
        
        # Run some verification queries on the production table
        logger.info(f"\n--- Production Database Verification ---")
        
        # Count records for this epoch
        cursor.execute("SELECT COUNT(*) FROM validator_xshin WHERE epoch = %s", (epoch_number,))
        total_count = cursor.fetchone()[0]
        logger.info(f"Total records in validator_xshin for epoch {epoch_number}: {total_count}")
        
        # Check for records with valid performance metrics vs None values
        cursor.execute("""
            SELECT 
                COUNT(*) as total,
                COUNT(average_vl) as has_vl,
                COUNT(average_llv) as has_llv,
                COUNT(average_cv) as has_cv
            FROM validator_xshin 
            WHERE epoch = %s
        """, (epoch_number,))
        result = cursor.fetchone()
        logger.info(f"Performance metrics availability:")
        logger.info(f"  - Total records: {result[0]}")
        logger.info(f"  - With vote latency: {result[1]} ({100*result[1]/result[0]:.1f}%)")
        logger.info(f"  - With last vote latency: {result[2]} ({100*result[2]/result[0]:.1f}%)")
        logger.info(f"  - With credits vs votes: {result[3]} ({100*result[3]/result[0]:.1f}%)")
        
        # Top 5 validators by stake for this epoch
        cursor.execute("""
            SELECT vote_account_pubkey, stake, average_vl, average_llv, average_cv, is_foundation_staked
            FROM validator_xshin 
            WHERE epoch = %s 
            ORDER BY stake DESC 
            LIMIT 5
        """, (epoch_number,))
        results = cursor.fetchall()
        logger.info(f"\nTop 5 validators by stake in epoch {epoch_number}:")
        for i, (vote_pubkey, stake, vl, llv, cv, is_foundation) in enumerate(results, 1):
            vl_str = f"{vl:.3f}" if vl is not None else "None"
            llv_str = f"{llv:.3f}" if llv is not None else "None"
            cv_str = f"{cv:.3f}" if cv is not None else "None"
            foundation_str = "Foundation" if is_foundation else "Non-foundation"
            logger.info(f"  {i}. {vote_pubkey[:8]}... - Stake: {stake:,}, VL: {vl_str}, LLV: {llv_str}, CV: {cv_str} ({foundation_str})")
        
        # Foundation staked analysis for this epoch
        cursor.execute("""
            SELECT is_foundation_staked, COUNT(*), 
                   AVG(stake)::BIGINT as avg_stake, 
                   AVG(average_vl) as avg_vl,
                   AVG(average_llv) as avg_llv,
                   AVG(average_cv) as avg_cv
            FROM validator_xshin 
            WHERE epoch = %s
            GROUP BY is_foundation_staked 
            ORDER BY is_foundation_staked
        """, (epoch_number,))
        results = cursor.fetchall()
        logger.info(f"\nFoundation staking analysis for epoch {epoch_number}:")
        for is_foundation, count, avg_stake, avg_vl, avg_llv, avg_cv in results:
            foundation_str = "Foundation staked" if is_foundation else "Non-foundation staked"
            vl_str = f"{avg_vl:.3f}" if avg_vl is not None else "N/A"
            llv_str = f"{avg_llv:.3f}" if avg_llv is not None else "N/A"
            cv_str = f"{avg_cv:.3f}" if avg_cv is not None else "N/A"
            logger.info(f"  {foundation_str}: {count} validators")
            logger.info(f"    Avg stake: {avg_stake:,}, VL: {vl_str}, LLV: {llv_str}, CV: {cv_str}")
        
        logger.info(f"\n✅ Production validator_xshin table updated successfully for epoch {epoch_number}!")
        logger.info(f"Query with: SELECT * FROM validator_xshin WHERE epoch = {epoch_number} LIMIT 10;")
        
        cursor.close()
        conn.close()
        
    except Exception as e:
        logger.error(f"Database operation failed: {e}")
        if 'conn' in locals():
            conn.rollback()
            conn.close()

def main():
    if len(sys.argv) != 2:
        print("Usage: python3 test_run0_processor.py <epoch_number>")
        print("Example: python3 test_run0_processor.py 828")
        sys.exit(1)
    
    try:
        epoch_number = int(sys.argv[1])
    except ValueError:
        logger.error("Epoch number must be an integer")
        sys.exit(1)
    
    logger.info(f"🚀 Processing run0 files for epoch {epoch_number}")
    
    # Load the JSON data
    data = load_run0_json_files(epoch_number)
    
    if not data:
        logger.error("No data loaded, exiting")
        sys.exit(1)
    
    # Analyze the data
    analyze_run0_data(data, epoch_number)
    
    # Update the production database
    update_production_database(data, epoch_number)
    
    logger.info("🎉 Run0 processing completed successfully!")

if __name__ == "__main__":
    main()