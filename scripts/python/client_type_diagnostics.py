#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Diagnostic script to compare epochs 824 and 825 client_type data
Run this to see what changed between the epochs
"""

import psycopg2
import pandas as pd
import logging

# Import your database configuration
from db_config import db_params

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def get_db_connection(db_params):
    conn = psycopg2.connect(**db_params)
    return conn

def run_diagnostic_queries():
    """Run diagnostic queries to compare epochs 824 and 825"""
    
    try:
        with get_db_connection(db_params) as conn:
            with conn.cursor() as cur:
                
                print("="*80)
                print("DIAGNOSTIC ANALYSIS: EPOCHS 824 vs 825")
                print("="*80)
                
                # Query 1: Client type distribution for both epochs
                print("\n1. CLIENT TYPE DISTRIBUTION BY EPOCH:")
                print("-" * 50)
                
                cur.execute("""
                    SELECT 
                        epoch,
                        client_type,
                        COUNT(*) as validator_count,
                        ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(PARTITION BY epoch), 2) as percentage
                    FROM validator_stats 
                    WHERE epoch IN (824, 825)
                    GROUP BY epoch, client_type
                    ORDER BY epoch, client_type;
                """)
                
                results = cur.fetchall()
                df1 = pd.DataFrame(results, columns=['epoch', 'client_type', 'validator_count', 'percentage'])
                print(df1.to_string(index=False))
                
                # Query 2: Missing client_type data comparison
                print("\n\n2. MISSING CLIENT_TYPE DATA COMPARISON:")
                print("-" * 50)
                
                cur.execute("""
                    SELECT 
                        epoch,
                        COUNT(*) as total_validators,
                        COUNT(client_type) as validators_with_client_type,
                        COUNT(*) - COUNT(client_type) as validators_missing_client_type,
                        ROUND((COUNT(*) - COUNT(client_type)) * 100.0 / COUNT(*), 2) as missing_percentage
                    FROM validator_stats 
                    WHERE epoch IN (824, 825)
                    GROUP BY epoch
                    ORDER BY epoch;
                """)
                
                results = cur.fetchall()
                df2 = pd.DataFrame(results, columns=['epoch', 'total_validators', 'validators_with_client_type', 'validators_missing_client_type', 'missing_percentage'])
                print(df2.to_string(index=False))
                
                # Query 3: Raw client_type values and their data types
                print("\n\n3. RAW CLIENT_TYPE VALUES FOR EPOCH 824:")
                print("-" * 50)
                
                cur.execute("""
                    SELECT 
                        client_type,
                        pg_typeof(client_type) as data_type,
                        COUNT(*) as count
                    FROM validator_stats 
                    WHERE epoch = 824
                    GROUP BY client_type, pg_typeof(client_type)
                    ORDER BY client_type;
                """)
                
                results = cur.fetchall()
                df3 = pd.DataFrame(results, columns=['client_type', 'data_type', 'count'])
                print(df3.to_string(index=False))
                
                print("\n\n4. RAW CLIENT_TYPE VALUES FOR EPOCH 825:")
                print("-" * 50)
                
                cur.execute("""
                    SELECT 
                        client_type,
                        pg_typeof(client_type) as data_type,
                        COUNT(*) as count
                    FROM validator_stats 
                    WHERE epoch = 825
                    GROUP BY client_type, pg_typeof(client_type)
                    ORDER BY client_type;
                """)
                
                results = cur.fetchall()
                df4 = pd.DataFrame(results, columns=['client_type', 'data_type', 'count'])
                print(df4.to_string(index=False))
                
                # Query 5: Sample of validators with their client_type for both epochs
                print("\n\n5. SAMPLE VALIDATORS - EPOCH 824 vs 825:")
                print("-" * 50)
                
                cur.execute("""
                    SELECT 
                        '824' as epoch,
                        LEFT(identity_pubkey, 8) as validator_short,
                        client_type,
                        name
                    FROM validator_stats vs
                    LEFT JOIN validator_info vi ON vs.identity_pubkey = vi.identity_pubkey
                    WHERE vs.epoch = 824
                    ORDER BY vs.stake_percentage DESC
                    LIMIT 10
                    
                    UNION ALL
                    
                    SELECT 
                        '825' as epoch,
                        LEFT(identity_pubkey, 8) as validator_short,
                        client_type,
                        name
                    FROM validator_stats vs
                    LEFT JOIN validator_info vi ON vs.identity_pubkey = vi.identity_pubkey
                    WHERE vs.epoch = 825
                    ORDER BY vs.stake_percentage DESC
                    LIMIT 10;
                """)
                
                results = cur.fetchall()
                df5 = pd.DataFrame(results, columns=['epoch', 'validator_short', 'client_type', 'name'])
                print(df5.to_string(index=False))
                
                # Query 6: Check if there are any validators that exist in 824 but not 825 or vice versa
                print("\n\n6. VALIDATOR COUNT COMPARISON:")
                print("-" * 50)
                
                cur.execute("""
                    WITH epoch_counts AS (
                        SELECT 
                            824 as epoch,
                            COUNT(DISTINCT identity_pubkey) as validator_count
                        FROM validator_stats 
                        WHERE epoch = 824
                        
                        UNION ALL
                        
                        SELECT 
                            825 as epoch,
                            COUNT(DISTINCT identity_pubkey) as validator_count
                        FROM validator_stats 
                        WHERE epoch = 825
                    )
                    SELECT * FROM epoch_counts
                    ORDER BY epoch;
                """)
                
                results = cur.fetchall()
                df6 = pd.DataFrame(results, columns=['epoch', 'validator_count'])
                print(df6.to_string(index=False))
                
                # Query 7: Check for any data collection issues
                print("\n\n7. DATA COMPLETENESS CHECK:")
                print("-" * 50)
                
                cur.execute("""
                    SELECT 
                        epoch,
                        COUNT(*) as total_records,
                        COUNT(client_type) as has_client_type,
                        COUNT(stake_percentage) as has_stake,
                        COUNT(continent) as has_continent,
                        COUNT(version) as has_version
                    FROM validator_stats 
                    WHERE epoch IN (824, 825)
                    GROUP BY epoch
                    ORDER BY epoch;
                """)
                
                results = cur.fetchall()
                df7 = pd.DataFrame(results, columns=['epoch', 'total_records', 'has_client_type', 'has_stake', 'has_continent', 'has_version'])
                print(df7.to_string(index=False))
                
                print("\n" + "="*80)
                print("ANALYSIS COMPLETE")
                print("="*80)
                
    except Exception as e:
        logger.error(f"Error running diagnostic queries: {e}")
        raise

if __name__ == "__main__":
    run_diagnostic_queries()