
                UPDATE validator_stats
                SET 
                    mev_to_jito_block_engine = CASE 
                        WHEN epoch < 752 THEN mev_earned / 0.95  -- Includes 751
                        WHEN epoch > 751 THEN mev_earned / 0.97
                        ELSE NULL
                    END,
                    mev_to_jito_tip_router = CASE
                        WHEN epoch > 751 THEN mev_earned / 0.97  -- Excludes 751
                        ELSE NULL
                    END
                WHERE epoch BETWEEN 752 AND 752;
            