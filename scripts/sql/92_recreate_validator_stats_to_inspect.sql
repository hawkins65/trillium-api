
                -- Drop the existing table if it exists
                DROP TABLE IF EXISTS validator_stats_to_inspect;

                -- Create validator_stats_to_inspect as an empty copy of validator_stats
                CREATE TABLE validator_stats_to_inspect (LIKE validator_stats INCLUDING ALL);
            