
                -- Drop the existing table if it exists
                DROP TABLE IF EXISTS validator_data_to_inspect;

                -- Create validator_data_to_inspect as an empty copy of validator_data
                CREATE TABLE validator_data_to_inspect (LIKE validator_data INCLUDING ALL);
            