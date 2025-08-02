CREATE OR REPLACE FUNCTION update_jito_blacklist(file_path TEXT, input_epoch INTEGER)
RETURNS VOID AS $$
BEGIN
    -- Create a temporary table to hold the CSV data
    CREATE TEMP TABLE temp_pubkeys (vote_account_pubkey VARCHAR(44));

    -- Copy data from the CSV file into the temporary table
    EXECUTE format('COPY temp_pubkeys (vote_account_pubkey) FROM %L', file_path);

    -- Insert or update records in jito_blacklist
    INSERT INTO jito_blacklist (vote_account_pubkey, epoch)
    SELECT vote_account_pubkey, input_epoch
    FROM temp_pubkeys
    ON CONFLICT (vote_account_pubkey, epoch) DO NOTHING;

    -- Drop the temporary table
    DROP TABLE temp_pubkeys;
END;
$$ LANGUAGE plpgsql;

-- Example usage:
-- SELECT update_jito_blacklist('/path/to/jito-blacklist.csv', 123);