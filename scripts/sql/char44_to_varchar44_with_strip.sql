DO $$
DECLARE
    tbl_name TEXT;
    column_exists BOOLEAN;
BEGIN
    RAISE NOTICE 'Starting the table modification process to revert char(44) to varchar(44).';

    -- List of tables to modify
    FOR tbl_name IN 
        SELECT unnest(ARRAY[
            'epoch_aggregate_data',
            'epoch_votes',
            'leader_schedule',
            'stake_accounts',
            'validator_data',
            'validator_info',
            'validator_stats',
            'validator_stats_to_inspect',
            'validator_xshin'
        ])
    LOOP
        RAISE NOTICE 'Processing table: %', tbl_name;

        -- Check and modify 'identity_pubkey' if it exists
        SELECT EXISTS (
            SELECT 1
            FROM information_schema.columns
            WHERE table_name = tbl_name AND column_name = 'identity_pubkey'
        ) INTO column_exists;

        IF column_exists THEN
            RAISE NOTICE 'Column "identity_pubkey" exists in table "%". Altering column...', tbl_name;

            -- Remove padding and convert to varchar(44)
            EXECUTE format(
                'ALTER TABLE %I ALTER COLUMN identity_pubkey TYPE varchar(44) USING TRIM(identity_pubkey);', 
                tbl_name
            );

            RAISE NOTICE 'Column "identity_pubkey" in table "%" successfully altered to varchar(44).', tbl_name;
        ELSE
            RAISE NOTICE 'Column "identity_pubkey" does not exist in table "%". Skipping.', tbl_name;
        END IF;

        -- Check and modify 'vote_account_pubkey' if it exists
        SELECT EXISTS (
            SELECT 1
            FROM information_schema.columns
            WHERE table_name = tbl_name AND column_name = 'vote_account_pubkey'
        ) INTO column_exists;

        IF column_exists THEN
            RAISE NOTICE 'Column "vote_account_pubkey" exists in table "%". Altering column...', tbl_name;

            -- Remove padding and convert to varchar(44)
            EXECUTE format(
                'ALTER TABLE %I ALTER COLUMN vote_account_pubkey TYPE varchar(44) USING TRIM(vote_account_pubkey);', 
                tbl_name
            );

            RAISE NOTICE 'Column "vote_account_pubkey" in table "%" successfully altered to varchar(44).', tbl_name;
        ELSE
            RAISE NOTICE 'Column "vote_account_pubkey" does not exist in table "%". Skipping.', tbl_name;
        END IF;

    END LOOP;

    RAISE NOTICE 'Table modification process completed.';
END $$;
