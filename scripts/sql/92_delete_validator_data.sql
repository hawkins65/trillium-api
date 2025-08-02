
                WITH vote_account_list AS (
                    SELECT DISTINCT vote_account_pubkey
                    FROM validator_stats
                )
                INSERT INTO validator_data_to_inspect
                SELECT vd.* FROM validator_data vd
                JOIN vote_account_list val ON vd.identity_pubkey = val.vote_account_pubkey;

                WITH vote_account_list AS (
                    SELECT DISTINCT vote_account_pubkey
                    FROM validator_stats
                )
                DELETE FROM validator_data
                WHERE identity_pubkey IN (
                    SELECT vote_account_pubkey
                    FROM vote_account_list
                );
            