
                WITH vote_account_list AS (
                    SELECT DISTINCT vote_account_pubkey
                    FROM validator_stats
                )
                INSERT INTO validator_stats_to_inspect
                SELECT vs.* 
                FROM validator_stats vs
                JOIN vote_account_list val ON vs.identity_pubkey = val.vote_account_pubkey;

                WITH vote_account_list AS (
                    SELECT DISTINCT vote_account_pubkey
                    FROM validator_stats
                )
                DELETE FROM validator_stats
                WHERE identity_pubkey IN (
                    SELECT vote_account_pubkey
                    FROM vote_account_list
                );
            