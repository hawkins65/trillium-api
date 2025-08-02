
                DELETE FROM validator_stats
                WHERE vote_account_pubkey LIKE '%1111111111111111111111111111%'
                OR identity_pubkey LIKE '%1111111111111111111111111111%';
            