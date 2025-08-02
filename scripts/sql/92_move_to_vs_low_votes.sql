INSERT INTO validator_stats_low_votes
SELECT * FROM validator_stats
WHERE votes_cast < 43000;
              
-- DELETE FROM validator_stats
-- WHERE votes_cast < 43000;