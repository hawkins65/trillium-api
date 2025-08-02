-- vote_latency_by_asn.sql
WITH filtered_data AS (
    SELECT 
        vt.vote_account_pubkey,
        vt.epoch,
        vt.mean_vote_latency,
        vt.vote_credits,
        vs.signatures,
        vs.skip_rate,
        vt.voted_slots,
        vs.asn,
        vs.asn_org,
        vs.stake_percentage
    FROM votes_table vt
    JOIN validator_stats vs
        ON vt.vote_account_pubkey = vs.vote_account_pubkey AND vt.epoch = vs.epoch
    WHERE vt.epoch = :epoch
),
report_header AS (
    SELECT 
        'Report Generated: ' || TO_CHAR(CURRENT_TIMESTAMP AT TIME ZONE 'UTC', 'YYYY-MM-DD HH24:MI:SS') || ' UTC | Epoch: ' || :epoch AS "Header",
        '' AS "ASN",
        '' AS "ASN Org",
        '' AS "Group Count",
        '' AS "Avg Vote Latency",
        '' AS "Min VL",
        '' AS "Max VL",
        '' AS "Median VL",
        '' AS "Avg Skip Rate",
        '' AS "Total Stake Percentage",
        0 AS sort_order
),
main_data AS (
    SELECT 
        '' AS "Header",
        asn::text AS "ASN",
        asn_org AS "ASN Org",
        COUNT(*)::text AS "Group Count",
        ROUND(AVG(mean_vote_latency), 3)::text AS "Avg Vote Latency",
        ROUND(MIN(mean_vote_latency), 3)::text AS "Min VL",
        ROUND(MAX(mean_vote_latency), 3)::text AS "Max VL",
        ROUND(AVG(mean_vote_latency), 3)::text AS "Median VL",
        ROUND(AVG(skip_rate), 3)::text AS "Avg Skip Rate",
        ROUND(SUM(stake_percentage), 3)::text AS "Total Stake Percentage",
        1 AS sort_order
    FROM filtered_data
    WHERE asn IS NOT NULL
    GROUP BY asn, asn_org
    HAVING AVG(mean_vote_latency) IS NOT NULL
)
SELECT "Header", "ASN", "ASN Org", "Group Count", "Avg Vote Latency", "Min VL", "Max VL", "Median VL", "Avg Skip Rate", "Total Stake Percentage"
FROM (
    SELECT * FROM report_header
    UNION ALL
    SELECT * FROM main_data
    ORDER BY sort_order, CASE WHEN sort_order = 1 THEN "Avg Vote Latency"::numeric ELSE 0 END DESC
) combined_results;
