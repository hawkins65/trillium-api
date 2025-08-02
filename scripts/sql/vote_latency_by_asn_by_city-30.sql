WITH report_header AS (
    SELECT 
        'Report Generated: ' || TO_CHAR(CURRENT_TIMESTAMP AT TIME ZONE 'UTC', 'YYYY-MM-DD HH24:MI:SS') || ' UTC | Epoch Range: ' || 
        (SELECT MAX(epoch)::text FROM votes_table) || ' to ' || 
        (SELECT (MAX(epoch) - 30)::text FROM votes_table) AS "Header",
        '' AS "Epoch",
        '' AS "ASN",
        '' AS "ASN Org",
        '' AS "City",
        '' AS "Country",
        '' AS "Group Count",
        '' AS "Avg Vote Latency",
        '' AS "Min VL",
        '' AS "Max VL",
        '' AS "Median VL",
        '' AS "Avg Skip Rate",
        '' AS "Total Stake Percentage",
        0 AS sort_order,
        0.0 AS sort_latency
),
main_data AS (
    SELECT 
        '' AS "Header",
        vt.epoch::text AS "Epoch",
        vs.asn::text AS "ASN",
        vs.asn_org AS "ASN Org", 
        vs.city AS "City",
        vs.country AS "Country",
        COUNT(*)::text AS "Group Count",
        ROUND(AVG(vt.mean_vote_latency), 3)::text AS "Avg Vote Latency",
        ROUND(MIN(vt.mean_vote_latency), 3)::text AS "Min VL",
        ROUND(MAX(vt.mean_vote_latency), 3)::text AS "Max VL",
        ROUND(AVG(vt.median_vote_latency), 3)::text AS "Median VL",
        ROUND(AVG(vs.skip_rate), 2)::text AS "Avg Skip Rate",
        ROUND(SUM(vs.stake_percentage), 3)::text AS "Total Stake Percentage",
        1 AS sort_order,
        AVG(vt.mean_vote_latency) AS sort_latency
    FROM votes_table vt
    JOIN validator_stats vs
        ON vt.vote_account_pubkey = vs.vote_account_pubkey AND vt.epoch = vs.epoch
    WHERE vt.epoch IN (
        SELECT DISTINCT epoch 
        FROM votes_table 
        WHERE epoch <= (SELECT MAX(epoch) FROM votes_table)
        ORDER BY epoch DESC
        LIMIT 30
    )
      AND vs.asn IS NOT NULL
      AND vs.city IS NOT NULL
      AND vs.city != ''
    GROUP BY vt.epoch, vs.asn, vs.asn_org, vs.city, vs.country
    HAVING AVG(vt.mean_vote_latency) IS NOT NULL
)
SELECT "Header", "Epoch", "ASN", "ASN Org", "City", "Country", "Group Count", "Avg Vote Latency", "Min VL", "Max VL", "Median VL", "Avg Skip Rate", "Total Stake Percentage"
FROM (
    SELECT * FROM report_header
    UNION ALL
    SELECT * FROM main_data
) combined_results
ORDER BY sort_order, "Epoch" DESC, sort_latency DESC;