WITH stats AS (
    SELECT 
        AVG(duration) AS mean_duration,
        STDDEV_SAMP(duration) AS stddev_duration
    FROM slot_duration
    WHERE epoch = 819
        AND duration IS NOT NULL
        AND duration > 0
),
outliers AS (
    SELECT 
        block_slot,
        duration,
        (duration < (stats.mean_duration - 2 * stats.stddev_duration)) AS is_low_outlier,
        (duration > (stats.mean_duration + 2 * stats.stddev_duration)) AS is_high_outlier
    FROM slot_duration
    CROSS JOIN stats
    WHERE epoch = 819
        AND duration IS NOT NULL
        AND duration > 0
),
counts AS (
    SELECT 
        COUNT(*) FILTER (WHERE is_low_outlier) AS low_outliers,
        COUNT(*) FILTER (WHERE is_high_outlier) AS high_outliers,
        COUNT(*) AS total_slots
    FROM outliers
)
SELECT 
    ROUND(CAST(stats.mean_duration / 1000000.0 AS numeric), 2) AS mean_duration_ms,
    ROUND(CAST(stats.stddev_duration / 1000000.0 AS numeric), 2) AS stddev_duration_ms,
    counts.low_outliers,
    ROUND(CAST(counts.low_outliers::FLOAT / NULLIF(counts.total_slots, 0) * 100 AS numeric), 2) AS low_outliers_percent,
    counts.high_outliers,
    ROUND(CAST(counts.high_outliers::FLOAT / NULLIF(counts.total_slots, 0) * 100 AS numeric), 2) AS high_outliers_percent,
    counts.total_slots,
    ROUND(CAST((stats.mean_duration - 2 * stats.stddev_duration) / 1000000.0 AS numeric), 2) AS low_outlier_threshold_ms,
    ROUND(CAST((stats.mean_duration + 2 * stats.stddev_duration) / 1000000.0 AS numeric), 2) AS high_outlier_threshold_ms
FROM stats
CROSS JOIN counts;