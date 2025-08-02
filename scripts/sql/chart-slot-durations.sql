WITH stats AS (
    SELECT 
        AVG(duration) AS mean_duration,
        STDDEV_SAMP(duration) AS stddev_duration
    FROM slot_duration
    WHERE epoch = 819
        AND duration IS NOT NULL
        AND duration > 0
),
bins AS (
    SELECT 
        -- Bin durations into 20 ms intervals
        FLOOR(duration / 20000000.0) * 20 AS bin_start_ms,
        COUNT(*) AS slot_count
    FROM slot_duration
    WHERE epoch = 819
        AND duration IS NOT NULL
        AND duration > 0
    GROUP BY FLOOR(duration / 20000000.0)
),
thresholds AS (
    SELECT 
        ROUND(mean_duration / 1000000.0, 2) AS mean_duration_ms,
        ROUND((mean_duration - 2 * stddev_duration) / 1000000.0, 2) AS low_outlier_threshold_ms,
        ROUND((mean_duration + 2 * stddev_duration) / 1000000.0, 2) AS high_outlier_threshold_ms
    FROM stats
)
SELECT 
    bins.bin_start_ms,
    bins.slot_count,
    thresholds.mean_duration_ms,
    thresholds.low_outlier_threshold_ms,
    thresholds.high_outlier_threshold_ms
FROM bins
CROSS JOIN thresholds
ORDER BY bins.bin_start_ms;