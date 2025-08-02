-- Step 1: Export records with NULL cities to a CSV file with specified columns
\copy (SELECT DISTINCT ip, city, country, continent, asn, asn_org, region FROM validator_stats WHERE city IS NULL) TO '92_null-cities.csv' WITH CSV HEADER;
\echo 'file created - 92_null-cities.csv'

-- Step 1.5: Update existing city_metro_mapping rows where metro = 'N/A' to set metro = city
UPDATE city_metro_mapping
SET metro = city
WHERE metro = 'N/A';

-- Step 2: Insert any missing cities from validator_stats into city_metro_mapping with metro set to city
INSERT INTO city_metro_mapping (city, metro)
SELECT DISTINCT vs.city, vs.city
FROM validator_stats vs
LEFT JOIN city_metro_mapping cm ON vs.city = cm.city
WHERE cm.city IS NULL
  AND vs.city IS NOT NULL
ON CONFLICT (city) DO NOTHING;

-- Step 3: Export cities with metro matching city (instead of 'N/A') to a CSV file with validator_stats columns
\copy (SELECT DISTINCT cm.city, cm.metro, vs.ip, vs.country, vs.continent FROM city_metro_mapping cm LEFT JOIN validator_stats vs ON cm.city = vs.city WHERE cm.metro = cm.city ORDER BY cm.city) TO '92_missing-city-to-metro.csv' WITH CSV HEADER;
\echo 'file created - 92_missing-city-to-metro.csv'

-- Step 4: Update validator_stats with metro values from city_metro_mapping
UPDATE validator_stats vs
SET metro = COALESCE(cm.metro, vs.city)  -- Changed from 'N/A' to vs.city
FROM city_metro_mapping cm
WHERE vs.city = cm.city;