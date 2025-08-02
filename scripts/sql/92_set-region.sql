UPDATE validator_stats vs
SET region = cr.region
FROM country_regions cr
WHERE UPPER(vs.country) = UPPER(cr.country);