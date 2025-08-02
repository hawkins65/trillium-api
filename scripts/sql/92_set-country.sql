-- Create and populate the temporary table with country mappings
CREATE TEMPORARY TABLE country_mappings (
    code VARCHAR(50),
    full_name VARCHAR(50)
);

INSERT INTO country_mappings (code, full_name) VALUES
('AT', 'Austria'),
('CA', 'Canada'),
('CZ', 'Czech Republic'),
('DE', 'Germany'),
('FR', 'France'),
('GB', 'United Kingdom'),
('HK', 'Hong Kong'),
('IL', 'Israel'),
('JP', 'Japan'),
('LT', 'Lithuania'),
('Republic of Lithuania', 'Lithuania'),
('LU', 'Luxembourg'),
('LV', 'Latvia'),
('NL', 'Netherlands'),
('NO', 'Norway'),
('PL', 'Poland'),
('RO', 'Romania'),
('RU', 'Russia'),
('UA', 'Ukraine'),
('US', 'United States'),
('Czechia', 'Czech Republic'),
('BG', 'Bulgaria'),
('FI', 'Finland'),
('IT', 'Italy'),
('SG', 'Singapore'),
('IE', 'Ireland'),
('SE', 'Sweden');

-- Update the validator_stats table using the temporary table
UPDATE validator_stats vs
SET country = (
    SELECT cm.full_name
    FROM country_mappings cm
    WHERE TRIM(vs.country) = cm.code
       OR TRIM(vs.country) = cm.full_name
    LIMIT 1  -- Ensure only one row is returned
)
WHERE EXISTS (
    SELECT 1
    FROM country_mappings cm
    WHERE TRIM(vs.country) = cm.code
       OR TRIM(vs.country) = cm.full_name
);

-- Handle 'The Netherlands' explicitly
UPDATE validator_stats
SET country = 'Netherlands'
WHERE TRIM(country) = 'The Netherlands';

-- Clean up the temporary table
DROP TABLE country_mappings;