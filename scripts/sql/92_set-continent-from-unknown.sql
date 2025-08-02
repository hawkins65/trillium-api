UPDATE validator_stats
SET continent = CASE 
    WHEN country IN (
        'Albania', 'Austria', 'Belgium', 'Bulgaria', 'Cyprus', 
        'Czech Republic', 'Czechia', 'Finland', 'France', 'Germany', 
        'Ireland', 'Italy', 'Latvia', 'Lithuania', 'Luxembourg', 
        'Netherlands', 'Norway', 'Poland', 
        'Portugal', 'Romania', 'Russia', 'Slovakia', 'Spain', 
        'Sweden', 'Switzerland', 'Ukraine', 'United Kingdom'
    ) THEN 'Europe'
    WHEN country IN (
        'Anguilla', 'Canada', 'Cayman Islands', 'Mexico', 'United States'
    ) THEN 'North America'
    WHEN country IN (
        'Argentina', 'Brazil', 'Chile', 'Colombia', 'Peru'
    ) THEN 'South America'
    WHEN country IN (
        'China', 'Hong Kong', 'India', 'Israel', 'Japan', 
        'Saudi Arabia', 'Singapore', 'South Korea', 'Turkey'
    ) THEN 'Asia'
    WHEN country = 'Australia' THEN 'Oceania'
    WHEN country = 'South Africa' THEN 'Africa'
    ELSE continent
END;