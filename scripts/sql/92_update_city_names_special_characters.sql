CREATE OR REPLACE FUNCTION remove_diacritics(text) RETURNS text AS $$
SELECT translate(
    $1,
    'ŠáàâãäÁÀÂÃÄéèêëÉÈÊËíìîïÍÌÎÏóòôõöÓÒÔÕÖúùûüÚÙÛÜçÇñÑōŌ''’‘',
    'SaaaaaAAAAAeeeeEEEEiiiiIIIIoooooOOOOOuuuuUUUUcCnNoO'
);
$$ LANGUAGE SQL IMMUTABLE;

UPDATE validator_stats
SET city = remove_diacritics(city),
    metro = remove_diacritics(metro);

UPDATE city_metro_mapping
SET city = remove_diacritics(city),
    metro = remove_diacritics(metro);