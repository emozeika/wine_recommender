 SELECT 
    upper(NULLIF(country.country_code, ''::text)) AS country_code,
    NULLIF(country.country_name, ''::text) AS country_name,
    NULLIF(country.country_seo_name, ''::text) AS country_seo_name,
    upper(NULLIF(country.country_curr_code, ''::text)) AS country_curr_code,
    NULLIF(country.country_curr_name, ''::text) AS country_curr_name
FROM country;