 SELECT 
    winery.id,
    NULLIF(replace(winery.name, 'NULL'::text, ''::text), ''::text) AS name,
    NULLIF(replace(winery.seo_name, 'NULL'::text, ''::text), ''::text) AS seo_name
FROM winery;