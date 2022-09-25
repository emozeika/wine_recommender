 SELECT 
    grape.id,
    NULLIF(grape.name, ''::text) AS wine_name,
    NULLIF(grape.seo_name, ''::text) AS wine_seo_name
FROM grape;