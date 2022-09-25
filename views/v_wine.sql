 SELECT 
    wine.id,
    wine.wine_type_id,
    wine.region_id,
    NULLIF(wine.winery_id, 'NULL'::text)::integer AS winery_id,
    NULLIF(wine.style_id, 'NULL'::text)::integer AS style_id,
    NULLIF(wine.name, ''::text) AS wine_name,
    NULLIF(wine.seo_name, ''::text) AS wine_seo_name,
    NULLIF(replace(wine.acidity, 'NULL'::text, ''::text), ''::text)::double precision AS acidity,
    NULLIF(replace(wine.fizziness, 'NULL'::text, ''::text), ''::text)::double precision AS fizziness,
    NULLIF(replace(wine.intensity, 'NULL'::text, ''::text), ''::text)::double precision AS intensity,
    NULLIF(replace(wine.sweetness, 'NULL'::text, ''::text), ''::text)::double precision AS sweetness,
    NULLIF(replace(wine.tannin, 'NULL'::text, ''::text), ''::text)::double precision AS tannin,
    wine.ratings_count AS rating_count,
    wine.avg_rating
FROM wine;