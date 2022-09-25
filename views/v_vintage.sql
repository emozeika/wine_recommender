 SELECT vintage.id,
    NULLIF(vintage.name, ''::text) AS vintage_name,
    NULLIF(vintage.seo_name, ''::text) AS vintage_seo_name,
    NULLIF(replace(vintage.year, 'N.V.'::text, ''::text), ''::text)::integer AS year,
    vintage.avg_rating,
    vintage.rating_count,
    vintage.wine_id
FROM vintage;