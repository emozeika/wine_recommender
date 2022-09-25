 SELECT 
    style.id,
    NULLIF(style.name, ''::text) AS wine_style_name,
    NULLIF(style.seo_name, ''::text) AS wine_style_seo_name,
    NULLIF(style.description, ''::text) AS description
FROM style;