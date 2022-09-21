-- View: public.v_region

-- DROP VIEW public.v_region;

CREATE OR REPLACE VIEW public.v_region
 AS
 SELECT region.id,
    NULLIF(region.name, ''::text) AS name,
    NULLIF(region.name_english, ''::text) AS name_english,
    NULLIF(region.country_name, ''::text) AS country_name,
    upper(NULLIF(region.country_code, ''::text)) AS country_code
   FROM region;

ALTER TABLE public.v_region
    OWNER TO postgres;