-- View: public.v_wine_type

-- DROP VIEW public.v_wine_type;

CREATE OR REPLACE VIEW public.v_wine_type
 AS
 SELECT wine_type.wine_type_id AS id,
    NULLIF(wine_type.wine_type_name::text, ''::text) AS wine_type_name
   FROM wine_type;

ALTER TABLE public.v_wine_type
    OWNER TO postgres;