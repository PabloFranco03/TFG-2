{{ config(
    materialized='table'
) }}

WITH raw_matches AS (
    SELECT 
        id_superficie,
        superficie
    FROM {{ ref("base_atp_db__matches") }}
),

enriquecida AS (
    SELECT DISTINCT
        id_superficie,
        superficie,

        CASE superficie
            WHEN 'Clay' THEN 'Tierra batida'
            WHEN 'Hard' THEN 'Dura'
            WHEN 'Grass' THEN 'Hierba'
            WHEN 'Carpet' THEN 'Moqueta'
            ELSE 'Desconocida'
        END AS nombre_superficie,

        CASE
            WHEN superficie IN ('Hard', 'Grass', 'Carpet') THEN TRUE
            WHEN superficie = 'Clay' THEN FALSE
            ELSE NULL
        END AS es_rapida

    FROM raw_matches
)

SELECT * FROM enriquecida