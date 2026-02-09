{{ config(
    materialized='table'
) }}

WITH raw_matches AS (
    SELECT 
        id_nivel_torneo,
        nivel_torneo
    FROM {{ ref("base_atp_db__matches") }}
),

enriquecida AS(
    SELECT DISTINCT
        id_nivel_torneo,
        nivel_torneo,

        CASE nivel_torneo
            WHEN 'A' THEN 'Torneo ATP'
            WHEN 'G' THEN 'Grand Slam'
            WHEN 'O' THEN 'Olimpiadas'
            WHEN 'F' THEN 'Next Gen Finals'
            WHEN 'M' THEN 'Master 1000'
            ELSE 'Desconocida'
        END AS nivel_torneo_desc

    FROM raw_matches
)

SELECT * FROM enriquecida