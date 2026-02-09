{{ config(
    materialized='table',
    liquid_clustered_by=['id_torneo', 'anio_inicio']
) }}

WITH raw_matches AS (
    SELECT
        id_torneo_anio,
        id_torneo,
        id_superficie, 
        id_nivel_torneo,
        fecha_inicio,
        total_jugadores 
    FROM {{ ref('base_atp_db__matches') }}
),

campos_torneo_anio AS (
    SELECT DISTINCT
        id_torneo_anio,
        id_torneo,
        id_superficie,
        id_nivel_torneo,        
        fecha_inicio,
        year(fecha_inicio) AS anio_inicio,   -- Spark SQL
        month(fecha_inicio) AS mes_inicio,   -- Spark SQL
        total_jugadores 
    FROM raw_matches
)

SELECT * FROM campos_torneo_anio