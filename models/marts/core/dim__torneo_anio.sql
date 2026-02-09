{{ config(
    materialized = 'table',
    liquid_clustered_by = ['id_torneo', 'anio_inicio']
) }}

WITH torneos_anio AS (
    SELECT *
    FROM {{ ref('stg_atp_db__torneos_anio') }}
)

select 
    id_torneo_anio,
    id_torneo,
    id_superficie,
    id_nivel_torneo,        
    fecha_inicio,
    anio_inicio,
    mes_inicio,
    total_jugadores 
from torneos_anio