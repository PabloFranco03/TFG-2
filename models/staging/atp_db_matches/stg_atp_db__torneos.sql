{{ config(
    materialized='table'
) }}

WITH torneo_base AS (
    SELECT DISTINCT
        id_torneo,
        MAX(nombre_torneo) AS nombre_torneo
    FROM {{ ref('base_atp_db__matches') }}
    GROUP BY id_torneo
),

torneos_info_limpio AS (
    SELECT *
    FROM {{ ref('base_atp_db__torneos_info') }}
)

SELECT
    b.id_torneo,
    b.nombre_torneo AS nombre,
    i.pais,
    i.ciudad,
    i.primera_edicion,
    i.estadio_principal
FROM torneo_base b
LEFT JOIN torneos_info_limpio i
    ON b.id_torneo = i.id_torneo