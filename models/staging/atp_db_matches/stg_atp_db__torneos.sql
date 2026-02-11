{{ config(
    materialized='table'
) }}

WITH torneo_base AS (
    SELECT DISTINCT
        id_torneo,
        MAX(nombre_torneo) AS nombre_torneo
    FROM {{ ref('base_atp_db__matches') }}
    GROUP BY id_torneo
)

SELECT
    id_torneo,
    nombre_torneo AS nombre
FROM torneo_base 