{{ config(
    materialized='table'
) }}

WITH raw_matches AS (
    SELECT 
        id_ronda_torneo,
        ronda_torneo
    FROM {{ ref("base_atp_db__matches") }}
),

enriquecida AS(
    SELECT DISTINCT
        id_ronda_torneo,
        ronda_torneo,

        CASE ronda_torneo
            WHEN 'F'     THEN 'Final'
            WHEN 'SF'    THEN 'Semifinal'
            WHEN 'QF'    THEN 'Cuartos de final'
            WHEN 'R16'   THEN 'Octavos de final'
            WHEN 'R32'   THEN 'Dieciseisavos de final'
            WHEN 'R64'   THEN 'Treintaidosavos de final'
            WHEN 'R128'  THEN 'Sesentaicuatroavos de final'
            WHEN 'RR'    THEN 'Round Robin'
            WHEN 'BR'    THEN 'Partido por el 3er puesto'
            WHEN 'ER'    THEN 'Primera ronda'
            ELSE 'Ronda desconocida'
        END AS ronda_torneo_desc

    FROM raw_matches
)

SELECT * FROM enriquecida