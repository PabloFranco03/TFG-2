{{ config(
    materialized = 'table',
    liquid_clustered_by = ['id_partido']
) }}

WITH partidos AS (
    SELECT
        m.id_partido,
        m.match_id_unificada AS match_id_unificada_atp, -- Renombrado para claridad
        m.id_ganador,
        m.id_perdedor,
        
        -- Datos del Grand Slam (si existen)
        b1.match_id AS match_id_gran_slam_gan1,
        b2.match_id AS match_id_gran_slam_gan2,
        b1.match_id_unificada_gan1,
        b2.match_id_unificada_gan2,
        b1.player1 AS player1_gs,
        b1.player2 AS player2_gs

    FROM {{ ref('base_atp_db__matches') }} m
    -- Intentamos cruzar asumiendo que el Ganador ATP = Jugador 1 GS
    LEFT JOIN {{ ref('base_atp_db__matches_gran_slam') }} b1
        ON m.match_id_unificada = b1.match_id_unificada_gan1
    -- Intentamos cruzar asumiendo que el Ganador ATP = Jugador 2 GS
    LEFT JOIN {{ ref('base_atp_db__matches_gran_slam') }} b2
        ON m.match_id_unificada = b2.match_id_unificada_gan2
)

SELECT
    -- COALESCE nativo en Spark para quedarse con el ID que haya cruzado
    COALESCE(match_id_gran_slam_gan1, match_id_gran_slam_gan2) AS id_partido_otro,
    
    id_partido,
    id_ganador,
    id_perdedor,

    -- Lógica de asignación de IDs según quién era el Ganador en el cruce
    CASE 
        WHEN match_id_unificada_atp = match_id_unificada_gan1 THEN id_ganador
        WHEN match_id_unificada_atp = match_id_unificada_gan2 THEN id_perdedor
        ELSE NULL 
    END AS id_player1,

    CASE 
        WHEN match_id_unificada_atp = match_id_unificada_gan1 THEN id_perdedor
        WHEN match_id_unificada_atp = match_id_unificada_gan2 THEN id_ganador
        ELSE NULL 
    END AS id_player2

FROM partidos
-- Filtramos solo los que realmente cruzaron (tienen datos de Grand Slam)
WHERE COALESCE(match_id_gran_slam_gan1, match_id_gran_slam_gan2) IS NOT NULL