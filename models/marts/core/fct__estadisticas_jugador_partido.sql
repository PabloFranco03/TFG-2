{{ config(
    materialized='incremental',
    unique_key='id_partido_estadisticas',
    liquid_clustered_by=['id_partido', 'id_jugador']
) }}

WITH partidos AS (
    SELECT 
        id_partido,
        id_torneo_anio,
        id_ronda_torneo,
        id_ganador,
        id_perdedor,
        duracion_minutos,
        resultado,
        sets_maximos,
        numero_partido_torneo,
        ingesta_tmz
    FROM {{ ref('stg_atp_db__partidos') }}
),

estadisticas AS (
    SELECT *
    FROM {{ ref('stg_atp_db__estadisticas_partido_jug') }}
),

partido_jugador AS (
    SELECT 
        e.id_partido_estadisticas,
        e.id_partido,
        e.id_jugador,
        e.ha_ganado,
        e.aces,
        e.dobles_faltas,
        e.puntos_saque,
        e.primeros_saques,
        e.puntos_ganados_1er,
        e.puntos_ganados_2do,
        e.juegos_saque,
        e.bp_salvados,
        e.bp_enfrentados,
        p.id_torneo_anio,
        p.id_ronda_torneo,
        p.id_ganador AS id_ganador_partido,
        p.id_perdedor AS id_perdedor_partido,
        p.duracion_minutos,
        p.resultado,
        p.sets_maximos,
        p.numero_partido_torneo,
        p.ingesta_tmz
    FROM estadisticas e
    LEFT JOIN partidos p
        ON e.id_partido = p.id_partido
)

SELECT * FROM partido_jugador

{% if is_incremental() %}
      WHERE ingesta_tmz > (SELECT MAX(ingesta_tmz) FROM {{ this }})
{% endif %}