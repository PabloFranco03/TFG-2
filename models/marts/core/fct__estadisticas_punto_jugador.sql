{{ config(
    materialized = 'incremental',
    unique_key = 'id_punto_estadisticas',
    liquid_clustered_by = ['id_partido', 'id_jugador']
) }}

WITH puntos_base AS (
    SELECT *
    FROM {{ ref('stg_atp_db__puntos') }}
    WHERE punto_winner IS NOT NULL
),

stats_jugador AS (
    SELECT *
    FROM {{ ref('stg_atp_db__estadisticas_punto_jug') }}
    WHERE id_jugador IS NOT NULL
),

partidos AS (
    SELECT 
        id_partido,
        id_torneo_anio,
        id_ronda_torneo,
        id_ganador,
        id_perdedor,
        sets_maximos,
        numero_partido_torneo
    FROM {{ ref('stg_atp_db__partidos') }}
),

puntos_con_partido AS (
    SELECT 
        s.id_punto_estadisticas,
        s.id_punto,
        p.id_juego,
        p.num_punto_partido,
        p.rally_count,
        p.punto_winner,
        p.punto_loser,
        p.resultado_momento_saque,

        s.id_jugador,
        s.velocidad_saque,
        s.numero_saque,
        s.indicador_saque,
        s.ha_ganado,
        s.ace,
        s.doble_falta,
        s.winner,
        s.error_no_forzado,
        s.sube_red,
        s.gana_en_red,
        s.bp_oportunidad,
        s.bp_convertido,
        s.bp_fallado,
        s.distancia_recorrida,
        s.rally_count AS rally_count_confirmado,
        s.tipo_winner,
        s.tipo_golpeo_winner,
        s.lateral_saque,
        s.profundidad_saque,
        s.profundidad_resto,
        s.ingesta_tmz,

        pa.id_partido,
        pa.id_torneo_anio,
        pa.id_ronda_torneo,
        pa.id_ganador AS id_ganador_partido,
        pa.id_perdedor AS id_perdedor_partido,
        pa.sets_maximos,
        pa.numero_partido_torneo

    FROM stats_jugador s
    LEFT JOIN puntos_base p ON s.id_punto = p.id_punto
    LEFT JOIN {{ ref('stg_atp_db__juegos') }} j ON p.id_juego = j.id_juego
    LEFT JOIN {{ ref("stg_atp_db__sets")}} sets ON j.id_set = sets.id_set 
    LEFT JOIN partidos pa ON sets.id_partido = pa.id_partido

    WHERE p.id_juego IS NOT NULL
)

SELECT * FROM puntos_con_partido

{% if is_incremental() %}
  WHERE ingesta_tmz > (SELECT MAX(ingesta_tmz) FROM {{ this }})
{% endif %}