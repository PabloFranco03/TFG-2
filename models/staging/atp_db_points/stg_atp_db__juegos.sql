{{ config(
    materialized='incremental',
    unique_key='id_juego',
    liquid_clustered_by=['id_set']
) }}

WITH puntos AS (
    SELECT DISTINCT
        match_id,
        numero_set,
        numero_juego,
        ganador_juego,
        sacador,
        CAST(ingesta_tmz AS TIMESTAMP) AS ingesta_tmz
    FROM {{ ref('base_atp_db__points_gran_slam') }}
    WHERE ganador_juego != 0
),

mapping AS (
    SELECT
        id_partido_otro,
        id_partido,
        id_player1,
        id_player2
    FROM {{ ref('int__union_ids') }}
),

agg_puntos AS (
    SELECT
        match_id,
        numero_set,
        numero_juego,
        MAX(ganador_juego) AS ganador_juego,
        MIN(sacador) AS sacador,
        MAX(ingesta_tmz) AS ingesta_tmz
    FROM puntos
    GROUP BY match_id, numero_set, numero_juego
),

cambio_id AS (
    SELECT 
        MAX(m.id_partido) AS id_partido,
        p.match_id,
        p.numero_set,
        p.numero_juego,
        MAX(m.id_player1) AS id_player1,
        MAX(m.id_player2) AS id_player2,
        MAX(p.ganador_juego) AS ganador_juego,
        MIN(p.sacador) AS sacador,

        CASE
            WHEN MAX(p.ganador_juego) = 1 THEN MAX(m.id_player1)
            WHEN MAX(p.ganador_juego) = 2 THEN MAX(m.id_player2)
            ELSE NULL
        END AS ganador_juego_id,

        CASE
            WHEN MAX(p.ganador_juego) = 1 THEN MAX(m.id_player2)
            WHEN MAX(p.ganador_juego) = 2 THEN MAX(m.id_player1)
            ELSE NULL
        END AS perdedor_juego_id,

        CASE
            WHEN MIN(p.sacador) = 1 THEN MAX(m.id_player1)
            WHEN MIN(p.sacador) = 2 THEN MAX(m.id_player2)
            ELSE NULL
        END AS sacador_id,

        MAX(p.ingesta_tmz) AS ingesta_tmz

    FROM agg_puntos p
    LEFT JOIN mapping m
        ON p.match_id = m.id_partido_otro
    WHERE m.id_partido IS NOT NULL
    GROUP BY p.match_id, p.numero_set, p.numero_juego
)

SELECT 
    {{ dbt_utils.generate_surrogate_key(['id_partido', 'numero_set', 'numero_juego']) }} AS id_juego,
    {{ dbt_utils.generate_surrogate_key(['id_partido', 'numero_set']) }} AS id_set,
    numero_juego,
    ganador_juego_id,
    perdedor_juego_id,
    sacador_id,
    ingesta_tmz
FROM cambio_id
{% if is_incremental() %}
  WHERE ingesta_tmz > (SELECT MAX(ingesta_tmz) FROM {{ this }})
{% endif %}