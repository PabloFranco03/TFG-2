{{ config(
    materialized='incremental',
    unique_key='id_punto',
    liquid_clustered_by=['id_juego']
) }}

WITH puntos AS (
    SELECT
        match_id,
        numero_set,
        numero_juego,
        num_punto_partido,
        ganador_punto,
        p1_score,
        p2_score,
        rally_count,
        CAST(ingesta_tmz AS TIMESTAMP) AS ingesta_tmz
    FROM {{ ref('base_atp_db__points_gran_slam') }}
    WHERE num_punto_partido IS NOT NULL
),

mapping AS (
    SELECT
        id_partido_otro,
        id_partido,
        id_player1,
        id_player2
    FROM {{ ref('int__union_ids') }}
),

limpio AS (
    SELECT
        {{ dbt_utils.generate_surrogate_key(['m.id_partido', 'p.numero_set', 'p.numero_juego']) }} AS id_juego,
        {{ dbt_utils.generate_surrogate_key(['m.id_partido', 'p.num_punto_partido']) }} AS id_punto,
        p.num_punto_partido AS num_punto_partido,
        p.rally_count,
        -- Asumimos que p1_score y p2_score ya son strings (ej. '15', 'AD').
        CONCAT(p.p1_score, '-', p.p2_score) AS resultado_momento_saque,

        CASE
            WHEN p.ganador_punto = 1 THEN m.id_player1
            WHEN p.ganador_punto = 2 THEN m.id_player2
            ELSE NULL
        END AS punto_winner,

        CASE
            WHEN p.ganador_punto = 1 THEN m.id_player2
            WHEN p.ganador_punto = 2 THEN m.id_player1
            ELSE NULL
        END AS punto_loser,

        p.ingesta_tmz
    FROM puntos p
    LEFT JOIN mapping m
      ON p.match_id = m.id_partido_otro
    WHERE m.id_partido IS NOT NULL
)

SELECT *
FROM limpio
{% if is_incremental() %}
  WHERE ingesta_tmz > (SELECT MAX(ingesta_tmz) FROM {{ this }})
{% endif %}