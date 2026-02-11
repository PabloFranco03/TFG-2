{{ config(
    materialized='incremental',
    unique_key='id_set',
    liquid_clustered_by=['id_partido']
) }}

WITH puntos AS (
    SELECT
        match_id,
        numero_set,
        p1_juegos_ganados,
        p2_juegos_ganados,
        ganador_set,
        CAST(ingesta_tmz AS TIMESTAMP) AS ingesta_tmz
    FROM {{ ref('base_atp_db__points_gran_slam') }}
    WHERE ganador_set != 0
),

mapping AS (
    SELECT
        id_partido_otro,
        id_partido,
        id_player1,
        id_player2
    FROM {{ ref('int__union_ids') }}
),

cambio_id AS (
    SELECT 
        MAX(m.id_partido) AS id_partido,
        p.match_id,
        p.numero_set,
        MAX(m.id_player1) AS id_player1,
        MAX(m.id_player2) AS id_player2,
        MAX(p.ganador_set) AS ganador_set,

        CASE
            WHEN MAX(p.ganador_set) = 1 THEN MAX(p.p1_juegos_ganados)
            WHEN MAX(p.ganador_set) = 2 THEN MAX(p.p2_juegos_ganados)
            ELSE NULL
        END AS ganador_set_juegos,

        CASE
            WHEN MAX(p.ganador_set) = 1 THEN MAX(p.p2_juegos_ganados)
            WHEN MAX(p.ganador_set) = 2 THEN MAX(p.p1_juegos_ganados)
            ELSE NULL
        END AS perdedor_set_juegos,

        CASE
            WHEN MAX(p.ganador_set) = 1 THEN MAX(m.id_player1)
            WHEN MAX(p.ganador_set) = 2 THEN MAX(m.id_player2)
            ELSE NULL
        END AS ganador_set_id,

        CASE
            WHEN MAX(p.ganador_set) = 1 THEN MAX(m.id_player2)
            WHEN MAX(p.ganador_set) = 2 THEN MAX(m.id_player1)
            ELSE NULL
        END AS perdedor_set_id,

        -- Spark SQL: CONCAT requiere strings. Añadimos CAST por seguridad.
        CONCAT(CAST(MAX(p.p1_juegos_ganados) AS STRING), '-', CAST(MAX(p.p2_juegos_ganados) AS STRING)) AS marcador_set,
        MAX(p.ingesta_tmz) AS ingesta_tmz

    FROM puntos p
    LEFT JOIN mapping m
        ON p.match_id = m.id_partido_otro
    WHERE m.id_partido IS NOT NULL
    GROUP BY p.match_id, p.numero_set
)

SELECT
    id_partido,
    {{ dbt_utils.generate_surrogate_key(['id_partido', 'numero_set']) }} AS id_set,
    numero_set,
    ganador_set_id,
    perdedor_set_id,
    ganador_set_juegos,
    perdedor_set_juegos,
    marcador_set,
    ingesta_tmz
    
FROM cambio_id
{% if is_incremental() %}
  WHERE ingesta_tmz > (SELECT MAX(ingesta_tmz) FROM {{ this }})
{% endif %}