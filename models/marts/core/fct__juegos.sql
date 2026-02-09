{{ config(
    materialized = 'incremental',
    unique_key = 'id_juego',
    liquid_clustered_by = ['id_set']
) }}

WITH juegos_base AS (
    SELECT *
    FROM {{ ref('stg_atp_db__juegos') }}
    WHERE ganador_juego_id IS NOT NULL
),

juegos_enriquecidos AS (
    SELECT
        id_juego,
        id_set,
        numero_juego,
        ganador_juego_id,
        perdedor_juego_id,
        sacador_id,
        ingesta_tmz,

        CASE 
            WHEN ganador_juego_id IS NOT NULL AND sacador_id IS NOT NULL 
                 AND ganador_juego_id != sacador_id THEN TRUE
            ELSE FALSE
        END AS es_break,

        CASE 
            WHEN ganador_juego_id IS NOT NULL AND sacador_id IS NOT NULL 
                 AND ganador_juego_id = sacador_id THEN TRUE
            ELSE FALSE
        END AS es_juego_servicio

    FROM juegos_base
)

SELECT *
FROM juegos_enriquecidos
{% if is_incremental() %}
  WHERE ingesta_tmz > (SELECT MAX(ingesta_tmz) FROM {{ this }})
{% endif %}