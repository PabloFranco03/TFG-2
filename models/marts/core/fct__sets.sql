{{ config(
    materialized = 'incremental',
    unique_key = 'id_set',
    liquid_clustered_by = ['id_partido']
) }}

WITH sets_base AS (
    SELECT *
    FROM {{ ref('stg_atp_db__sets') }}
    WHERE id_partido IS NOT NULL
),

sets_enriquecidos AS (
    SELECT
        id_set,
        id_partido,
        numero_set,
        ganador_set_id,
        perdedor_set_id,
        ganador_set_juegos,
        perdedor_set_juegos,
        marcador_set,
        ingesta_tmz,

        (ganador_set_juegos + perdedor_set_juegos) AS total_juegos_set,
        CASE 
            WHEN (ganador_set_juegos + perdedor_set_juegos) = 0 THEN NULL
            -- Spark SQL prefiere DOUBLE.
            ELSE ROUND(CAST(ganador_set_juegos AS DOUBLE) / NULLIF(ganador_set_juegos + perdedor_set_juegos, 0), 3)
        END AS pct_juegos_ganados_ganador,


        CASE 
            WHEN marcador_set IN ('7-6', '6-7') THEN TRUE
            ELSE FALSE
        END AS fue_tiebreak,

        ROW_NUMBER() OVER (PARTITION BY id_partido ORDER BY numero_set DESC) = 1 AS set_decisivo

    FROM sets_base
)

SELECT *
FROM sets_enriquecidos
{% if is_incremental() %}
  WHERE ingesta_tmz > (SELECT MAX(ingesta_tmz) FROM {{ this }})
{% endif %}