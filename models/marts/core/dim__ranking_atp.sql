{{ config(
    materialized = 'incremental',
    unique_key = 'id_ranking',
    liquid_clustered_by = ['ranking_fecha', 'id_jugador']
) }}

WITH ranking_atp AS (
    SELECT * FROM {{ ref('stg_atp_db__ranking_atp') }}
),

renamed_casted AS (
    SELECT
        id_ranking,
        ranking_fecha,
        posicion_ranking,
        id_jugador,
        puntos,
        ingesta_tmz
    FROM ranking_atp
    {% if is_incremental() %}
        WHERE ingesta_tmz > (SELECT MAX(ingesta_tmz) FROM {{ this }})
    {% endif %}
)

SELECT * FROM renamed_casted