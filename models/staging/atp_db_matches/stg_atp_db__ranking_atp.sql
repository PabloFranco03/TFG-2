{{ config(
    materialized = 'incremental',
    unique_key = 'id_ranking',
    liquid_clustered_by = ['ranking_fecha', 'id_jugador']
) }}

WITH fuente AS (
    SELECT * FROM {{ source('atp_db', 'ranking_atp') }}
),

casted AS (
    SELECT 
        ranking_date,
        player_id,
        -- Spark SQL: formato 'yyyyMMdd' para parsear strings tipo '20230101'
        to_date(CAST(ranking_date AS STRING), 'yyyyMMdd') AS ranking_fecha,
        CAST(rank AS INT) AS posicion_ranking,
        CAST(points AS INT) AS puntos,
        ingesta_tmz
    FROM fuente
    {% if is_incremental() %}
        WHERE ingesta_tmz > (SELECT MAX(ingesta_tmz) FROM {{ this }})
    {% endif %}
),

filtrado AS (
    SELECT *,
        ROW_NUMBER() OVER (PARTITION BY player_id, ranking_date ORDER BY ingesta_tmz DESC) AS fila
    FROM casted
)

SELECT
    {{ dbt_utils.generate_surrogate_key(['ranking_date','player_id']) }} AS id_ranking,
    ranking_fecha,
    posicion_ranking,
    {{ dbt_utils.generate_surrogate_key(['player_id']) }} AS id_jugador,
    puntos,
    ingesta_tmz
FROM filtrado
WHERE fila = 1