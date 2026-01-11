{{
    config(
        materialized = 'view'
    )
}}

WITH raw AS (
    SELECT *
    FROM {{ source('extra_grand_slam', 'matches_grand_slam') }}
    -- Filtro para obtener Men's Singles (generalmente empiezan por 1 en estos datasets)
    -- En Spark SQL, LEFT y CAST funcionan igual que en SQL estándar.
    WHERE LEFT(CAST(match_num AS STRING), 1) = '1' 
),

casted AS (
    SELECT
        match_id,
        CAST(year AS INT) AS year,
        {{ limpiar_texto('slam') }} AS slam_limpio,
        CAST(match_num AS INT) AS match_num,
        {{ limpiar_texto('player1') }} AS player1_limpio,
        {{ limpiar_texto('player2') }} AS player2_limpio,
        player1,
        player2,
        ingesta_tmz
    FROM raw
)

SELECT 
    -- Generación de claves para cruzar tanto si el jugador es el 1 como si es el 2
    {{ dbt_utils.generate_surrogate_key(['year','slam_limpio','player1_limpio','player2_limpio']) }} AS match_id_unificada_gan1,
    {{ dbt_utils.generate_surrogate_key(['year','slam_limpio','player2_limpio','player1_limpio']) }} AS match_id_unificada_gan2,
    match_id,
    player1,
    player2,
    ingesta_tmz
FROM casted