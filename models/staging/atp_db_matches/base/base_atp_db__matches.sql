{{
    config(
        materialized = 'view'
    )
}}

with source as (
    select *
    from {{ source('atp', 'matches') }}
    where {{ filtrado_copa_davis_y_finals('tourney_level') }}
),

partidos AS (
    SELECT
        -- Generación de claves (dbt_utils funciona bien en Spark)
        {{ dbt_utils.generate_surrogate_key(['tourney_id']) }} AS id_torneo_anio,
        {{ dbt_utils.generate_surrogate_key(['tourney_id', 'match_num', 'winner_id']) }} AS id_partido_estadisticas_gan,
        {{ dbt_utils.generate_surrogate_key(['tourney_id', 'match_num', 'loser_id']) }} AS id_partido_estadisticas_per,
        {{ dbt_utils.generate_surrogate_key(['tourney_id', 'match_num']) }} AS id_partido,
        
        -- Limpieza y textos
        {{ limpiar_texto('tourney_name') }} AS torneo_limpio,
        tourney_name AS nombre_torneo,
        {{ limpiar_texto('surface') }} AS superficie_limpia,
        surface AS superficie,
        
        CAST(draw_size AS INT) as total_jugadores,
        
        {{ dbt_utils.generate_surrogate_key(['tourney_level']) }} AS id_nivel_torneo,
        tourney_level AS nivel_torneo,
        
        to_date(CAST(tourney_date AS STRING), 'yyyyMMdd') AS fecha_inicio,
        
        CAST(match_num AS INT) as numero_partido_torneo,
        
        {{ dbt_utils.generate_surrogate_key(['winner_id']) }} AS id_ganador,
        WINNER_NAME,
        WINNER_HAND,
        CAST(WINNER_HT AS INT) AS WINNER_HT,
        WINNER_IOC,
        CAST(WINNER_AGE AS INT) AS WINNER_AGE,
        
        {{ dbt_utils.generate_surrogate_key(['loser_id']) }} AS id_perdedor,
        LOSER_NAME,
        LOSER_HAND,
        CAST(LOSER_HT AS INT) AS LOSER_HT,
        LOSER_IOC,
        CAST(LOSER_AGE AS INT) AS LOSER_AGE,
        
        score AS resultado,
        CAST(best_of AS INT) AS sets_maximos,
        
        {{ dbt_utils.generate_surrogate_key(['round']) }} AS id_ronda_torneo,
        round AS ronda_torneo,
        CAST(minutes AS INT) AS duracion_minutos,
        
        CAST(W_ACE AS INT) AS W_ACE,
        CAST(W_DF AS INT) AS W_DF,
        CAST(W_SVPT AS INT) AS W_SVPT,
        CAST(W_1STIN AS INT) AS W_1STIN,
        CAST(W_1STWON AS INT) AS W_1STWON,
        CAST(W_2NDWON AS INT) AS W_2NDWON,
        CAST(W_SVGMS AS INT) AS W_SVGMS,
        CAST(W_BPSAVED AS INT) AS W_BPSAVED,
        CAST(W_BPFACED AS INT) AS W_BPFACED,
        
        CAST(L_ACE AS INT) AS L_ACE,
        CAST(L_DF AS INT) AS L_DF,
        CAST(L_SVPT AS INT) AS L_SVPT,
        CAST(L_1STIN AS INT) AS L_1STIN,
        CAST(L_1STWON AS INT) AS L_1STWON,
        CAST(L_2NDWON AS INT) AS L_2NDWON,
        CAST(L_SVGMS AS INT) AS L_SVGMS,
        CAST(L_BPSAVED AS INT) AS L_BPSAVED,
        CAST(L_BPFACED AS INT) AS L_BPFACED,
        
        CAST(WINNER_RANK AS INT) AS WINNER_RANK,
        CAST(WINNER_RANK_POINTS AS INT) AS WINNER_RANK_POINTS,
        CAST(LOSER_RANK AS INT) AS LOSER_RANK,
        CAST(LOSER_RANK_POINTS AS INT) AS LOSER_RANK_POINTS,
        
        INGESTA_TMZ,
        
        year(to_date(CAST(tourney_date AS STRING), 'yyyyMMdd')) AS year,
        
        {{ limpiar_texto('winner_name') }} AS player1_limpio,
        {{ limpiar_texto('loser_name') }} AS player2_limpio

    FROM source
)

SELECT 
    *,
    {{ dbt_utils.generate_surrogate_key(['torneo_limpio']) }} AS id_torneo,
    {{ dbt_utils.generate_surrogate_key(['superficie_limpia']) }} AS id_superficie,
    {{ dbt_utils.generate_surrogate_key(['year','torneo_limpio','player1_limpio','player2_limpio']) }} AS match_id_unificada
FROM partidos