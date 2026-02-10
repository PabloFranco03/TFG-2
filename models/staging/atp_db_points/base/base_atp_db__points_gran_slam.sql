{{ config(
    materialized = 'view'
) }}

WITH raw AS (
    SELECT *
    FROM {{ source('extra_grand_slam', 'puntos_grand_slam') }}
    -- SPARK SQL: split devuelve un array. El índice empieza en 0.
    -- La 3ª parte en Snowflake (índice 3) equivale al índice [2] en Spark.
    WHERE LEFT(split(match_id, '-')[2], 1) != '2' 
),

renamed AS (
    SELECT
        match_id,
        elapsed_time AS tiempo_transcurrido,
        
        -- Casteos a Enteros
        CAST(set_no AS INT) AS numero_set,
        CAST(p1_games_won AS INT) AS p1_juegos_ganados,
        CAST(p2_games_won AS INT) AS p2_juegos_ganados,
        CAST(set_winner AS INT) AS ganador_set,
        CAST(game_no AS INT) AS numero_juego,
        CAST(game_winner AS INT) AS ganador_juego,
        
        point_number AS num_punto_partido, -- A veces es 0X, lo dejamos string
        CAST(point_winner AS INT) AS ganador_punto,
        CAST(point_server AS INT) AS sacador,
        
        -- Casteos a Decimales/Double
        CAST(speed_kmh AS DOUBLE) AS velocidad_saque_kmh,
        
        rally,
        p1_score,
        p2_score,
        
        -- Contadores y Estadísticas (INT)
        CAST(p1_points_won AS INT) AS puntos_ganados_p1,
        CAST(p2_points_won AS INT) AS puntos_ganados_p2,
        
        -- Indicadores binarios (0/1) -> INT
        CAST(p1_ace AS INT) AS p1_ace,
        CAST(p2_ace AS INT) AS p2_ace,
        CAST(p1_winner AS INT) AS p1_winner,
        CAST(p2_winner AS INT) AS p2_winner,
        CAST(p1_double_fault AS INT) AS p1_double_fault,
        CAST(p2_double_fault AS INT) AS p2_double_fault,
        CAST(p1_unf_err AS INT) AS p1_unf_err,
        CAST(p2_unf_err AS INT) AS p2_unf_err,
        CAST(p1_net_point AS INT) AS p1_net_point,
        CAST(p2_net_point AS INT) AS p2_net_point,
        CAST(p1_net_point_won AS INT) AS p1_net_point_won,
        CAST(p2_net_point_won AS INT) AS p2_net_point_won,
        CAST(p1_break_point AS INT) AS p1_break_point,
        CAST(p2_break_point AS INT) AS p2_break_point,
        CAST(p1_break_point_won AS INT) AS p1_break_point_won,
        CAST(p2_break_point_won AS INT) AS p2_break_point_won,
        CAST(p1_break_point_missed AS INT) AS p1_break_point_missed,
        CAST(p2_break_point_missed AS INT) AS p2_break_point_missed,
        
        serve_indicator AS indicador_saque,
        CAST(serve_number AS INT) AS numero_saque,
        
        winner_type AS tipo_winner,
        winner_shot_type AS tipo_golpeo_winner,
        
        -- Distancias (Double)
        CAST(p1_distance_run AS DOUBLE) AS distancia_recorrida_p1,
        CAST(p2_distance_run AS DOUBLE) AS distancia_recorrida_p2,
        
        CAST(rally_count AS INT) AS rally_count,
        
        serve_width AS lateral_saque,
        serve_depth AS profundidad_saque,
        return_depth AS profundidad_resto,
        
        ingesta_tmz
    FROM raw
)

SELECT *
FROM renamed