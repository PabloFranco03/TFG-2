{{ config(
    materialized = 'incremental',
    unique_key = 'id_punto_estadisticas',
    liquid_clustered_by = ['id_punto', 'id_jugador']
) }}

WITH source AS (
    SELECT *
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

puntos_enriquecidos AS (
    SELECT
        {{ dbt_utils.generate_surrogate_key(['m.id_partido', 'p.num_punto_partido']) }} AS id_punto,
        m.id_player1,
        m.id_player2,
        p.num_punto_partido,
        p.ganador_punto,
        p.sacador,
        p.velocidad_saque_kmh,
        p.indicador_saque,
        p.numero_saque,
        p.tipo_winner,
        p.tipo_golpeo_winner,
        p.rally_count,
        p.lateral_saque,
        p.profundidad_saque,
        p.profundidad_resto,
        p.ingesta_tmz,
        p.p1_ace, p.p2_ace,
        p.p1_winner, p.p2_winner,
        p.p1_double_fault, p.p2_double_fault,
        p.p1_unf_err, p.p2_unf_err,
        p.p1_net_point, p.p2_net_point,
        p.p1_net_point_won, p.p2_net_point_won,
        p.p1_break_point, p.p2_break_point,
        p.p1_break_point_won, p.p2_break_point_won,
        p.p1_break_point_missed, p.p2_break_point_missed,
        p.distancia_recorrida_p1, p.distancia_recorrida_p2
    FROM source p
    LEFT JOIN mapping m
      ON p.match_id = m.id_partido_otro
    WHERE m.id_partido IS NOT NULL
),

jugador1 AS (
    SELECT
        {{ dbt_utils.generate_surrogate_key(['id_punto', 'id_player1']) }} AS id_punto_estadisticas,
        id_punto,
        id_player1 AS id_jugador,
        TRUE AS es_jugador1,
        CASE WHEN ganador_punto = 1 THEN TRUE ELSE FALSE END AS ha_ganado,

        CASE WHEN sacador = 1 THEN velocidad_saque_kmh ELSE NULL END AS velocidad_saque,
        CASE WHEN sacador = 1 THEN indicador_saque ELSE NULL END AS indicador_saque,
        CASE WHEN sacador = 1 THEN numero_saque ELSE NULL END AS numero_saque,

        tipo_winner,
        tipo_golpeo_winner,
        rally_count,
        lateral_saque,
        profundidad_saque,
        profundidad_resto,

        p1_ace AS ace,
        p1_winner AS winner,
        p1_double_fault AS doble_falta,
        p1_unf_err AS error_no_forzado,
        p1_net_point AS sube_red,
        p1_net_point_won AS gana_en_red,
        p1_break_point AS bp_oportunidad,
        p1_break_point_won AS bp_convertido,
        p1_break_point_missed AS bp_fallado,
        distancia_recorrida_p1 AS distancia_recorrida,
        ingesta_tmz
    FROM puntos_enriquecidos
),

jugador2 AS (
    SELECT
        {{ dbt_utils.generate_surrogate_key(['id_punto', 'id_player2']) }} AS id_punto_estadisticas,
        id_punto,
        id_player2 AS id_jugador,
        FALSE AS es_jugador1,
        CASE WHEN ganador_punto = 2 THEN TRUE ELSE FALSE END AS ha_ganado,

        CASE WHEN sacador = 2 THEN velocidad_saque_kmh ELSE NULL END AS velocidad_saque,
        CASE WHEN sacador = 2 THEN indicador_saque ELSE NULL END AS indicador_saque,
        CASE WHEN sacador = 2 THEN numero_saque ELSE NULL END AS numero_saque,

        tipo_winner,
        tipo_golpeo_winner,
        rally_count,
        lateral_saque,
        profundidad_saque,
        profundidad_resto,

        p2_ace AS ace,
        p2_winner AS winner,
        p2_double_fault AS doble_falta,
        p2_unf_err AS error_no_forzado,
        p2_net_point AS sube_red,
        p2_net_point_won AS gana_en_red,
        p2_break_point AS bp_oportunidad,
        p2_break_point_won AS bp_convertido,
        p2_break_point_missed AS bp_fallado,
        distancia_recorrida_p2 AS distancia_recorrida,
        ingesta_tmz
    FROM puntos_enriquecidos
),

estadisticas_union AS (
    SELECT * FROM jugador1
    UNION ALL
    SELECT * FROM jugador2
)

SELECT *
FROM estadisticas_union
{% if is_incremental() %}
  WHERE ingesta_tmz > (SELECT MAX(ingesta_tmz) FROM {{ this }})
{% endif %}