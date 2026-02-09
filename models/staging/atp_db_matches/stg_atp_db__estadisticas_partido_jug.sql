{{ config(
    materialized = 'incremental',
    unique_key = 'id_partido_estadisticas',
    liquid_clustered_by = ['id_partido', 'id_jugador']
) }}

with source AS (
    SELECT *
    FROM {{ ref("base_atp_db__matches") }}
),

ganador AS (
    SELECT
        id_partido_estadisticas_gan AS id_partido_estadisticas,
        id_partido,
        id_ganador AS id_jugador,
        TRUE AS ha_ganado,
        w_ace AS aces,
        w_df AS dobles_faltas,
        w_svpt AS puntos_saque,
        w_1stIn AS primeros_saques,
        w_1stWon AS puntos_ganados_1er,
        w_2ndWon AS puntos_ganados_2do,
        w_SvGms AS juegos_saque,
        w_bpSaved AS bp_salvados,
        w_bpFaced AS bp_enfrentados,
        ingesta_tmz
    FROM source
    {% if is_incremental() %}
            where ingesta_tmz > (select max(ingesta_tmz) from {{ this }})
    {% endif %}
),

perdedor AS (
    SELECT
        id_partido_estadisticas_per AS id_partido_estadisticas,
        id_partido,
        id_perdedor AS id_jugador,
        FALSE AS ha_ganado,
        l_ace AS aces,
        l_df AS dobles_faltas,
        l_svpt AS puntos_saque,
        l_1stIn AS primeros_saques,
        l_1stWon AS puntos_ganados_1er,
        l_2ndWon AS puntos_ganados_2do,
        l_SvGms AS juegos_saque,
        l_bpSaved AS bp_salvados,
        l_bpFaced AS bp_enfrentados,
        ingesta_tmz
    FROM source
    {% if is_incremental() %}
            where ingesta_tmz > (select max(ingesta_tmz) from {{ this }})
    {% endif %}
),

estadisticas_union AS (
    SELECT * FROM ganador
    UNION ALL
    SELECT * FROM perdedor
)

SELECT * FROM estadisticas_union