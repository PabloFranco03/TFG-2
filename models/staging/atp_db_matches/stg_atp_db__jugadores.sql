{{ config(
    materialized='table',
    liquid_clustered_by=['id_jugador']
) }}

WITH source AS (
    SELECT
        id_ganador, 
        winner_name, 
        winner_hand,
        winner_ht,
        winner_ioc,
        id_perdedor,
        loser_name,
        loser_hand,
        loser_ht,
        loser_ioc
    FROM {{ ref("base_atp_db__matches") }}
),

ganadores AS (
    SELECT DISTINCT
        id_ganador AS id_jugador,
        winner_name AS nombre_jugador,
        winner_hand AS mano_dominante,
        winner_ht AS altura_cm,
        winner_ioc AS cod_pais
    FROM source
),

perdedores AS (
    SELECT DISTINCT
        id_perdedor AS id_jugador,
        loser_name AS nombre_jugador,
        loser_hand AS mano_dominante,
        loser_ht AS altura_cm,
        loser_ioc AS cod_pais
    FROM source
),

jugadores_union AS (
    SELECT * FROM ganadores
    UNION
    SELECT * FROM perdedores
),

registro_vacio AS (
    SELECT
        '-1' AS id_jugador, 
        'Desconocido' AS nombre_jugador,
        NULL AS mano_dominante,
        NULL AS altura_cm,
        NULL AS cod_pais
),

union_total AS (
    SELECT * FROM jugadores_union
    UNION ALL
    SELECT * FROM registro_vacio
),

ioc_paises AS (
    SELECT *
    FROM {{ ref('paises_ioc') }}
),

jugadores_base AS (
    SELECT id_jugador, fecha_nacimiento, wikidata_id
    FROM {{ ref('base_atp_db__jugadores') }}
),

deduplicado AS (
    SELECT *,
           ROW_NUMBER() OVER (PARTITION BY id_jugador ORDER BY nombre_jugador) AS fila
    FROM (
        SELECT
            j.id_jugador,
            j.nombre_jugador,
            j.mano_dominante,
            j.altura_cm,
            j.cod_pais,
            p.pais_completo AS pais_desc,
            b.fecha_nacimiento,
            b.wikidata_id
        FROM union_total j
        LEFT JOIN ioc_paises p
            ON j.cod_pais = p.cod_ioc
        LEFT JOIN jugadores_base b
            ON j.id_jugador = b.id_jugador
    ) sub
)

SELECT *
FROM deduplicado
WHERE fila = 1