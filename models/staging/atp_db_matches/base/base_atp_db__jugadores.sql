{{
    config(
        materialized = 'view'
    )
}}

WITH source AS (
    SELECT *
    FROM {{ source('extra_jugadores', 'players') }}
),

renamed AS (
    SELECT
        {{ dbt_utils.generate_surrogate_key(['player_id']) }} AS id_jugador,
        
        TRIM(name_first) AS nombre,
        
        TRIM(name_last) AS apellido,
        
        -- CONCAT_WS funciona igual en ambos, ignora nulos automáticamente
        CONCAT_WS(' ', name_first, name_last) AS nombre_completo,
        
        hand AS mano_dominante,
        
        -- CAMBIO CLAVE: Sintaxis Spark para fechas
        -- 1. Convertimos a string por si viene como numero
        -- 2. Usamos el patrón 'yyyyMMdd'
        to_date(CAST(dob AS STRING), 'yyyyMMdd') AS fecha_nacimiento,
        
        ioc AS cod_pais,
        
        -- CAMBIO CLAVE: Casteo explícito a entero
        CAST(height AS INT) AS altura_cm,
        
        wikidata_id,
        
        ingesta_tmz
        
    FROM source
)

SELECT *
FROM renamed