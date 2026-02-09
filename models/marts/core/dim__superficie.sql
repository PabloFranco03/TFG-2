{{ config(materialized='table') }}

WITH superficie AS (
    SELECT * FROM {{ ref('stg_atp_db__superficie') }}
)

SELECT
    id_superficie,
    superficie,
    nombre_superficie,
    es_rapida
FROM superficie