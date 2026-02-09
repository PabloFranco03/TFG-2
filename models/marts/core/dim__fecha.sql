{{ config(
    materialized = 'table',
    liquid_clustered_by = ['fecha']
) }}

WITH stg_fecha AS (
    SELECT * FROM {{ ref('stg__date') }}
),

dim_fecha AS (
    SELECT 
        date AS fecha,
        anio,
        mes,
        dia,
        dia_semana,
        mes_nombre,
        dia_nombre
    FROM stg_fecha
)

SELECT * FROM dim_fecha