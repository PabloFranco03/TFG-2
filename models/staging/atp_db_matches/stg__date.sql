{{ config(
    materialized='table',
    liquid_clustered_by=['date']
) }}

WITH date_spine AS (
    {{ dbt_utils.date_spine(
        start_date="'2000-01-01'",
        end_date="'2029-12-31'",
        datepart="day"
    ) }}
)

SELECT 
    CAST(date_day AS DATE) AS date,
    year(date_day) AS anio,
    month(date_day) AS mes,
    day(date_day) AS dia,
    dayofweek(date_day) AS dia_semana, -- 1=Domingo en Spark
    date_format(date_day, 'MMMM') AS mes_nombre, -- Nombre completo
    date_format(date_day, 'E') AS dia_nombre     -- Abreviado (Mon, Tue...)
FROM date_spine
ORDER BY date_day