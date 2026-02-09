{{ config(materialized='table') }}

WITH nivel_torneo AS (
    SELECT * FROM {{ ref('stg_atp_db__nivel_torneo') }}
)

SELECT
    id_nivel_torneo,
    nivel_torneo,
    nivel_torneo_desc
FROM nivel_torneo