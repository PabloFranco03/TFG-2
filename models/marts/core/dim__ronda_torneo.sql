{{ config(materialized='table') }}

WITH ronda_torneo AS (
    SELECT * FROM {{ ref('stg_atp_db__ronda_torneo') }}
)

SELECT
    id_ronda_torneo,
    ronda_torneo,
    ronda_torneo_desc
FROM ronda_torneo