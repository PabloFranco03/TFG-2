{{ config(
    materialized='incremental',
    unique_key='id_partido',
    liquid_clustered_by=['id_torneo_anio', 'id_ganador']
) }}

WITH partidos AS (
    SELECT 
        id_partido,
        id_torneo_anio,
        id_ronda_torneo,
        id_ganador,
        id_perdedor,
        duracion_minutos,
        resultado,
        sets_maximos,
        numero_partido_torneo,
        ingesta_tmz

    FROM {{ ref('base_atp_db__matches') }}
    {% if is_incremental() %}
      WHERE ingesta_tmz > (SELECT MAX(ingesta_tmz) FROM {{ this }})
    {% endif %}
)

SELECT * FROM partidos