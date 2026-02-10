{% macro comparar_flag_con_logica(
    tabla,
    campo_flag,
    expresion_logica
) %}
SELECT *
FROM {{ ref(tabla) }}
WHERE {{ campo_flag }} IS DISTINCT FROM ({{ expresion_logica }})
{% endmacro %}