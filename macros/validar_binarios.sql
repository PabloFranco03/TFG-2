{% macro validar_binaria_por_partido(modelo, campo, id_campo) %}
SELECT {{ id_campo }}
FROM {{ ref(modelo) }}
GROUP BY {{ id_campo }}
HAVING SUM(CASE WHEN {{ campo }} THEN 1 ELSE 0 END) != 1
    OR COUNT(*) != 2
{% endmacro %}