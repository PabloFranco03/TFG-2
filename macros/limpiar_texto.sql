{% macro limpiar_texto(campo) %}
    LOWER(TRIM(REGEXP_REPLACE({{ campo }}, '[^a-zA-Z0-9 ]', '')))
{% endmacro %}