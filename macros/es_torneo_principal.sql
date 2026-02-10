{% macro es_torneo_principal(nivel_col) %}
  {{ nivel_col }} IN ('G', 'M')
{% endmacro %}