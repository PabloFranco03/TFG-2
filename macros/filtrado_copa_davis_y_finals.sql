{% macro filtrado_copa_davis_y_finals(nivel_col) %}
  {{ nivel_col }} NOT IN('D','F')
{% endmacro %}