{% macro cents_to_dolars(column_name, decimal_places=2) -%}
round({{ column_name }} /100, {{ decimal_places }})
{%- endmacro %}
