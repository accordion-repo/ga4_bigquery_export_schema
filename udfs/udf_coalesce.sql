{% macro udf_coalesce(param1, param2) %}
    coalesce({{ param1 }},{{ param2 }})
{% endmacro %}
