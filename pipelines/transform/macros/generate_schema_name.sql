{#- Use a model's custom schema verbatim: iceberg.silver / iceberg.gold, as GR1 names
    them (Listing 4.2), rather than dbt's default "<target schema>_<custom schema>". -#}
{% macro generate_schema_name(custom_schema_name, node) -%}
    {{ (custom_schema_name or target.schema) | trim }}
{%- endmacro %}
