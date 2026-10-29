{# Default dbt concatenates custom schema onto target.schema (e.g. RAW_STAGING).
   Return the custom name alone so +schema: STAGING lands in MARKET.STAGING. #}
{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- if custom_schema_name is none -%}
        {{ target.schema }}
    {%- else -%}
        {{ custom_schema_name | trim }}
    {%- endif -%}
{%- endmacro %}
