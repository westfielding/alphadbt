-- macros/generate_alias.sql
-- Ensures every model is prefixed with its layer name.
-- The model file name is expected to already include the prefix
-- (bronze_dividends, silver_dividends, etc.) so this macro is a
-- safety net that guarantees the prefix is preserved even if dbt
-- strips it during alias resolution.

{% macro generate_alias(custom_alias_name, node) %}
    {%- if custom_alias_name -%}
        {{ custom_alias_name | trim }}
    {%- elif node.fqn | length > 2 -%}
        {#- Derive layer from the second-to-last path component (bronze/silver/gold) -#}
        {%- set layer = node.fqn[-2] -%}
        {%- set allowed_layers = ['bronze', 'silver', 'gold'] -%}
        {%- if layer in allowed_layers and not node.name.startswith(layer ~ '_') -%}
            {{ layer }}_{{ node.name | trim }}
        {%- else -%}
            {{ node.name | trim }}
        {%- endif -%}
    {%- else -%}
        {{ node.name | trim }}
    {%- endif -%}
{% endmacro %}
