-- =============================================================================
-- macros/generate_schema_name.sql
-- Override default dbt schema naming behaviour.
--
-- Default dbt behaviour: prepend target schema prefix to every model schema.
-- e.g. target schema = "staging", model schema = "marts"
--      → dbt creates table in "staging_marts" (not "marts")
--
-- This macro makes dbt use the EXACT schema name specified in dbt_project.yml
-- without any prefix, so:
--   staging  models → schema "staging"
--   marts    models → schema "marts"
--   dw       models → schema "dw"
-- =============================================================================

{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- if custom_schema_name is none -%}
        {{ target.schema }}
    {%- else -%}
        {{ custom_schema_name | trim }}
    {%- endif -%}
{%- endmacro %}
