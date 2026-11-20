{#
    Source systems send dates either as ISO (2024-03-17) or as US style
    (03/17/2024). Returns a date, or null when neither format matches.
#}
{% macro parse_source_date(column) -%}
    coalesce(
        try_cast({{ column }} as date),
        cast(try_strptime({{ column }}, '%m/%d/%Y') as date)
    )
{%- endmacro %}


{# Turns free-text labels such as " Mass Market" into snake_case codes. #}
{% macro standardize_label(column) -%}
    nullif(replace(lower(trim({{ column }})), ' ', '_'), '')
{%- endmacro %}


{# Key used to look a raw channel label up in the `channel_mapping` seed. #}
{% macro channel_lookup_key(column) -%}
    lower(trim({{ column }}))
{%- endmacro %}
