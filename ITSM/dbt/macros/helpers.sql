{% macro parse_timestamp(col) -%}
    -- Try parsing several formats; dbt will pass the column name, so we return SQL expression
    try_cast({{ col }} as timestamp)
{%- endmacro %}

{% macro try_parse_numeric(col) -%}
    case when regexp_replace(coalesce({{ col }},''), '[^0-9.\-]', '') ~ '^[0-9\.\-]+$' then regexp_replace(coalesce({{ col }},''), '[^0-9\.\-]', '') else null end
{%- endmacro %}