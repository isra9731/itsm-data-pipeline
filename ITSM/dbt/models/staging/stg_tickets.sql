-- models/staging/stg_tickets.sql
-- Stage raw tickets: basic cleaning, dedupe, standardize date formats.
with raw as (
    select *
    from {{ source('raw','tickets_raw') }} -- source should be configured in a separate sources.yml
), cleaned as (
    -- remove exact duplicates by all columns
    select distinct *
    from raw
), normalized as (
    select
        "Ticket ID" as ticket_id,
        coalesce(nullif(trim("Category"),''),'Unknown') as category,
        coalesce(nullif(trim("Sub-Category"),''),'Unknown') as sub_category,
        coalesce(nullif(trim("Priority"),''),'Unspecified') as priority,
        coalesce(nullif(trim("Status"),''),'Unknown') as status,
        coalesce(nullif(trim("Assigned Group"),''),'Unassigned') as assigned_group,
        coalesce(nullif(trim("Technician"),''),'Unknown') as technician,
        -- Standardize dates: try multiple common columns
        coalesce(parse_timestamp("Created Date"), parse_timestamp("CreatedDate")) as created_at,
        coalesce(parse_timestamp("Resolved Date"), parse_timestamp("ResolvedDate")) as resolved_at,
        -- If resolution hours provided, use it; otherwise compute
        case when try_parse_numeric("Resolution Time (Hrs)") is not null then cast(try_parse_numeric("Resolution Time (Hrs)") as double precision)
             else null end as resolution_hours,
        "Customer Impact" as customer_impact
    from cleaned
)
select * from normalized