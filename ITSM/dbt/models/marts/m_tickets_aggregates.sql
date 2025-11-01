-- models/marts/m_tickets_aggregates.sql
-- Aggregations: average resolution time per category and priority; closure rate per assigned group; monthly summary.
with base as (
    select
        ticket_id,
        category,
        priority,
        assigned_group,
        created_at,
        resolved_at,
        resolution_hours
    from {{ ref('stg_tickets') }}
), with_resolution as (
    select
        *,
        coalesce(resolution_hours, extract(epoch from (resolved_at - created_at))/3600.0) as resolution_hours_calc,
        case when lower(status) in ('closed','resolved','completed') or resolved_at is not null then 1 else 0 end as is_closed
    from base
)

-- avg resolution per category & priority
, avg_res as (
    select category, priority, avg(resolution_hours_calc) as avg_resolution_hrs
    from with_resolution
    group by category, priority
)

-- closure rate per assigned group
, closure as (
    select assigned_group, sum(is_closed) as closed_count, count(*) as total_count,
           (sum(is_closed)::float / nullif(count(*),0)) as closure_rate
    from with_resolution
    group by assigned_group
)

-- monthly summary
, monthly as (
    select date_trunc('month', created_at) as month_start,
           count(*) as tickets,
           avg(resolution_hours_calc) as avg_resolution_hrs,
           sum(is_closed)::float / nullif(count(*),0) as closure_rate
    from with_resolution
    group by date_trunc('month', created_at)
)

select * from avg_res;
-- You can create separate models for closure and monthly; here we provide queries for reference.