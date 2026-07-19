-- Adapt analytics.customers and published.customer to your Trino catalog.
-- The query emits staged SCD2 rows for one transactional Delta MERGE.
with ranked_source as (
    select
        cast(tenant_id as varchar) as tenant_id,
        cast(customer_id as varchar) as customer_id,
        cast(customer_segment as varchar) as customer_segment,
        cast(updated_at as timestamp(6)) as effective_at,
        row_number() over (partition by tenant_id, customer_id order by updated_at desc) as row_number
    from analytics.customers
    where updated_at >= timestamp '{start_ts}'
      and updated_at < timestamp '{end_ts}'
      and ({customer_segment} is null or cast(customer_segment as varchar) = {customer_segment})
),
incoming as (
    select
        tenant_id,
        customer_id,
        customer_segment,
        effective_at,
        to_hex(md5(to_utf8(coalesce(customer_segment, '')))) as attribute_hash,
        cast(bitwise_and(from_big_endian_64(xxhash64(to_utf8(concat(tenant_id, '|', customer_id)))), 255) as integer) as entity_bucket
    from ranked_source
    where row_number = 1
),
current_target as (
    select tenant_id, customer_id, attribute_hash
    from published.customer
    where is_current = true
),
changes as (
    select i.*, t.customer_id as existing_customer_id
    from incoming i
    left join current_target t using (tenant_id, customer_id)
    where t.customer_id is null or t.attribute_hash <> i.attribute_hash
)
select
    cast(customer_id as varchar) as merge_key,
    tenant_id,
    customer_id,
    customer_segment,
    effective_at as valid_from,
    effective_at as valid_to,
    false as is_current,
    attribute_hash,
    entity_bucket
from changes
where existing_customer_id is not null
union all
select
    cast(null as varchar) as merge_key,
    tenant_id,
    customer_id,
    customer_segment,
    effective_at as valid_from,
    timestamp '9999-12-31 23:59:59.999999' as valid_to,
    true as is_current,
    attribute_hash,
    entity_bucket
from changes
