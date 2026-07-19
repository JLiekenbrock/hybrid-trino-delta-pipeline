-- Initial customer load: the target Delta table does not exist yet.
with ranked_source as (
    select
        cast(tenant_id as varchar) as tenant_id,
        cast(customer_id as varchar) as customer_id,
        cast(customer_segment as varchar) as customer_segment,
        cast(updated_at as timestamp(6)) as effective_at,
        row_number() over (partition by tenant_id, customer_id order by updated_at desc) as row_number
    from analytics.customers
    where updated_at < timestamp '{end_ts}'
      and ({customer_segment} is null or cast(customer_segment as varchar) = {customer_segment})
)
select
    cast(null as varchar) as merge_key,
    tenant_id,
    customer_id,
    customer_segment,
    effective_at as valid_from,
    timestamp '9999-12-31 23:59:59.999999' as valid_to,
    true as is_current,
    to_hex(md5(to_utf8(coalesce(customer_segment, '')))) as attribute_hash,
    cast(bitwise_and(from_big_endian_64(xxhash64(to_utf8(concat(tenant_id, '|', customer_id)))), 255) as integer) as entity_bucket
from ranked_source
where row_number = 1
