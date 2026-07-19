-- Initial account load after the customer Delta table has been published to Trino.
with ranked_source as (
    select
        cast(a.tenant_id as varchar) as tenant_id,
        cast(a.account_id as varchar) as account_id,
        cast(a.customer_id as varchar) as customer_id,
        cast(a.account_type as varchar) as account_type,
        cast(a.updated_at as timestamp(6)) as effective_at,
        row_number() over (partition by a.tenant_id, a.account_id order by a.updated_at desc) as row_number
    from analytics.accounts a
    inner join published.customer c
        on cast(a.tenant_id as varchar) = c.tenant_id
       and cast(a.customer_id as varchar) = c.customer_id
       and c.is_current = true
    where a.updated_at < timestamp '{end_ts}'
      and ({all_tenants} or cast(a.tenant_id as varchar) in ({tenant_values}))
      and ({customer_segment} is null or c.customer_segment = {customer_segment})
      and ({account_status} is null or cast(a.account_type as varchar) = {account_status})
)
select
    cast(null as varchar) as merge_key,
    tenant_id,
    account_id,
    customer_id,
    account_type,
    effective_at as valid_from,
    timestamp '9999-12-31 23:59:59.999999' as valid_to,
    true as is_current,
    to_hex(md5(to_utf8(concat(customer_id, '|', coalesce(account_type, ''))))) as attribute_hash,
    cast(bitwise_and(from_big_endian_64(xxhash64(to_utf8(concat(tenant_id, '|', account_id)))), 255) as integer) as entity_bucket
from ranked_source
where row_number = 1
