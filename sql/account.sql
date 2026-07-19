-- Adapt source and published table names to your Trino catalog.
with ranked_source as (
    select
        cast(a.account_id as varchar) as account_id,
        cast(a.customer_id as varchar) as customer_id,
        cast(a.account_type as varchar) as account_type,
        cast(a.updated_at as timestamp(6)) as effective_at,
        row_number() over (partition by a.account_id order by a.updated_at desc) as row_number
    from analytics.accounts a
    inner join published.customer c
        on cast(a.customer_id as varchar) = c.customer_id
       and c.is_current = true
    where a.updated_at >= timestamp '{start_ts}'
      and a.updated_at < timestamp '{end_ts}'
      and ({customer_segment} is null or c.customer_segment = {customer_segment})
      and ({account_status} is null or cast(a.account_type as varchar) = {account_status})
),
incoming as (
    select
        account_id,
        customer_id,
        account_type,
        effective_at,
        to_hex(md5(to_utf8(concat(customer_id, '|', coalesce(account_type, ''))))) as attribute_hash,
        cast(bitwise_and(from_big_endian_64(xxhash64(to_utf8(account_id))), 255) as integer) as entity_bucket
    from ranked_source
    where row_number = 1
),
current_target as (
    select account_id, attribute_hash
    from published.account
    where is_current = true
),
changes as (
    select i.*, t.account_id as existing_account_id
    from incoming i
    left join current_target t using (account_id)
    where t.account_id is null or t.attribute_hash <> i.attribute_hash
)
select
    cast(account_id as varchar) as merge_key,
    account_id,
    customer_id,
    account_type,
    effective_at as valid_from,
    effective_at as valid_to,
    false as is_current,
    attribute_hash,
    entity_bucket
from changes
where existing_account_id is not null
union all
select
    cast(null as varchar) as merge_key,
    account_id,
    customer_id,
    account_type,
    effective_at as valid_from,
    timestamp '9999-12-31 23:59:59.999999' as valid_to,
    true as is_current,
    attribute_hash,
    entity_bucket
from changes
