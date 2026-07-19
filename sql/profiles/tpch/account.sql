-- Local demo: TPCH orders represent accounts owned by TPCH customers.
select
    cast(null as varchar) as merge_key,
    cast('tpch' as varchar) as tenant_id,
    cast(o.orderkey as varchar) as account_id,
    cast(o.custkey as varchar) as customer_id,
    cast(o.orderstatus as varchar) as account_type,
    timestamp '{start_ts}' as valid_from,
    timestamp '9999-12-31 23:59:59.999999' as valid_to,
    true as is_current,
    to_hex(md5(to_utf8(concat(cast(o.custkey as varchar), '|', o.orderstatus)))) as attribute_hash,
    cast(bitwise_and(o.orderkey, 255) as integer) as entity_bucket
from orders o
inner join customer c on o.custkey = c.custkey
where ({customer_segment} is null or c.mktsegment = {customer_segment})
  and ({all_tenants} or 'tpch' in ({tenant_values}))
  and ({account_status} is null or o.orderstatus = {account_status})
