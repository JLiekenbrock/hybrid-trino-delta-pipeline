-- Local demo: TPCH customer is emitted as a stable SCD2 snapshot.
select
    cast(null as varchar) as merge_key,
    cast('tpch' as varchar) as tenant_id,
    cast(custkey as varchar) as customer_id,
    cast(mktsegment as varchar) as customer_segment,
    timestamp '{start_ts}' as valid_from,
    timestamp '9999-12-31 23:59:59.999999' as valid_to,
    true as is_current,
    to_hex(md5(to_utf8(concat(name, '|', mktsegment, '|', cast(nationkey as varchar))))) as attribute_hash,
    cast(bitwise_and(custkey, 255) as integer) as entity_bucket
from customer
where ({all_tenants} or 'tpch' in ({tenant_values}))
  and ({customer_segment} is null or mktsegment = {customer_segment})
