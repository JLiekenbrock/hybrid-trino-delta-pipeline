-- Local demo: TPCH orders represent accounts owned by TPCH customers.
select
    cast(null as varchar) as merge_key,
    cast(orderkey as varchar) as account_id,
    cast(custkey as varchar) as customer_id,
    cast(orderstatus as varchar) as account_type,
    timestamp '{start_ts}' as valid_from,
    timestamp '9999-12-31 23:59:59.999999' as valid_to,
    true as is_current,
    to_hex(md5(to_utf8(concat(cast(custkey as varchar), '|', orderstatus)))) as attribute_hash,
    cast(bitwise_and(orderkey, 255) as integer) as entity_bucket
from orders
