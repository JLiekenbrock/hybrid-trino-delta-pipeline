-- Local demo: TPCH lineitem represents immutable transaction facts.
select
    cast('tpch' as varchar) as tenant_id,
    concat(cast(l.orderkey as varchar), '-', cast(l.linenumber as varchar)) as transaction_id,
    cast(l.orderkey as varchar) as account_id,
    cast(o.custkey as varchar) as customer_id,
    cast(o.orderdate as timestamp(6)) as transaction_ts,
    date '{business_date}' as business_date,
    cast(l.extendedprice as decimal(18, 2)) as amount,
    cast('USD' as varchar) as currency
from lineitem l
inner join orders o on l.orderkey = o.orderkey
inner join customer c on o.custkey = c.custkey
where ({customer_segment} is null or c.mktsegment = {customer_segment})
  and ({all_tenants} or 'tpch' in ({tenant_values}))
  and ({account_status} is null or o.orderstatus = {account_status})
  and l.extendedprice >= {minimum_transaction_amount}
