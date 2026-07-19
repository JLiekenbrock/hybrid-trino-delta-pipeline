-- Local demo: TPCH lineitem represents immutable transaction facts.
select
    concat(cast(l.orderkey as varchar), '-', cast(l.linenumber as varchar)) as transaction_id,
    cast(l.orderkey as varchar) as account_id,
    cast(o.custkey as varchar) as customer_id,
    cast(o.orderdate as timestamp(6)) as transaction_ts,
    date '{business_date}' as business_date,
    cast(l.extendedprice as decimal(18, 2)) as amount,
    cast('USD' as varchar) as currency
from lineitem l
inner join orders o on l.orderkey = o.orderkey
