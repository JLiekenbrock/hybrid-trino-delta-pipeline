-- Transactions are immutable facts. One business_date partition is replaced on retry.
select
    cast(t.transaction_id as varchar) as transaction_id,
    cast(t.account_id as varchar) as account_id,
    a.customer_id,
    cast(t.transaction_ts as timestamp(6)) as transaction_ts,
    cast(t.transaction_ts as date) as business_date,
    cast(t.amount as decimal(18, 2)) as amount,
    cast(t.currency as varchar) as currency
from analytics.transactions t
inner join published.account a
    on cast(t.account_id as varchar) = a.account_id
   and a.is_current = true
where t.transaction_ts >= timestamp '{start_ts}'
  and t.transaction_ts < timestamp '{end_ts}'
  and t.status = 'posted'
