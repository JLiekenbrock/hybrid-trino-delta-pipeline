with versions as (
    select
        tenant_id,
        account_id,
        valid_from,
        valid_to,
        lag(valid_to) over (partition by tenant_id, account_id order by valid_from) as previous_valid_to
    from {{ ref('account') }}
)
select *
from versions
where valid_from >= valid_to
   or (previous_valid_to is not null and valid_from < previous_valid_to)
