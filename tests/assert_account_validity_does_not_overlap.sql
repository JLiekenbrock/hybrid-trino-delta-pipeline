with versions as (
    select
        account_id,
        valid_from,
        valid_to,
        lag(valid_to) over (partition by account_id order by valid_from) as previous_valid_to
    from {{ ref('account') }}
)
select *
from versions
where valid_from >= valid_to
   or (previous_valid_to is not null and valid_from < previous_valid_to)
