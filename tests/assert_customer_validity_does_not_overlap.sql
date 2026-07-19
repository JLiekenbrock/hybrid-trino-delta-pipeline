with versions as (
    select
        customer_id,
        valid_from,
        valid_to,
        lag(valid_to) over (partition by customer_id order by valid_from) as previous_valid_to
    from {{ ref('customer') }}
)
select *
from versions
where valid_from >= valid_to
   or (previous_valid_to is not null and valid_from < previous_valid_to)
