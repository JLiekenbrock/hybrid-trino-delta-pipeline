select tenant_id, account_id
from {{ ref('account') }}
group by tenant_id, account_id
having sum(case when is_current then 1 else 0 end) <> 1
