select tenant_id, transaction_id
from {{ ref('transactions') }}
group by tenant_id, transaction_id
having count(*) > 1
