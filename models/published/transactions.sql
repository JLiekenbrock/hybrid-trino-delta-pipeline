-- depends_on: {{ ref('account') }}
select *
from delta_scan('{{ var("output_uri") }}/transactions')
