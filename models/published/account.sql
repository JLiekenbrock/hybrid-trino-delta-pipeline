-- depends_on: {{ ref('customer') }}
select *
from delta_scan('{{ var("output_uri") }}/account')
