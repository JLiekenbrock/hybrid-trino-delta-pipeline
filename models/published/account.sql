-- depends_on: {{ ref('customer') }}
-- depends_on: {{ source('trino_tpch', 'orders') }}
select *
from delta_scan('{{ var("output_uri") }}/account')
