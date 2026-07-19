-- depends_on: {{ source('trino_tpch', 'customer') }}
select *
from delta_scan('{{ var("output_uri") }}/customer')
