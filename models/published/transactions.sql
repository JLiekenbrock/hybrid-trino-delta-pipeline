-- depends_on: {{ ref('account') }}
-- depends_on: {{ source('trino_tpch', 'orders') }}
-- depends_on: {{ source('trino_tpch', 'lineitem') }}
select *
from delta_scan('{{ var("output_uri") }}/transactions')
