select *
from delta_scan('{{ var("output_uri") }}/customer')
