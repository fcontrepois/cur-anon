-- models/silver/bill_payer_account_id_map.sql
select
  bill_payer_account_id,
  md5(cast(bill_payer_account_id as text)) as bill_payer_account_id_anon
from (
  select distinct bill_payer_account_id
  from {{ ref('raw_cur') }}
) t

