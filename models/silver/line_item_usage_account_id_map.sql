-- models/silver/line_item_usage_account_id_map.sql
select
  line_item_usage_account_id,
  md5(cast(line_item_usage_account_id as text)) as line_item_usage_account_id_anon
from (
  select distinct line_item_usage_account_id
  from {{ ref('raw_cur') }}
) t

