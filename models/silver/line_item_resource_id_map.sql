-- models/silver/line_item_resource_id_map.sql
select
  line_item_resource_id,
  md5(cast(line_item_resource_id as text)) as line_item_resource_id_anon
from (
  select distinct line_item_resource_id
  from {{ ref('raw_cur') }}
) t

