-- models/silver/reservation_subscription_id_map.sql
select
  reservation_subscription_id,
  md5(cast(reservation_subscription_id as text)) as reservation_subscription_id_anon
from (
  select distinct reservation_subscription_id
  from {{ ref('raw_cur') }}
) t

