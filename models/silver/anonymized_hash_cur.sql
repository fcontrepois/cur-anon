-- models/silver/anonymized_hash_cur.sql

with raw as (
  select * from {{ ref('raw_cur') }}
),
bill_payer_map as (
  select * from {{ ref('bill_payer_account_id_map') }}
),
line_item_usage_map as (
  select * from {{ ref('line_item_usage_account_id_map') }}
),
line_item_resource_map as (
  select * from {{ ref('line_item_resource_id_map') }}
),
reservation_subscription_map as (
  select * from {{ ref('reservation_subscription_id_map') }}
)

select
  bill_payer_map.bill_payer_account_id_anon as bill_payer_account_id,
  line_item_usage_map.line_item_usage_account_id_anon as line_item_usage_account_id,
  line_item_resource_map.line_item_resource_id_anon as line_item_resource_id,
  reservation_subscription_map.reservation_subscription_id_anon as reservation_subscription_id,
  -- Dynamically include all resource_tags columns
  {%- set columns = adapter.get_columns_in_relation(ref('raw_cur')) -%}
  {%- for col in columns if col.name.startswith('resource_tags') %}
    raw.{{ col.name }}{% if not loop.last %},{% endif %}
  {%- endfor %}
  {%- set anon_columns = [
      'bill_payer_account_id',
      'line_item_usage_account_id',
      'line_item_resource_id',
      'reservation_subscription_id'
    ] -%}
  {%- for col in columns if not col.name.startswith('resource_tags') and col.name not in anon_columns %}
    ,raw.{{ col.name }}
  {%- endfor %}
from raw
left join bill_payer_map
  on raw.bill_payer_account_id = bill_payer_map.bill_payer_account_id
left join line_item_usage_map
  on raw.line_item_usage_account_id = line_item_usage_map.line_item_usage_account_id
left join line_item_resource_map
  on raw.line_item_resource_id = line_item_resource_map.line_item_resource_id
left join reservation_subscription_map
  on raw.reservation_subscription_id = reservation_subscription_map.reservation_subscription_id

