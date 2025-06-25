{#-- List additional columns to anonymize and their masking settings --#}
{%- set extra_masked_columns = [
    {'name': 'bill_payer_account_id', 'n': 8, 'keep_n': 4, 'keep_dir': 'left'},
    {'name': 'line_item_usage_account_id', 'n': 8, 'keep_n': 4, 'keep_dir': 'left'},
    {'name': 'line_item_resource_id', 'n': 8, 'keep_n': 4, 'keep_dir': 'left'},
    {'name': 'reservation_subscription_id', 'n': 8, 'keep_n': 4, 'keep_dir': 'left'}
] -%}

{%- set columns = adapter.get_columns_in_relation(ref('raw_cur')) -%}
{%- set tag_columns = [] -%}
{%- set masked_columns = extra_masked_columns | map(attribute='name') | list -%}
{%- set other_columns = [] -%}

{%- for col in columns -%}
    {%- if col.name.startswith('resource_tags') -%}
        {%- do tag_columns.append(col.name) -%}
    {%- elif col.name not in masked_columns -%}
        {%- do other_columns.append(col.name) -%}
    {%- endif -%}
{%- endfor -%}

select
    -- Mask extra specified columns
    {%- for col in extra_masked_columns %}
    {{ dbt_privacy.safe_mask(
        col.name,
        n=col.n,
        keep_n=col.keep_n,
        keep_dir='' ~ col.keep_dir ~ ''
    ) }} as {{ col.name }}{{ "," if not loop.last or tag_columns | length > 0 }}
    {%- endfor %}

    -- Mask all resource_tags columns dynamically
    {%- for col in tag_columns %}
    {{ dbt_privacy.safe_mask(col, n=8, keep_n=4, keep_dir="right") }} as {{ col }}{{ "," if not loop.last or other_columns | length > 0 }}
    {%- endfor %}

    -- Select all other columns as-is
    {{ other_columns | join(',\n    ') }}

from {{ ref('raw_cur') }}

