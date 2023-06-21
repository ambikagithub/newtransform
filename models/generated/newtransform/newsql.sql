{{
  config(
    materialized='table',
    unique_key='id'
  )
}}

{%- set newsql = ref('date_convertor_ab3') ~ '_new' -%}
{%- set customer_type = 'new_column' -%}

{% set custom_values = dbt_utils.get_column_values(
    table=ref('date_convertor_ab3'),
    column='customer_domain'
) %}
{% for a in customer_values %}
    {% if a == 'hpe.com' %}
        'internal'    customer_type
    else
        'external'    customer_type
    {% endif %}
{% endfor %}


CREATE TABLE {{ newsql }} AS (
select
customer_domain,
NULL::dbt_utils.type_string() AS {{ customer_type }},
_airbyte_ab_id,
_airbyte_emitted_at,
{{ current_timestamp() }} as _airbyte_normalized_at,
_airbyte_date_convertor_hashid
from {{ ref('date_convertor_ab3') }}

where 1 = 1
)
