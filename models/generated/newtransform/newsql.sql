
{% set cust_domains = ["hpe.com", "arubanetworks.com", "hpe.hr","hpecds.com","jpn.hpe.com","hpecds.com"] %}

select
customer_domain,
{% for customer_domain in cust_domains %}
(case when customer_domain = '{{customer_domain}}' then 'Internal' end) as customer_type,
{% endfor %}
_airbyte_ab_id,
_airbyte_emitted_at,
{{ current_timestamp() }} as _airbyte_normalized_at,
_airbyte_date_convertor_hashid
from {{ ref('date_convertor_ab3') }}
-- date_convertor from {{ source('public', '_airbyte_raw_date_convertor') }}
where 1 = 1


