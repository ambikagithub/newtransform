{{ config(
    indexes = [{'columns':['_airbyte_emitted_at'],'type':'btree'}],
    unique_key = '_airbyte_ab_id',
    schema = "public",
    post_hook = ["
                    {%
                        set scd_table_relation = adapter.get_relation(
                            database=this.database,
                            schema=this.schema,
                            identifier='date_convertor_scd'
                        )
                    %}
                    {%
                        if scd_table_relation is not none
                    %}
                    {%
                            do adapter.drop_relation(scd_table_relation)
                    %}
                    {% endif %}
                        "],
    tags = [ "top-level" ]
) }}
-- Final base SQL model
-- depends_on: {{ ref('date_convertor_ab3') }}
{% set cust_dom = 'new_customer_domain' %}
{% set cust_domains = ["hpe.com","arubanetworks.com","hpe.hr","hpecds.com","jpn.hpe.com","hpecds.com"] %}
select
coalesce(customer_domain,'no_value') as customer_domain,
substring(case_contact_email, POSITION('@' IN case_contact_email) + 1) AS {{ cust_dom }},
CASE
    WHEN customer_domain = 'hpe.com' THEN 'internal'
    WHEN customer_domain = 'arubanetworks.com' THEN 'internal'
    ELSE 'external'
END AS customer_type,
_airbyte_ab_id,
_airbyte_emitted_at,
{{ current_timestamp() }} as _airbyte_normalized_at,
_airbyte_date_convertor_hashid
from {{ ref('date_convertor_ab3') }}
-- date_convertor from {{ source('public', '_airbyte_raw_date_convertor') }}
where 1 = 1


