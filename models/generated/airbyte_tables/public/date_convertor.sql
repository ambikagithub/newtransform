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
select
    _id,
    cq3,
    cq7,
    cq8,
    cq9,
    cq10,
    cq16,
    cq4t,
    cq10a,
    cq10b,
    cq13b,
    queue,
    region,
    status,
    severity,
    bug_cases,
    case_type,
    defect_id,
    asset_name,
    case_owner,
    openeddate,
    case_number,
    case_origin,
    account_name,
    federal_case,
    product_line,
    case_category,
    customer_type,
    handover_type,
    loyalty_index,
    product_group,
    exclusion_flag,
    previous_owner,
    product_number,
    requestor_lang,
    escalation_date,
    customer_domain,
    customer_ref_id,
    detailed_status,
    escalation_date_1,
    escalation_flag,
    resolution_code,
    resolution_date,
    case_subcategory,
    date_time_closed,
    date_time_opened,
    entitlement_type,
    escalation_tier1,
    escalation_tier2,
    escalation_tier3,
    escalation_tier4,
    highest_severity,
    physical_country,
    product_category,
    software_version,
    availability_flag,
    account_page_level,
    case_contact_email,
    case_owner_manager,
    overall_experience,
    parent_case_number,
    resolution_subcode,
    entitlement_summary,
    part_arrived_ontime,
    survey_received_date,
    esc_addressed_datetime,
    closeddate_aibyte_transform,
    resolution_provided_satisfactory,
    _airbyte_ab_id,
    _airbyte_emitted_at,
    {{ current_timestamp() }} as _airbyte_normalized_at,
    _airbyte_date_convertor_hashid
from {{ ref('date_convertor_ab3') }}
-- date_convertor from {{ source('public', '_airbyte_raw_date_convertor') }}
where 1 = 1

