select
    case_number,
    case_origin,
    account_name,
    federal_case,
    exclusion_flag,
    previous_owner,
    product_number,
    requestor_lang,
    escalation_date,
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

