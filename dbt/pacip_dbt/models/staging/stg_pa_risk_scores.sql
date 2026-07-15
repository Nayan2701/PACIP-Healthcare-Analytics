with source as (
    select * from {{ source('pacip_spectrum', 'pa_risk_scores') }}
),

staged as (
    select
        service_request_id,
        patient_id,
        status,
        authored_on,
        payer_name,
        coverage_type,
        claim_type,
        historical_approval_rate,
        historical_denial_rate,
        avg_submitted_charge,
        pa_risk_score,
        risk_tier,
        revenue_at_risk,
        -- Derived fields
        case
            when payer_name = 'NO_INSURANCE' then true
            else false
        end as is_uninsured,
        current_timestamp as dbt_loaded_at
    from source
    where service_request_id is not null
)

select * from staged
