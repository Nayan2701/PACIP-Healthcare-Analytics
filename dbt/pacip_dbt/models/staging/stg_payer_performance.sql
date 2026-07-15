with source as (
    select * from {{ source('pacip_spectrum', 'payer_performance') }}
),

staged as (
    select
        payer_name,
        unique_patients,
        total_claims,
        total_approved,
        avg_approval_rate,
        avg_denial_rate,
        avg_payment_amount,
        avg_submitted_charge,
        total_revenue_at_risk,
        -- Tier classification
        case
            when avg_approval_rate >= 0.80 then 'TIER_1_BEST'
            when avg_approval_rate >= 0.75 then 'TIER_2_GOOD'
            when avg_approval_rate >= 0.70 then 'TIER_3_AVERAGE'
            else 'TIER_4_POOR'
        end as payer_tier,
        current_timestamp as dbt_loaded_at
    from source
    where payer_name is not null
)

select * from staged
