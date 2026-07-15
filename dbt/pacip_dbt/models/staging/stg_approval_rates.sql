with source as (
    select * from {{ source('pacip_spectrum', 'procedure_approval_rates') }}
),

staged as (
    select
        ar_claim_type,
        ar_payer_name,
        total_patients,
        total_claims,
        total_approved,
        approval_rate,
        denial_rate,
        avg_payment_amount,
        avg_submitted_charge,
        -- Financial efficiency ratio
        round(
            case
                when avg_payment_amount > 0
                then avg_submitted_charge / avg_payment_amount
                else null
            end,
            4
        ) as charge_to_payment_ratio,
        current_timestamp as dbt_loaded_at
    from source
    where ar_claim_type is not null
)

select * from staged
