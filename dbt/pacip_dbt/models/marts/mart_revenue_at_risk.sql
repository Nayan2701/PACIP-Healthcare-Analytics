/*
  PACIP Mart: Revenue at Risk Analysis
  Quantifies financial exposure from high-denial-rate payer combinations.
  Used by hospital CFOs and healthcare finance teams.
*/
with approval_rates as (
    select * from {{ ref('stg_approval_rates') }}
),

revenue_analysis as (
    select
        ar_claim_type,
        ar_payer_name,
        total_patients,
        total_claims,
        total_approved,
        total_claims - total_approved as total_denied,
        approval_rate,
        denial_rate,
        avg_payment_amount,
        avg_submitted_charge,
        charge_to_payment_ratio,
        -- Revenue at risk = denied claims x avg charge
        round(
            (total_claims - total_approved) * avg_submitted_charge, 2
        ) as revenue_at_risk,
        -- Recovery potential = if approval rate improved to 80%
        round(
            case
                when approval_rate < 0.80
                then (0.80 - approval_rate) * total_claims * avg_payment_amount
                else 0
            end, 2
        ) as recovery_potential_at_80pct,
        -- Risk classification
        case
            when denial_rate >= 0.35 then 'CRITICAL'
            when denial_rate >= 0.25 then 'HIGH'
            when denial_rate >= 0.20 then 'MEDIUM'
            else 'LOW'
        end as financial_risk_level,
        current_timestamp as mart_updated_at
    from approval_rates
)

select * from revenue_analysis
order by revenue_at_risk desc
