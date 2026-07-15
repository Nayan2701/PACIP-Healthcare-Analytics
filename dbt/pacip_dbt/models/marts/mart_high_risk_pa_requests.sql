/*
  PACIP Mart: High Risk PA Requests
  Identifies prior authorization requests most likely to be denied.
  Used by utilization management teams to prioritize appeals.
*/
with pa_scores as (
    select * from {{ ref('stg_pa_risk_scores') }}
),

high_risk as (
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
        is_uninsured,
        -- Priority score: higher = more urgent to review
        round(
            pa_risk_score * coalesce(avg_submitted_charge, 0) / 1000, 4
        ) as priority_score,
        -- Appeal recommendation
        case
            when pa_risk_score >= 0.7 and avg_submitted_charge > 2000
                then 'IMMEDIATE_APPEAL_RECOMMENDED'
            when pa_risk_score >= 0.7
                then 'APPEAL_RECOMMENDED'
            when pa_risk_score >= 0.4
                then 'MONITOR'
            else 'NO_ACTION'
        end as recommended_action,
        current_timestamp as mart_updated_at
    from pa_scores
    where risk_tier in ('HIGH', 'MEDIUM')
)

select * from high_risk
order by priority_score desc
