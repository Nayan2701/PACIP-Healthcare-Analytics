/*
  PACIP Mart: PA Compliance Scorecard
  CMS-0057-F requires payers to report PA approval/denial rates publicly.
  This mart produces the compliance scorecard per payer.
*/
with pa_scores as (
    select * from {{ ref('stg_pa_risk_scores') }}
),

payer_perf as (
    select * from {{ ref('stg_payer_performance') }}
),

compliance_scorecard as (
    select
        pp.payer_name,
        pp.payer_tier,
        pp.unique_patients,
        pp.total_claims,
        pp.total_approved,
        pp.avg_approval_rate,
        pp.avg_denial_rate,
        pp.avg_payment_amount,
        pp.avg_submitted_charge,
        pp.total_revenue_at_risk,
        -- PA risk metrics from scored requests
        count(pa.service_request_id) as total_pa_requests,
        sum(case when pa.risk_tier = 'HIGH' then 1 else 0 end) as high_risk_pa_count,
        sum(case when pa.risk_tier = 'MEDIUM' then 1 else 0 end) as medium_risk_pa_count,
        sum(case when pa.risk_tier = 'LOW' then 1 else 0 end) as low_risk_pa_count,
        round(avg(pa.pa_risk_score), 4) as avg_pa_risk_score,
        round(sum(pa.revenue_at_risk), 2) as total_pa_revenue_at_risk,
        -- CMS-0057-F compliance flags
        case
            when pp.avg_denial_rate > 0.35 then 'NON_COMPLIANT_HIGH_DENIAL'
            when pp.avg_denial_rate > 0.25 then 'WATCH_LIST'
            else 'COMPLIANT'
        end as cms_compliance_status,
        current_timestamp as mart_updated_at
    from payer_perf pp
    left join pa_scores pa
        on pp.payer_name = pa.payer_name
    group by
        pp.payer_name, pp.payer_tier, pp.unique_patients,
        pp.total_claims, pp.total_approved, pp.avg_approval_rate,
        pp.avg_denial_rate, pp.avg_payment_amount, pp.avg_submitted_charge,
        pp.total_revenue_at_risk
)

select * from compliance_scorecard
order by total_pa_revenue_at_risk desc
