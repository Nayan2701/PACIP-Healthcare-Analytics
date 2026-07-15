/*
  PACIP Mart: Payer Benchmarks
  Cross-payer benchmarking for provider contract negotiations.
  Shows how each payer compares to the market median.
*/
with approval_rates as (
    select * from {{ ref('stg_approval_rates') }}
),

payer_summary as (
    select
        ar_payer_name as payer_name,
        sum(total_claims) as total_claims,
        sum(total_approved) as total_approved,
        round(
            sum(total_approved)::decimal / nullif(sum(total_claims), 0), 4
        ) as overall_approval_rate,
        round(avg(avg_payment_amount), 2) as avg_payment,
        round(avg(avg_submitted_charge), 2) as avg_charge,
        round(avg(charge_to_payment_ratio), 4) as avg_charge_ratio,
        count(distinct ar_claim_type) as claim_types_covered
    from approval_rates
    group by ar_payer_name
),

benchmarks as (
    select
        payer_name,
        total_claims,
        total_approved,
        overall_approval_rate,
        avg_payment,
        avg_charge,
        avg_charge_ratio,
        claim_types_covered,
        -- Market median comparison
        round(
            overall_approval_rate - avg(overall_approval_rate) over (), 4
        ) as vs_market_approval_rate,
        round(
            avg_charge - avg(avg_charge) over (), 2
        ) as vs_market_avg_charge,
        -- Rank by approval rate
        rank() over (order by overall_approval_rate desc) as approval_rate_rank,
        count(*) over () as total_payers,
        current_timestamp as mart_updated_at
    from payer_summary
)

select * from benchmarks
order by approval_rate_rank
