import streamlit as st
import os
import snowflake.connector
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go

# ── Page config ───────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="PACIP — Prior Authorization Intelligence",
    page_icon="🏥",
    layout="wide",
    initial_sidebar_state="expanded"
)

# ── Snowflake connection ──────────────────────────────────────────────────────
@st.cache_resource
def get_connection():
    # Use env vars on Render, fall back to secrets.toml for local dev
    try:
        user = os.environ["SNOWFLAKE_USER"]
        password = os.environ["SNOWFLAKE_PASSWORD"]
        account = os.environ["SNOWFLAKE_ACCOUNT"]
    except KeyError:
        user = st.secrets["snowflake"]["user"]
        password = st.secrets["snowflake"]["password"]
        account = st.secrets["snowflake"]["account"]
    return snowflake.connector.connect(
        user=user,
        password=password,
        account=account,
        warehouse="PACIP_WH",
        database="PACIP_DB",
        schema="ANALYTICS"
    )

@st.cache_data(ttl=300)
def run_query(query):
    conn = get_connection()
    cur = conn.cursor()
    cur.execute(query)
    df = pd.DataFrame(cur.fetchall(), columns=[d[0] for d in cur.description])
    return df

# ── Sidebar ───────────────────────────────────────────────────────────────────
st.sidebar.image(
    "https://img.shields.io/badge/PACIP-Healthcare%20Analytics-blue",
    use_column_width=True
)
st.sidebar.title("PACIP Dashboard")
st.sidebar.markdown("""
**Prior Authorization & Claims Intelligence Platform**

Built on:
- AWS Glue + EMR Serverless
- Redshift Serverless + dbt
- Snowflake Secure Data Share
- Step Functions + Airflow

*Aligned with CMS-0057-F Prior Authorization mandate*
""")

page = st.sidebar.radio(
    "Navigate",
    ["PA Compliance Scorecard",
     "Revenue at Risk",
     "Payer Benchmarks",
     "High Risk PA Explorer"]
)

# ── PA Compliance Scorecard ───────────────────────────────────────────────────
if page == "PA Compliance Scorecard":
    st.title("🏥 PA Compliance Scorecard")
    st.markdown(
        "CMS-0057-F requires payers to report Prior Authorization "
        "approval/denial rates. This scorecard tracks compliance status per payer."
    )

    df = run_query("SELECT * FROM VW_PA_COMPLIANCE_SCORECARD")

    # KPI metrics row
    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Total PA Requests",
                f"{df['TOTAL_PA_REQUESTS'].sum():,.0f}")
    col2.metric("High Risk PAs",
                f"{df['HIGH_RISK_PA_COUNT'].sum():,.0f}")
    col3.metric("Total Revenue at Risk",
                f"${df['TOTAL_PA_REVENUE_AT_RISK'].sum()/1e6:.1f}M")
    col4.metric("Non-Compliant Payers",
                f"{(df['CMS_COMPLIANCE_STATUS'] == 'NON_COMPLIANT_HIGH_DENIAL').sum()}")

    st.divider()

    # Compliance status by payer
    col1, col2 = st.columns(2)

    with col1:
        st.subheader("Compliance Status by Payer")
        fig = px.bar(
            df.sort_values('TOTAL_PA_REVENUE_AT_RISK', ascending=True),
            x='TOTAL_PA_REVENUE_AT_RISK',
            y='PAYER_NAME',
            color='CMS_COMPLIANCE_STATUS',
            orientation='h',
            color_discrete_map={
                'COMPLIANT': '#2ecc71',
                'WATCH_LIST': '#f39c12',
                'NON_COMPLIANT_HIGH_DENIAL': '#e74c3c'
            },
            labels={
                'TOTAL_PA_REVENUE_AT_RISK': 'Revenue at Risk ($)',
                'PAYER_NAME': 'Payer'
            }
        )
        fig.update_layout(height=400, showlegend=True)
        st.plotly_chart(fig, use_container_width=True)

    with col2:
        st.subheader("Risk Tier Distribution")
        risk_data = pd.DataFrame({
            'Risk Tier': ['HIGH', 'MEDIUM', 'LOW'],
            'Count': [
                df['HIGH_RISK_PA_COUNT'].sum(),
                df['MEDIUM_RISK_PA_COUNT'].sum(),
                df['LOW_RISK_PA_COUNT'].sum()
            ]
        })
        fig = px.pie(
            risk_data,
            values='Count',
            names='Risk Tier',
            color='Risk Tier',
            color_discrete_map={
                'HIGH': '#e74c3c',
                'MEDIUM': '#f39c12',
                'LOW': '#2ecc71'
            },
            hole=0.4
        )
        fig.update_layout(height=400)
        st.plotly_chart(fig, use_container_width=True)

    st.subheader("Detailed Scorecard")
    st.dataframe(
        df[[
            'PAYER_NAME', 'CMS_COMPLIANCE_STATUS', 'TOTAL_PA_REQUESTS',
            'HIGH_RISK_PA_COUNT', 'AVG_PA_RISK_SCORE',
            'TOTAL_PA_REVENUE_AT_RISK', 'AVG_DENIAL_RATE'
        ]].style.format({
            'TOTAL_PA_REVENUE_AT_RISK': '${:,.0f}',
            'AVG_PA_RISK_SCORE': '{:.3f}',
            'AVG_DENIAL_RATE': '{:.1%}'
        }),
        use_container_width=True
    )

# ── Revenue at Risk ───────────────────────────────────────────────────────────
elif page == "Revenue at Risk":
    st.title("💰 Revenue at Risk Analysis")
    st.markdown(
        "Financial exposure from high-denial-rate payer combinations. "
        "Used by hospital CFOs for contract negotiations."
    )

    df = run_query("SELECT * FROM VW_REVENUE_AT_RISK")

    col1, col2, col3 = st.columns(3)
    col1.metric("Total Revenue at Risk",
                f"${df['REVENUE_AT_RISK'].sum()/1e6:.1f}M")
    col2.metric("Critical Risk Combinations",
                f"{(df['FINANCIAL_RISK_LEVEL'] == 'CRITICAL').sum()}")
    col3.metric("Avg Denial Rate",
                f"{df['DENIAL_PCT'].mean():.1f}%")

    st.divider()

    col1, col2 = st.columns(2)

    with col1:
        st.subheader("Revenue at Risk by Payer × Claim Type")
        fig = px.treemap(
            df,
            path=['PAYER_NAME', 'CLAIM_TYPE'],
            values='REVENUE_AT_RISK',
            color='DENIAL_PCT',
            color_continuous_scale='RdYlGn_r',
            labels={'REVENUE_AT_RISK': 'Revenue at Risk ($)'}
        )
        fig.update_layout(height=450)
        st.plotly_chart(fig, use_container_width=True)

    with col2:
        st.subheader("Denial Rate by Risk Level")
        fig = px.scatter(
            df,
            x='DENIAL_PCT',
            y='REVENUE_AT_RISK',
            color='FINANCIAL_RISK_LEVEL',
            size='TOTAL_CLAIMS',
            hover_data=['PAYER_NAME', 'CLAIM_TYPE'],
            color_discrete_map={
                'CRITICAL': '#e74c3c',
                'HIGH': '#e67e22',
                'MEDIUM': '#f39c12',
                'LOW': '#2ecc71'
            },
            labels={
                'DENIAL_PCT': 'Denial Rate (%)',
                'REVENUE_AT_RISK': 'Revenue at Risk ($)'
            }
        )
        fig.update_layout(height=450)
        st.plotly_chart(fig, use_container_width=True)

    st.subheader("Full Revenue at Risk Table")
    st.dataframe(
        df.style.format({
            'REVENUE_AT_RISK': '${:,.0f}',
            'DENIAL_PCT': '{:.1f}%',
            'APPROVAL_PCT': '{:.1f}%',
            'AVG_SUBMITTED_CHARGE': '${:,.2f}'
        }),
        use_container_width=True
    )

# ── Payer Benchmarks ──────────────────────────────────────────────────────────
elif page == "Payer Benchmarks":
    st.title("📊 Payer Benchmarks")
    st.markdown(
        "Cross-payer performance benchmarking. "
        "Shows how each payer compares to the market average approval rate."
    )

    df = run_query("SELECT * FROM VW_PAYER_BENCHMARKS")

    st.subheader("Approval Rate vs Market Average")
    fig = go.Figure()

    market_avg = df['APPROVAL_PCT'].mean()

    fig.add_trace(go.Bar(
        x=df['PAYER_NAME'],
        y=df['APPROVAL_PCT'],
        marker_color=[
            '#2ecc71' if x >= market_avg else '#e74c3c'
            for x in df['APPROVAL_PCT']
        ],
        name='Approval Rate %'
    ))

    fig.add_hline(
        y=market_avg,
        line_dash='dash',
        line_color='white',
        annotation_text=f'Market Avg: {market_avg:.1f}%'
    )

    fig.update_layout(
        height=400,
        xaxis_title='Payer',
        yaxis_title='Approval Rate (%)',
        showlegend=False
    )
    st.plotly_chart(fig, use_container_width=True)

    col1, col2 = st.columns(2)

    with col1:
        st.subheader("Revenue at Risk by Payer")
        fig = px.bar(
            df.sort_values('TOTAL_REVENUE_AT_RISK', ascending=False),
            x='PAYER_NAME',
            y='TOTAL_REVENUE_AT_RISK',
            color='APPROVAL_PCT',
            color_continuous_scale='RdYlGn',
            labels={
                'TOTAL_REVENUE_AT_RISK': 'Revenue at Risk ($)',
                'PAYER_NAME': 'Payer'
            }
        )
        fig.update_layout(height=350)
        st.plotly_chart(fig, use_container_width=True)

    with col2:
        st.subheader("Approval Rate Ranking")
        st.dataframe(
            df[['APPROVAL_RANK', 'PAYER_NAME', 'APPROVAL_PCT',
                'DENIAL_PCT', 'VS_MARKET_APPROVAL_RATE',
                'TOTAL_CLAIMS', 'TOTAL_REVENUE_AT_RISK'
            ]].style.format({
                'APPROVAL_PCT': '{:.1f}%',
                'DENIAL_PCT': '{:.1f}%',
                'VS_MARKET_APPROVAL_RATE': '{:+.3f}',
                'TOTAL_REVENUE_AT_RISK': '${:,.0f}'
            }),
            use_container_width=True,
            height=350
        )

# ── High Risk PA Explorer ─────────────────────────────────────────────────────
elif page == "High Risk PA Explorer":
    st.title("⚠️ High Risk PA Request Explorer")
    st.markdown(
        "Prior authorization requests most likely to be denied. "
        "Utilization management teams use this to prioritize appeals."
    )

    df_all = run_query("""
        SELECT
            service_request_id,
            patient_id,
            payer_name,
            claim_type,
            coverage_type,
            pa_risk_score,
            risk_tier,
            historical_approval_rate,
            historical_denial_rate,
            avg_submitted_charge,
            revenue_at_risk
        FROM pacip_db.raw.pa_risk_scores
        WHERE risk_tier IN ('HIGH', 'MEDIUM')
        ORDER BY pa_risk_score DESC
        LIMIT 5000
    """)

    # Filters
    col1, col2, col3 = st.columns(3)
    with col1:
        payer_filter = st.multiselect(
            "Filter by Payer",
            options=df_all['PAYER_NAME'].unique(),
            default=[]
        )
    with col2:
        tier_filter = st.multiselect(
            "Filter by Risk Tier",
            options=['HIGH', 'MEDIUM'],
            default=['HIGH']
        )
    with col3:
        claim_filter = st.multiselect(
            "Filter by Claim Type",
            options=df_all['CLAIM_TYPE'].dropna().unique(),
            default=[]
        )

    df = df_all.copy()
    if payer_filter:
        df = df[df['PAYER_NAME'].isin(payer_filter)]
    if tier_filter:
        df = df[df['RISK_TIER'].isin(tier_filter)]
    if claim_filter:
        df = df[df['CLAIM_TYPE'].isin(claim_filter)]

    col1, col2, col3 = st.columns(3)
    col1.metric("PA Requests Shown", f"{len(df):,}")
    col2.metric("Avg Risk Score", f"{df['PA_RISK_SCORE'].mean():.3f}")
    col3.metric("Total Revenue at Risk",
                f"${df['REVENUE_AT_RISK'].sum()/1e6:.1f}M")

    st.subheader("Risk Score Distribution")
    fig = px.histogram(
        df,
        x='PA_RISK_SCORE',
        color='RISK_TIER',
        nbins=20,
        color_discrete_map={
            'HIGH': '#e74c3c',
            'MEDIUM': '#f39c12'
        },
        labels={'PA_RISK_SCORE': 'PA Risk Score'}
    )
    fig.update_layout(height=300)
    st.plotly_chart(fig, use_container_width=True)

    st.subheader("PA Requests")
    st.dataframe(
        df[[
            'SERVICE_REQUEST_ID', 'PAYER_NAME', 'CLAIM_TYPE',
            'RISK_TIER', 'PA_RISK_SCORE', 'HISTORICAL_DENIAL_RATE',
            'AVG_SUBMITTED_CHARGE', 'REVENUE_AT_RISK'
        ]].style.format({
            'PA_RISK_SCORE': '{:.3f}',
            'HISTORICAL_DENIAL_RATE': '{:.1%}',
            'AVG_SUBMITTED_CHARGE': '${:,.2f}',
            'REVENUE_AT_RISK': '${:,.2f}'
        }),
        use_container_width=True
    )
