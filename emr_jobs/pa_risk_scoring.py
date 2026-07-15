from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import sys

spark = SparkSession.builder \
    .appName("PACIP-PA-Risk-Scoring-v12") \
    .config("spark.sql.adaptive.enabled", "true") \
    .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

BUCKET = sys.argv[1]
SILVER = f"s3://{BUCKET}/silver"
GOLD   = f"s3://{BUCKET}/gold"

print("PACIP Module 3 v12 starting")

# ── EOB — aggregate payment per patient ───────────────────────────────────────
eob = spark.read.parquet(f"{SILVER}/explanations/").select(
    F.col("patient_id").alias("eob_pid"),
    F.col("payment_amount").alias("eob_amt")
).filter(F.col("eob_amt").isNotNull())

eob_agg = eob.groupBy("eob_pid").agg(
    F.count("*").alias("total_claims"),
    F.sum(F.when(F.col("eob_amt") > 0, 1).otherwise(0)).alias("approved_claims"),
    F.round(F.avg(F.when(F.col("eob_amt") > 0, 1).otherwise(0)), 4).alias("approval_rate"),
    F.round(F.avg("eob_amt"), 2).alias("avg_payment")
)
print(f"EOB agg rows: {eob_agg.count()}")

# ── CLAIMS — aggregate per patient ────────────────────────────────────────────
claims = spark.read.parquet(f"{SILVER}/claims/").select(
    F.col("patient_id").alias("cl_pid"),
    F.col("claim_type").alias("cl_type"),
    F.col("total_submitted_charge").alias("cl_charge")
).filter(F.col("cl_type").isNotNull())

claims_agg = claims.groupBy("cl_pid").agg(
    F.first("cl_type", ignorenulls=True).alias("primary_claim_type"),
    F.round(F.avg("cl_charge"), 2).alias("avg_charge")
)
print(f"Claims agg rows: {claims_agg.count()}")

# ── COVERAGE — simple read, no cov_max join ───────────────────────────────────
# Athena confirms payer_name IS populated. Just read + deduplicate.
cov_all = spark.read.parquet(f"{SILVER}/coverages/")
print(f"Raw coverage rows: {cov_all.count()}")
print("Coverage schema:")
cov_all.printSchema()
print("Coverage sample:")
cov_all.show(3, truncate=False)

cov = cov_all.select(
    F.col("patient_id").alias("cov_pid"),
    F.col("payer_name").alias("cov_payer"),
    F.col("coverage_type").alias("cov_type")
).dropDuplicates(["cov_pid"])
print(f"Coverage deduped rows: {cov.count()}")

# ── PA requests ───────────────────────────────────────────────────────────────
pa = spark.read.parquet(f"{SILVER}/prior_auth/").select(
    F.col("service_request_id").alias("pa_id"),
    F.col("patient_id").alias("pa_pid"),
    F.col("status").alias("pa_status"),
    F.col("authored_on").alias("pa_authored")
)
print(f"PA rows: {pa.count()}")

# ── Patient profile: claims_agg + eob_agg + coverage ─────────────────────────
profile = claims_agg.join(
    eob_agg,
    on=[claims_agg.cl_pid == eob_agg.eob_pid],
    how="inner"
).join(
    cov,
    on=[claims_agg.cl_pid == cov.cov_pid],
    how="left"
).select(
    claims_agg.cl_pid.alias("patient_id"),
    claims_agg.primary_claim_type,
    claims_agg.avg_charge,
    eob_agg.total_claims,
    eob_agg.approved_claims,
    eob_agg.approval_rate,
    eob_agg.avg_payment,
    cov.cov_payer.alias("payer_name"),
    cov.cov_type.alias("coverage_type")
)
print(f"Profile rows (before payer filter): {profile.count()}")

profile_filtered = profile.filter(F.col("payer_name").isNotNull())
print(f"Profile rows (after payer filter): {profile_filtered.count()}")

# ── Gold Table 1: Approval rates by claim_type x payer ───────────────────────
approval_rates = profile_filtered.groupBy(
    F.col("primary_claim_type").alias("ar_claim_type"),
    F.col("payer_name").alias("ar_payer_name")
).agg(
    F.count("*").alias("total_patients"),
    F.sum("total_claims").alias("total_claims"),
    F.sum("approved_claims").alias("total_approved"),
    F.round(F.avg("approval_rate"), 4).alias("approval_rate"),
    F.round(1.0 - F.avg("approval_rate"), 4).alias("denial_rate"),
    F.round(F.avg("avg_payment"), 2).alias("avg_payment_amount"),
    F.round(F.avg("avg_charge"), 2).alias("avg_submitted_charge")
)

approval_rates.write.mode("overwrite") \
    .parquet(f"{GOLD}/procedure_approval_rates/")
print(f"Approval rates rows: {approval_rates.count()}")
approval_rates.show(20, truncate=False)

# ── Gold Table 2: Payer performance ──────────────────────────────────────────
payer_perf = profile_filtered.groupBy("payer_name").agg(
    F.count("*").alias("unique_patients"),
    F.sum("total_claims").alias("total_claims"),
    F.sum("approved_claims").alias("total_approved"),
    F.round(F.avg("approval_rate"), 4).alias("avg_approval_rate"),
    F.round(1.0 - F.avg("approval_rate"), 4).alias("avg_denial_rate"),
    F.round(F.avg("avg_payment"), 2).alias("avg_payment_amount"),
    F.round(F.avg("avg_charge"), 2).alias("avg_submitted_charge"),
    F.round(F.sum("avg_charge") * (1.0 - F.avg("approval_rate")), 2)
        .alias("total_revenue_at_risk")
)

payer_perf.write.mode("overwrite").parquet(f"{GOLD}/payer_performance/")
print(f"Payer performance rows: {payer_perf.count()}")
payer_perf.show(20, truncate=False)

# ── Gold Table 3: PA risk scores ──────────────────────────────────────────────
pa_profile = pa.join(
    profile_filtered.select(
        "patient_id", "payer_name", "coverage_type",
        "primary_claim_type", "approval_rate", "avg_charge",
        F.round(1.0 - F.col("approval_rate"), 4).alias("denial_rate")
    ),
    on=[pa.pa_pid == F.col("patient_id")],
    how="left"
).select(
    pa.pa_id.alias("service_request_id"),
    pa.pa_pid.alias("patient_id"),
    pa.pa_status.alias("status"),
    pa.pa_authored.alias("authored_on"),
    F.col("payer_name"),
    F.col("coverage_type"),
    F.col("primary_claim_type").alias("claim_type"),
    F.col("approval_rate").alias("historical_approval_rate"),
    F.col("denial_rate").alias("historical_denial_rate"),
    F.col("avg_charge").alias("avg_submitted_charge")
).withColumn("pa_risk_score",
    F.round(F.coalesce(F.col("historical_denial_rate"), F.lit(0.5)), 4)
).withColumn("risk_tier",
    F.when(F.col("pa_risk_score") >= 0.7, "HIGH")
    .when(F.col("pa_risk_score") >= 0.4, "MEDIUM")
    .otherwise("LOW")
).withColumn("revenue_at_risk",
    F.round(
        F.coalesce(F.col("avg_submitted_charge"), F.lit(0.0)) *
        F.col("pa_risk_score"), 2
    )
)

pa_profile.write.mode("overwrite").parquet(f"{GOLD}/pa_risk_scores/")
print(f"PA risk scores rows: {pa_profile.count()}")

print("\n=== Risk Tier Distribution ===")
pa_profile.groupBy("risk_tier").agg(
    F.count("*").alias("count"),
    F.round(F.avg("pa_risk_score"), 4).alias("avg_risk_score"),
    F.round(F.sum("revenue_at_risk"), 2).alias("total_revenue_at_risk")
).orderBy("risk_tier").show()

print("\n=== Risk by Payer ===")
pa_profile.filter(F.col("payer_name").isNotNull()) \
    .groupBy("payer_name") \
    .agg(
        F.count("*").alias("pa_requests"),
        F.round(F.avg("pa_risk_score"), 4).alias("avg_risk_score"),
        F.round(F.sum("revenue_at_risk"), 2).alias("total_revenue_at_risk")
    ).orderBy(F.desc("total_revenue_at_risk")).show(20, truncate=False)

print("\nPACIP Module 3 v12 complete")
spark.stop()
