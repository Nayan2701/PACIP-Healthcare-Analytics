import sys
import json
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import functions as F

# ── Job init ──────────────────────────────────────────────────────────────────
args = getResolvedOptions(sys.argv, ['JOB_NAME', 'S3_BUCKET'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

BUCKET = args['S3_BUCKET']
BRONZE = f"s3://{BUCKET}/bronze/fhir/synthea/"
SILVER = f"s3://{BUCKET}/silver/"

# ── Read FHIR bundles as raw text ─────────────────────────────────────────────
raw_rdd = sc.wholeTextFiles(BRONZE)

def extract_resources(file_tuple):
    """
    Yields every resource in the bundle — both top-level entry resources
    AND contained resources (Coverage + ServiceRequest live here in Synthea).
    Injects parent_id into contained resource IDs to ensure uniqueness.
    """
    _, content = file_tuple
    try:
        bundle = json.loads(content)
        results = []
        for entry in bundle.get("entry", []):
            resource = entry.get("resource", {})
            if not resource:
                continue

            # Top-level resource
            results.append((
                json.dumps(resource),
                resource.get("resourceType", "")
            ))

            # Contained resources — Coverage and ServiceRequest live here
            parent_id = resource.get("id", "unknown")
            for contained in resource.get("contained", []):
                # Make contained IDs unique by prefixing with parent ID
                # (all Coverage resources have id="coverage" otherwise)
                enriched = dict(contained)
                if "id" in enriched:
                    enriched["id"] = f"{parent_id}_{enriched['id']}"
                results.append((
                    json.dumps(enriched),
                    enriched.get("resourceType", "")
                ))
        return results
    except Exception:
        return []

resources_rdd = raw_rdd.flatMap(extract_resources)
res_df = spark.createDataFrame(
    resources_rdd, ["resource_json", "resourceType"]
).cache()

# Diagnostic — verify all resource types including contained ones
print("=== Resource type distribution (including contained) ===")
res_df.groupBy("resourceType").count().orderBy(F.desc("count")).show(30)

# ── PATIENT ───────────────────────────────────────────────────────────────────
res_df.filter(F.col("resourceType") == "Patient").select(
    F.get_json_object("resource_json", "$.id").alias("patient_id"),
    F.get_json_object("resource_json", "$.birthDate").alias("birth_date"),
    F.get_json_object("resource_json", "$.gender").alias("gender"),
    F.get_json_object("resource_json", "$.address[0].state").alias("state"),
    F.get_json_object("resource_json", "$.address[0].city").alias("city"),
    F.get_json_object("resource_json", "$.address[0].postalCode").alias("postal_code"),
    F.get_json_object("resource_json", "$.maritalStatus.text").alias("marital_status")
).write.mode("overwrite").parquet(f"{SILVER}patients/")
print("✓ Patients done")

# ── CLAIM ─────────────────────────────────────────────────────────────────────
res_df.filter(F.col("resourceType") == "Claim").select(
    F.get_json_object("resource_json", "$.id").alias("claim_id"),
    F.get_json_object("resource_json", "$.status").alias("status"),
    F.get_json_object("resource_json", "$.created").alias("created"),
    F.year(F.to_timestamp(
        F.get_json_object("resource_json", "$.created"))).alias("claim_year"),
    F.regexp_extract(
        F.get_json_object("resource_json", "$.patient.reference"),
        r"(?:Patient/|urn:uuid:)(.+)", 1).alias("patient_id"),
    F.regexp_extract(
        F.get_json_object("resource_json", "$.provider.reference"),
        r"Practitioner/(.+)", 1).alias("provider_id"),
    F.regexp_extract(
        F.get_json_object("resource_json", "$.insurer.reference"),
        r"Organization/(.+)", 1).alias("payer_id"),
    F.get_json_object("resource_json", "$.total.value")
        .cast("double").alias("total_submitted_charge"),
    F.get_json_object("resource_json", "$.type.coding[0].code").alias("claim_type"),
    F.get_json_object("resource_json",
        "$.diagnosis[0].diagnosisCodeableConcept.coding[0].code")
        .alias("primary_diagnosis_code"),
    F.get_json_object("resource_json",
        "$.diagnosis[0].diagnosisCodeableConcept.text")
        .alias("primary_diagnosis_desc")
).write.mode("overwrite").partitionBy("claim_year").parquet(f"{SILVER}claims/")
print("✓ Claims done")

# ── EXPLANATION OF BENEFIT ────────────────────────────────────────────────────
res_df.filter(F.col("resourceType") == "ExplanationOfBenefit").select(
    F.get_json_object("resource_json", "$.id").alias("eob_id"),
    F.get_json_object("resource_json", "$.status").alias("status"),
    F.get_json_object("resource_json", "$.outcome").alias("outcome"),
    F.get_json_object("resource_json", "$.created").alias("created"),
    F.year(F.to_timestamp(
        F.get_json_object("resource_json", "$.created"))).alias("eob_year"),
    F.regexp_extract(
        F.get_json_object("resource_json", "$.patient.reference"),
        r"(?:Patient/|urn:uuid:)(.+)", 1).alias("patient_id"),
    F.regexp_extract(
        F.get_json_object("resource_json", "$.claim.reference"),
        r"Claim/(.+)", 1).alias("claim_id"),
    F.get_json_object("resource_json", "$.payment.amount.value")
        .cast("double").alias("payment_amount"),
    F.get_json_object("resource_json", "$.insurance[0].preAuthRef[0]")
        .alias("prior_auth_ref"),
    F.get_json_object("resource_json", "$.insurance[0].coverage.reference")
        .alias("coverage_ref")
).write.mode("overwrite").partitionBy("eob_year").parquet(f"{SILVER}explanations/")
print("✓ EOBs done")

# ── COVERAGE ──────────────────────────────────────────────────────────────────
# Note: beneficiary.reference uses urn:uuid: format in Synthea contained resources
res_df.filter(F.col("resourceType") == "Coverage").select(
    F.get_json_object("resource_json", "$.id").alias("coverage_id"),
    F.get_json_object("resource_json", "$.status").alias("status"),
    F.regexp_extract(
        F.get_json_object("resource_json", "$.beneficiary.reference"),
        r"(?:Patient/|urn:uuid:)(.+)", 1).alias("patient_id"),
    F.get_json_object("resource_json", "$.payor[0].display").alias("payer_name"),
    F.get_json_object("resource_json", "$.period.start").alias("coverage_start"),
    F.get_json_object("resource_json", "$.period.end").alias("coverage_end"),
    F.get_json_object("resource_json", "$.type.text").alias("coverage_type")
).write.mode("overwrite").parquet(f"{SILVER}coverages/")
print("✓ Coverages done")

# ── SERVICE REQUEST (Prior Authorization) ─────────────────────────────────────
# THIS IS THE CORE OF PACIP — ServiceRequest is the FHIR prior auth resource
# Synthea embeds these in ExplanationOfBenefit's contained array
res_df.filter(F.col("resourceType") == "ServiceRequest").select(
    F.get_json_object("resource_json", "$.id").alias("service_request_id"),
    F.get_json_object("resource_json", "$.status").alias("status"),
    F.get_json_object("resource_json", "$.intent").alias("intent"),
    F.regexp_extract(
        F.get_json_object("resource_json", "$.subject.reference"),
        r"(?:Patient/|urn:uuid:)(.+)", 1).alias("patient_id"),
    F.get_json_object("resource_json", "$.code.coding[0].code")
        .alias("procedure_code"),
    F.get_json_object("resource_json", "$.code.text")
        .alias("procedure_description"),
    F.get_json_object("resource_json", "$.authoredOn").alias("authored_on"),
    F.year(F.to_timestamp(
        F.get_json_object("resource_json", "$.authoredOn"))).alias("auth_year"),
    F.get_json_object("resource_json", "$.reasonCode[0].coding[0].code")
        .alias("reason_code"),
    F.get_json_object("resource_json", "$.reasonCode[0].text")
        .alias("reason_description"),
    F.get_json_object("resource_json", "$.requester.reference")
        .alias("requester_ref")
).write.mode("overwrite").partitionBy("auth_year").parquet(f"{SILVER}prior_auth/")
print("✓ Prior Auth (ServiceRequest) done")

# ── ENCOUNTER ─────────────────────────────────────────────────────────────────
res_df.filter(F.col("resourceType") == "Encounter").select(
    F.get_json_object("resource_json", "$.id").alias("encounter_id"),
    F.get_json_object("resource_json", "$.status").alias("status"),
    F.year(F.to_timestamp(
        F.get_json_object("resource_json", "$.period.start"))).alias("encounter_year"),
    F.regexp_extract(
        F.get_json_object("resource_json", "$.subject.reference"),
        r"(?:Patient/|urn:uuid:)(.+)", 1).alias("patient_id"),
    F.get_json_object("resource_json", "$.class.code").alias("encounter_class"),
    F.get_json_object("resource_json", "$.type[0].text").alias("encounter_type"),
    F.get_json_object("resource_json", "$.period.start").alias("encounter_start"),
    F.get_json_object("resource_json", "$.period.end").alias("encounter_end"),
    F.get_json_object("resource_json", "$.serviceProvider.display")
        .alias("provider_name")
).write.mode("overwrite").partitionBy("encounter_year").parquet(f"{SILVER}encounters/")
print("✓ Encounters done")

# ── CONDITION ─────────────────────────────────────────────────────────────────
res_df.filter(F.col("resourceType") == "Condition").select(
    F.get_json_object("resource_json", "$.id").alias("condition_id"),
    F.regexp_extract(
        F.get_json_object("resource_json", "$.subject.reference"),
        r"(?:Patient/|urn:uuid:)(.+)", 1).alias("patient_id"),
    F.get_json_object("resource_json", "$.code.coding[0].code").alias("icd10_code"),
    F.get_json_object("resource_json", "$.code.text").alias("condition_description"),
    F.get_json_object("resource_json", "$.onsetDateTime").alias("onset_date"),
    F.get_json_object("resource_json", "$.abatementDateTime").alias("abatement_date"),
    F.get_json_object("resource_json",
        "$.clinicalStatus.coding[0].code").alias("clinical_status")
).write.mode("overwrite").parquet(f"{SILVER}conditions/")
print("✓ Conditions done")

job.commit()
print("✓ FHIR parser job complete")
