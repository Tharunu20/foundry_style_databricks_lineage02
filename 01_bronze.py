# Databricks notebook source
import uuid
import subprocess
from pyspark.sql.functions import current_timestamp

# =========================
# 1. Capture Real Git Info
# =========================
try:
    git_commit = subprocess.check_output(
        ["git", "rev-parse", "HEAD"]
    ).decode().strip()

    git_branch = subprocess.check_output(
        ["git", "rev-parse", "--abbrev-ref", "HEAD"]
    ).decode().strip()
except:
    git_commit = "unknown"
    git_branch = "unknown"

# =========================
# 2. Capture Job Context
# =========================
try:
    context = dbutils.notebook.entry_point.getDbutils().notebook().getContext()
    run_id = context.tags().get("runId").get()
    job_id = context.tags().get("jobId").get()
except:
    run_id = str(uuid.uuid4())
    job_id = "manual"

job_name = "bronze_ingest"
env = "dev"

# =========================
# 3. Log Pipeline Start
# =========================
spark.sql(f"""
INSERT INTO pipeline_runs VALUES (
  '{run_id}',
  '{job_id}',
  '{job_name}',
  '{git_commit}',
  '{git_branch}',
  current_timestamp(),
  '{env}',
  'STARTED'
)
""")

# =========================
# 4. Create Sample Data
# =========================
data = [
    (1, "completed", 1000),
    (2, "pending", 500),
    (3, "completed", 2000)
]

df = spark.createDataFrame(data, ["order_id", "status", "amount"])
df = df.withColumn("ingested_at", current_timestamp())

df.write.format("delta").mode("overwrite").saveAsTable("bronze_orders")

# Capture Delta Version
bronze_version = spark.sql("DESCRIBE HISTORY bronze_orders") \
    .select("version").first()[0]

spark.sql(f"""
INSERT INTO dataset_versions VALUES (
  'bronze_orders',
  '{bronze_version}',
  '{run_id}',
  current_timestamp()
)
""")

spark.sql(f"""
UPDATE pipeline_runs
SET status = 'SUCCESS'
WHERE run_id = '{run_id}'
""")