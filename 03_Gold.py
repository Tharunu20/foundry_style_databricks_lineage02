# Databricks notebook source
import uuid
import subprocess
from pyspark.sql.functions import current_timestamp, sum as _sum, lit

# ======================================
# 1. Capture Real Git Information
# ======================================
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

# ======================================
# 2. Capture Job Context
# ======================================
try:
    context = dbutils.notebook.entry_point.getDbutils().notebook().getContext()
    run_id = context.tags().get("runId").get()
    job_id = context.tags().get("jobId").get()
except:
    run_id = str(uuid.uuid4())
    job_id = "manual"

job_name = "gold_aggregation"
env = "dev"

# ======================================
# 3. Log Pipeline Start
# ======================================
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

# ======================================
# 4. Capture Input Version (Silver)
# ======================================
input_version = spark.sql("DESCRIBE HISTORY silver_orders") \
    .select("version").first()[0]

# ======================================
# 5. Transform (Aggregate Revenue)
# ======================================
df = spark.read.table("silver_orders")

gold_df = df.groupBy() \
    .agg(_sum("amount").alias("total_revenue")) \
    .withColumn("run_id", lit(run_id)) \
    .withColumn("git_commit", lit(git_commit)) \
    .withColumn("processed_at", current_timestamp())

# ======================================
# 6. Write Gold Table
# ======================================
gold_df.write.format("delta") \
    .mode("overwrite") \
    .saveAsTable("gold_revenue")

# ======================================
# 7. Capture Output Version
# ======================================
output_version = spark.sql("DESCRIBE HISTORY gold_revenue") \
    .select("version").first()[0]

spark.sql(f"""
INSERT INTO dataset_versions VALUES (
  'gold_revenue',
  '{output_version}',
  '{run_id}',
  current_timestamp()
)
""")

# ======================================
# 8. Record Lineage (Silver → Gold)
# ======================================
spark.sql(f"""
INSERT INTO lineage_edges VALUES (
  'gold_revenue',
  'silver_orders',
  '{input_version}',
  '{run_id}'
)
""")

# ======================================
# 9. Mark Pipeline Success
# ======================================
spark.sql(f"""
UPDATE pipeline_runs
SET status = 'SUCCESS'
WHERE run_id = '{run_id}'
""")