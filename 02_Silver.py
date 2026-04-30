# Databricks notebook source
import uuid
import subprocess
from pyspark.sql.functions import lit, current_timestamp

# Git Info
git_commit = subprocess.check_output(
    ["git", "rev-parse", "HEAD"]
).decode().strip()

git_branch = subprocess.check_output(
    ["git", "rev-parse", "--abbrev-ref", "HEAD"]
).decode().strip()

# Job Context
context = dbutils.notebook.entry_point.getDbutils().notebook().getContext()
run_id = context.tags().get("runId").get()
job_id = context.tags().get("jobId").get()

job_name = "silver_transform"
env = "dev"

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

# Capture Input Version
input_version = spark.sql("DESCRIBE HISTORY bronze_orders") \
    .select("version").first()[0]

df = spark.read.table("bronze_orders") \
    .filter("status = 'completed'")

df_out = df \
    .withColumn("run_id", lit(run_id)) \
    .withColumn("git_commit", lit(git_commit)) \
    .withColumn("processed_at", current_timestamp())

df_out.write.format("delta").mode("append").saveAsTable("silver_orders")

output_version = spark.sql("DESCRIBE HISTORY silver_orders") \
    .select("version").first()[0]

spark.sql(f"""
INSERT INTO dataset_versions VALUES (
  'silver_orders',
  '{output_version}',
  '{run_id}',
  current_timestamp()
)
""")

spark.sql(f"""
INSERT INTO lineage_edges VALUES (
  'silver_orders',
  'bronze_orders',
  '{input_version}',
  '{run_id}'
)
""")

spark.sql(f"""
UPDATE pipeline_runs
SET status = 'SUCCESS'
WHERE run_id = '{run_id}'
""")