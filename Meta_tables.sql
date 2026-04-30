-- Databricks notebook source
CREATE TABLE IF NOT EXISTS pipeline_runs (
  run_id STRING,
  job_id STRING,
  job_name STRING,
  git_commit STRING,
  git_branch STRING,
  start_time TIMESTAMP,
  env STRING,
  status STRING
);

CREATE TABLE IF NOT EXISTS dataset_versions (
  table_name STRING,
  version STRING,
  run_id STRING,
  created_at TIMESTAMP
);

CREATE TABLE IF NOT EXISTS lineage_edges (
  output_table STRING,
  input_table STRING,
  input_version STRING,
  run_id STRING
);