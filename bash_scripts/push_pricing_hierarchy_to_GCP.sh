#!/bin/bash
trap 'exit' ERR
kinit -kt ~/a-phelasa.keytab a-phelasa@GRAYBAR.COM
spark-submit --master yarn --num-executors 6 --executor-memory 5GB --conf spark.executor.memoryOverhead=1GB --conf spark.dynamicAllocation.enabled=false /home/data_analytics/data_analytics/cost_benchmark/pyspark_etl/create_pricing_hierarchy.py
hadoop distcp hdfs://splhdpmp01/user/a-phelasa/pricing_hierarchy_for_gcp.parquet gs://data-discovery/projects/cost_benchmark/phase_3/extracts/
gsutil rm -r gs://data-discovery/projects/cost_benchmark/phase_3/extracts/pricing_hierarchy_table
gsutil mv gs://data-discovery/projects/cost_benchmark/phase_3/extracts/pricing_hierarchy_for_gcp.parquet gs://data-discovery/projects/cost_benchmark/phase_3/extracts/pricing_hierarchy_table
gsutil rm -r gs://data-discovery/projects/cost_benchmark/phase_3/extracts/pricing_hierarchy_for_gcp.parquet
