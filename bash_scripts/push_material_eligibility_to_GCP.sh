#!/bin/bash
trap 'exit' ERR
kinit -kt ~/a-phelasa.keytab a-phelasa@GRAYBAR.COM
spark-submit --master yarn --num-executors 12 --executor-memory 5g --driver-memory 6g --conf spark.executor.memoryOverhead=1g --conf spark.dynamicAllocation.enabled=false /home/data_analytics/data_analytics/cost_benchmark/pyspark_etl/material_eligibility_etl.py
hadoop distcp hdfs://splhdpmp01/user/a-phelasa/material_eligibility_for_gcp.parquet gs://data-discovery/projects/cost_benchmark/phase_3/eligibility_files/
gsutil rm -r gs://data-discovery/projects/cost_benchmark/phase_3/eligibility_files/material_eligibility_table
gsutil mv gs://data-discovery/projects/cost_benchmark/phase_3/eligibility_files/material_eligibility_for_gcp.parquet gs://data-discovery/projects/cost_benchmark/phase_3/eligibility_files/material_eligibility_table
gsutil rm -r gs://data-discovery/projects/cost_benchmark/phase_3/eligibility_files/material_eligibility_for_gcp.parquet
