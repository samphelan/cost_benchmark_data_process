#!/bin/bash
trap 'exit' ERR
kinit -kt ~/a-phelasa.keytab a-phelasa@GRAYBAR.COM
spark-submit --conf spark.sql.autoBroadcastJoinThreshold=-1 /home/data_analytics/data_analytics/cost_benchmark/pyspark_etl/create_customer_eligibility.py
hadoop distcp hdfs://splhdpmp01/user/a-phelasa/customer_eligibility.parquet gs://data-discovery/projects/cost_benchmark/phase_3/eligibility_files/
gsutil rm -r gs://data-discovery/projects/cost_benchmark/phase_3/eligibility_files/customer_eligibility_table
gsutil mv gs://data-discovery/projects/cost_benchmark/phase_3/eligibility_files/customer_eligibility.parquet gs://data-discovery/projects/cost_benchmark/phase_3/eligibility_files/customer_eligibility_table
gsutil rm -r gs://data-discovery/projects/cost_benchmark/phase_3/eligibility_files/customer_eligibility.parquet

