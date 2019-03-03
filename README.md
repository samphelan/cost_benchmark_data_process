# Cost Benchmark Data Process

This application accepts a list of customers and materials and retrieves information regarding all of the eligible SPAs (Special Pricing Agreements) with the intention of expediting the quotes process. The main script - `cost_benchmark_gcp.py` - is initiated by an API call to Google Cloud Dataproc whenever a user clicks "Submit" on the web application.

## Main Script

The main script essentially reads two large tables, the customer eligibility table, created by `./pyspark_etl/create_customer_eligibility.py`, and the material eligibility table, created by `./pyspark_etl/material_eligibility_etl.py`, and filters them both by the lists of customers and materials, respectively, and then performs a join on the filtered tables to create a table of all eligible agreements for those customers and materials.

The reason the main join is not performed in the ETL process, which would allow the tool to essentially only perform aggregation queries and thus run more quickly, is because it would produce a table of 30 billion rows, which is too expensive, both computationally and financially, to justify performing every day.

After the table is created, aggregations are performed and supplemental data is brought in from other tables to produce various reports.

## ETL Scripts

The scripts in `/pyspark_etl` create all of the tables necessary for this project. They essentially recreate a process that used to be done manually by quotes and pricing specialists. They are called by the scripts in `./bash_scripts/`.

## Bash Scripts

The scripts in `/bash_scripts` call the scripts in `pyspark_etl` and then push them to GCP in such a way that if any part of the process fails, there will still be a backup table from the last push.

## HQL Scripts

There is just one HQL script which takes the columns that I need from VBRP and puts them into a Managed Table in Hive (quotes_project.vbrp_top_item) so that I can access them from my Spark scripts. (VBRP is an Acid Table in Hive because it is created using the Delta process. Acid Tables cannot be accessed via Spark SQL.)

## Dependency Tables

All of the following tables must be present for the tool to work. These are created by the scripts in `./pyspark_etl/` which must be run regularly by the cronjobs listed at the bottom of this README.

1. `gs://data-discovery/projects/cost_benchmark/phase_3/eligibility_files/material_eligibility_table`
2. `gs://data-discovery/projects/cost_benchmark/phase_3/eligibility_files/customer_eligibility_table`
3. `gs://data-discovery/projects/cost_benchmark/phase_3/extracts/stock_cost_table`
4. `gs://data-discovery/projects/cost_benchmark/phase_3/extracts/material_description_table`
5. `gs://data-discovery/projects/cost_benchmark/phase_3/extracts/pricing_hierarchy_table`


## Cronjobs

All of the following cronjobs should be run on the edge node of our on-prem Hadoop cluster in order for this project to work.

```
0 7 * * 1-5 gcloud beta dataproc clusters create cost-benchmark --bucket=data-discovery --region=global --zone=us-central1-b --num-preemptible-workers=50 --max-age=11h
20 2 * * 1-5 /home/data_analytics/data_analytics/cost_benchmark/bash_scripts/push_material_eligibility_to_GCP.sh
20 3 * * 1-5 /home/data_analytics/data_analytics/cost_benchmark/bash_scripts/push_customer_eligibility_to_GCP.sh
15 5 * * 1-5 /home/data_analytics/data_analytics/cost_benchmark/bash_scripts/push_mbew_to_GCP.sh
45 5 * * 1-5 /home/data_analytics/data_analytics/cost_benchmark/bash_scripts/push_material_descriptions_to_GCP.sh
15 6 * * 1-5 /home/data_analytics/data_analytics/cost_benchmark/bash_scripts/push_pricing_hierarchy_to_GCP.sh
56 9 * * 1-5 hive -f /home/data_analytics/data_analytics/cost_benchmark/hql_scripts/create_top_item_table.hql
```