import sys
from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql import functions as F

location = 'gs://data-discovery/projects/cost_benchmark/phase_3/'

# Customer Input Type: can be either 1, 2, or 3, corresponding to upload list of customers, upload list of hierarchies, and customer attribute selection, respectively
customer_option = sys.argv[1]

# List of customer attributes for filtering eligibility files as an alternative to uploading a customer file.
customer_attributes = sys.argv[2].split('|')

# Type of customer eligibilities to include: must be a string of 4 digits, each is either 1 or 0, representing whether or not to include that type of eligibility
eligibility_options = sys.argv[3]

# Which reports to include: must be a string of 4 digits, each is either 1 or 0, representing whether or not to include that report
report_options = sys.argv[4]

# Name of the customer or hierarchy file that is uploaded to Google Cloud Storage folder (gs://data-discovery/projects/cost_benchamrk/phase_3/input_files/)
customer_file = sys.argv[5]

# Name of the material file that is uploaded to Google Cloud Storage folder (gs://data-discovery/projects/cost_benchmark/phase_3/input_files/)
material_file = sys.argv[6]

# Name of the unique Project ID
project_id = sys.argv[7]

print(sys.argv)

# Declare a SparkSession
conf = SparkConf()
spark = SparkSession.builder.enableHiveSupport().getOrCreate()

def create_customer_attributes_dict(customer_attributes_list):
	output = {'vertical_market': [],'customer_market': [],'customer_submarket': [],'cot': [],'district': [],'zone': [],'sales_office': [] }
	print(customer_attributes_list)
	for item in customer_attributes_list:
		item = item.split(':')
	
		output[item[0]].append(item[1])
	return output

def filter_by_attributes(eligibility_list, customer_attributes_dict):
	print(customer_attributes_dict)
	
	output_df = (eligibility_list
		.filter(eligibility_list['sales_office_vkbur'].isin(customer_attributes_dict['sales_office']))
		.filter(eligibility_list['vertical_mkt_desc'].isin(customer_attributes_dict['vertical_market']))
		.filter(eligibility_list['customer_market_description'].isin(customer_attributes_dict['customer_market']))
		.filter(eligibility_list['customer_submarket_description'].isin(customer_attributes_dict['customer_submarket']))
		.filter(eligibility_list['cot_desc'].isin(customer_attributes_dict['cot']))
		.filter(eligibility_list['district_name_bztxt'].isin(customer_attributes_dict['district']))
		.filter(eligibility_list['zone_zzone'].isin(customer_attributes_dict['zone']))
		.drop('vertical_mkt_desc')
		.drop('customer_market_description')
		.drop('customer_submarket_description')
		.drop('cot_desc')
		.drop('district_name_bztxt')
		)
	return output_df

def filter_by_customer_eligibility(customer_eligibility):
	possible_options = ['Customer','Hierarchy','COT/District','COT/Sales Office']
	selected_options = []
	for x in range(4):
		if eligibility_options[x] == '1':
			selected_options.append(possible_options[x])
	return customer_eligibility.filter(customer_eligibility.customer_eligibility_type.isin(selected_options))

def generate_material_summary(long_report, material_list):
	"""
	Aggregates the raw SPA report to a material level, finds min, avg, and max SPA costs, adds into-stock costs from MBEW, and descriptive columns from descriptions table
	"""
	selected_locations = long_report.groupby('delivering_plant_vwerk').count().drop('count')

	mbew = spark.read.parquet(location + 'extracts/stock_cost_table')
	mbew_filtered = mbew.join(selected_locations, mbew.plant_werks == selected_locations.delivering_plant_vwerk, 'inner')

	descriptions = spark.read.parquet(location + 'extracts/material_description_table')
	filtered_descriptions = material_list.join(descriptions, "material_matnr", 'left_outer')

	#Standardize the 'pricing_unit_kpein' (UOM) field so that there is only one UOM for each material, and then find min, avg, max SPA costs
	#This UOM standardization process can possibly be moved to material_eligibility_etl.py to reduce job run time.
	temp_table = long_report.groupby('material_matnr','pricing_unit_kpein').count()
	temp_table2 = temp_table.groupby('material_matnr').max('count')
	temp_table3 = temp_table.join(temp_table2, 'material_matnr')
	common_per_table = temp_table3.filter(temp_table3['count'] == temp_table3['max(count)']).groupby('material_matnr').max('pricing_unit_kpein').select(F.col('max(pricing_unit_kpein)').alias('common_per_SPA'), 'material_matnr')
	# Add the new standardized common per back into the long report
	long_report_common_per = long_report.join(common_per_table, 'material_matnr')
	long_report_common_per = long_report_common_per.withColumn('net_rate', long_report_common_per['net_rate'] / long_report_common_per['pricing_unit_kpein'] * long_report_common_per['common_per_SPA'])
	# Find min, average
	agg_report = long_report_common_per.groupby('material_matnr','common_per_SPA').agg(F.countDistinct('sales_deal_knumaag'), F.min('net_rate'), F.avg('net_rate'), F.max('net_rate'), F.countDistinct('net_rate'))
	agg_report_and_desc = filtered_descriptions.join(agg_report, 'material_matnr', 'left_outer')
	stock_costs = material_list.select('material_matnr').join(mbew_filtered, 'material_matnr').groupby('material_matnr', 'common_per_stock').agg(F.min('standard_price_stprs'), F.avg('standard_price_stprs'), F.max('standard_price_stprs'))
	material_summary_report = agg_report_and_desc.join(stock_costs, 'material_matnr', 'left_outer')

	column_order = ['material_matnr',
		'supplier_name',
		'supplier_num',
		'supplier_status',
		'product_hierarchy',
		'vendor_hierarchy_1',
		'vendor_hierarchy_2',
		'vendor_hierarchy_3',
		'mfrpn',
		'mfrpn_abbreviated',
		'material_desc',
		'material_grp_desc',
		'material_grp_num',
		'material_type',
		'natl_dno_code',
		't3_cost',
		'l_cost',
		'a_cost',
		'b_cost',
		'sd_cost',
		'natl_net_cost',
		'slab_uom',
		'min(standard_price_stprs)',
		'avg(standard_price_stprs)',
		'max(standard_price_stprs)',
		'common_per_stock',
		'top_item',
		'upc',
		'count(DISTINCT sales_deal_knumaag)',
		'count(DISTINCT net_rate)',
		'min(net_rate)',
		'avg(net_rate)',
		'max(net_rate)',
		'common_per_SPA']
	material_summary_report = material_summary_report.select(*column_order)

	return material_summary_report

def generate_customer_report(long_report):
	"""
	Aggregates the raw SPA report to a customer level and adds some addition customer data
	"""
	agg_report = long_report.groupby('customer_kunnr','sales_office_vkbur','customer_classific_kukla','customer_market_zzmarket','customer_submarket_zzsubmarket','delivering_plant_vwerk','name_name1','sales_district_bzirk','vertical_mkt_attrib_zzbran6').count().drop('count')
	values_location = location + 'extracts/'
	values_schema = StructType([
		StructField('name',StringType(),True),
		StructField('value',StringType(),True)])
	cot_values = spark.read.schema(values_schema).csv(values_location + 'cot_values.csv')
	vertical_mkt_values = spark.read.schema(values_schema).csv(values_location + 'vertical_mkt_values.csv')
	customer_mkt_values = spark.read.schema(values_schema).csv(values_location + 'customer_mkt_values.csv')
	customer_submkt_values = spark.read.schema(values_schema).csv(values_location + 'customer_submkt_values.csv')
	district_values = spark.read.schema(values_schema).csv(values_location + 'district_values.csv')
	agg_report = agg_report.join(cot_values.withColumnRenamed('name','class_of_trade'), agg_report.customer_classific_kukla == cot_values.value, 'left_outer').drop('value')
	agg_report = agg_report.join(vertical_mkt_values.withColumnRenamed('name','vertical_market'), agg_report.vertical_mkt_attrib_zzbran6 == vertical_mkt_values.value, 'left_outer').drop('value')
	agg_report = agg_report.join(customer_mkt_values.withColumnRenamed('name','customer_market'), agg_report.customer_market_zzmarket == customer_mkt_values.value, 'left_outer').drop('value')
	agg_report = agg_report.join(customer_submkt_values.withColumnRenamed('name','customer_submarket'), agg_report.customer_submarket_zzsubmarket == customer_submkt_values.value, 'left_outer').drop('value')
	agg_report = agg_report.join(district_values.withColumnRenamed('name','district'), agg_report.sales_district_bzirk == district_values.value, 'left_outer').drop('value')
	customer_report = agg_report.drop('sales_district_bzirk','customer_submarket_zzsubmarket','customer_classific_kukla','vertical_mkt_attrib_zzbran6','customer_market_zzmarket')
	return customer_report

def generate_material_agreement_report(long_report):
	"""
	Aggregates the raw SPA report to an aggreement level (omits the customer field)
	"""
	return long_report.groupby('condition_type_desc',
		'condition_type_kschl',
		'eligibility_type',
		'description_botext',
		'external_description_abrex',
		'material_matnr',
		'valid_from_datab',
		'valid_to_datbi',
		'percentage',
		'price',
		'price_list_pltypd',
		'pricing_unit_kpein',
		'sales_deal_knumaag',
		'net_rate').count().drop('count')

def generate_long_report(long_report):
	"""
	Renames the fields of the raw long report so that it is ready for consumptioni
	"""
	long_report = long_report.selectExpr('condition_type_desc as condition_type_description',
		'condition_type_kschl as condition_type',
		'customer_kunnr as customer_number',
		'eligibility_type',
		'material_matnr as material_number',
		'valid_from_datab',
		'valid_to_datbi',
		'net_rate as spa_rate',
		'percentage as percent_discount',
		'price',
		'price_list_pltypd as price_list',
		'pricing_unit_kpein as uom',
		'sales_deal_knumaag as agreement_number',
		'external_description_abrex as external_description',
		'description_botext as description')
	return long_report

def generate_customers_from_hierarchies(hierarchy_list):
	"""
	Accepts a list of hierarchy numbers and returns a list of customer numbers
	"""
	hierarchy_list = hierarchy_list.withColumnRenamed('_c0','input_lvl_1')
	hierarchy_list = hierarchy_list.withColumn('input_lvl_1', F.lpad(hierarchy_list['input_lvl_1'],10,'0'))

	pricing_hierarchy = spark.read.parquet(location + 'extracts/pricing_hierarchy_table')

	hierarchy_list = (hierarchy_list.join(pricing_hierarchy, hierarchy_list.input_lvl_1 == pricing_hierarchy.higherlevel_customer_hkunnr, 'left_outer')
		.drop('higherlevel_customer_hkunnr'))
	hierarchy_list = hierarchy_list.withColumnRenamed('customer_kunnr','input_lvl_2')

	hierarchy_list = (hierarchy_list.join(pricing_hierarchy, hierarchy_list.input_lvl_2 == pricing_hierarchy.higherlevel_customer_hkunnr, 'left_outer')
		.drop('higherlevel_customer_hkunnr'))
	hierarchy_list = hierarchy_list.withColumnRenamed('customer_kunnr','input_lvl_3')

	hierarchy_list = (hierarchy_list.join(pricing_hierarchy, hierarchy_list.input_lvl_3 == pricing_hierarchy.higherlevel_customer_hkunnr, 'left_outer')
		.drop('higherlevel_customer_hkunnr'))
	hierarchy_list = hierarchy_list.withColumnRenamed('customer_kunnr','input_lvl_4')

	hierarchy_list = (hierarchy_list.join(pricing_hierarchy, hierarchy_list.input_lvl_4 == pricing_hierarchy.higherlevel_customer_hkunnr, 'left_outer')
		.drop('higherlevel_customer_hkunnr'))
	hierarchy_list = hierarchy_list.withColumnRenamed('customer_kunnr','input_lvl_5')



	def pick_highest_hierarchy(input_1,input_2,input_3,input_4,input_5):
		if(input_5):
			return input_5
		elif(input_4):
			return input_4
		elif(input_3):
			return input_3
		elif(input_2):
			return input_2
		else:
			return input_1
	hierarchy_selector_UDF = F.udf(pick_highest_hierarchy, StringType())
	hierarchy_list = hierarchy_list.withColumn('customer_kunnr',hierarchy_selector_UDF(hierarchy_list.input_lvl_1, hierarchy_list.input_lvl_2, hierarchy_list.input_lvl_3, hierarchy_list.input_lvl_4, hierarchy_list.input_lvl_5))
	
	return hierarchy_list.selectExpr('customer_kunnr as _c0')

# Create a dataframe to hold the list of customers
if(customer_option == '1'):
	customer_input = spark.read.csv("gs://data-discovery/" + customer_file)
	
elif(customer_option == '2'):
	customer_input = generate_customers_from_hierarchies(spark.read.csv("gs://data-discovery/" + customer_file))
	

# Combine all of the eligibility files into one dataframe
customer_eligibility = filter_by_customer_eligibility(spark.read.parquet(location + 'eligibility_files/customer_eligibility_table'))


# Filter the eligibility dataframe to the relevant customers
if(customer_option == '1' or customer_option == '2'):
	padded_customer_input = customer_input.withColumn('customer_kunnr', F.lpad(customer_input['_c0'],10,'0')).drop('_c0')

	
	customer_eligibility = (customer_eligibility
		.drop('vertical_mkt_desc')
		.drop('customer_market_description')
		.drop('customer_submarket_description')
		.drop('cot_desc')
		.drop('district_name_bztxt')
		)

	filtered_eligibility = customer_eligibility.join(F.broadcast(padded_customer_input), 'customer_kunnr')

else:
	customer_attributes_dict = create_customer_attributes_dict(customer_attributes)
	filtered_eligibility = filter_by_attributes(customer_eligibility, customer_attributes_dict)

# Create a dataframe to hold the list of materials
material_input = spark.read.schema(StructType([
		StructField('material_matnr',StringType(),True)])).csv("gs://data-discovery/" + material_file)

# Add primary keys to material list
material_input_with_id = material_input.withColumn("id", F.monotonically_increasing_id())

#Create a dataframe to hold the material eligility table
material_eligibility = spark.read.parquet(location + 'eligibility_files/material_eligibility_table')

#Add leading zeros to material input file so it can join with material_eligibility table
material_input = material_input.withColumn("material_matnr", F.lpad(material_input["material_matnr"],18,"0"))

#Filter elgibility files to the relevant materials
filtered_agreements = material_eligibility.join(F.broadcast(material_input), "material_matnr")

long_report = filtered_agreements.join(filtered_eligibility, "sales_deal_knumaag")


# Based on user input, call the appropriate methods to generate each type of report
if report_options[0] == '1':
	material_summary = generate_material_summary(long_report, material_input)

	material_summary.coalesce(32).write.csv(location + 'output_files/' + project_id + '_material_summary', mode="overwrite", compression="none")

if report_options[1] == '1':
	customer_report = generate_customer_report(long_report)
	
	customer_report.coalesce(32).write.csv(location + 'output_files/' + project_id + '_customer_report', mode="overwrite", compression="none")

if report_options[2] == '1':
	material_agreement_report = generate_material_agreement_report(long_report)
	
	material_agreement_report.coalesce(32).write.csv(location + 'output_files/' + project_id + '_material_agreement_report_condensed', mode="overwrite", compression="none")

if report_options[3] == '1':
	long_report_final = generate_long_report(long_report)
	
	long_report_final.coalesce(32).write.csv(location + 'output_files/' + project_id + '_material_agreement_report_long', mode="overwrite", compression="none")

