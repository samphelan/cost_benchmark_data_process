from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql import functions as F
import datetime

conf = SparkConf()
spark = SparkSession.builder.enableHiveSupport().getOrCreate()

# Get agreements from A-Tables
a524 = spark.sql("SELECT *,'A524' AS source FROM saperp.a524")
a524 = a524.drop('client_mandt').drop('application_kappl')

a525 = spark.sql("SELECT *,'A525' AS source FROM saperp.a525")
a525 = a525.drop('client_mandt').drop('aaplication_kappl')

a526 = spark.sql("SELECT *,'A526' AS source FROM saperp.a526")
a526 = a526.drop('client_mandt').drop('aaplication_kappl')

a527 = spark.sql("SELECT *,'A527' AS source FROM saperp.a527")
a527 = a527.drop('client_mandt').drop('aaplication_kappl')

a528 = spark.sql("SELECT *,'A528' AS source FROM saperp.a528")
a528 = a528.drop('client_mandt').drop('aaplication_kappl').drop('vendor_hierarchy_3_zzvendh3')

a529 = spark.sql("SELECT *,'A529' AS source FROM saperp.a529")
a529 = a529.drop('client_mandt').drop('aaplication_kappl')

a530 = spark.sql("SELECT *,'A530' AS source FROM saperp.a530")
a530 = a530.drop('client_mandt').drop('aaplication_kappl')

a586 = spark.sql("SELECT *,'A586' AS source FROM saperp.a586")
a586 = a586.drop('client_mandt').drop('aaplication_kappl').drop('mnitm_prcrefmatl_upmat')

a587 = spark.sql("SELECT *,'A587' AS source FROM saperp.a587")
a587 = a587.drop('client_mandt').drop('aaplication_kappl')

# Filter to active agreements, remove % off cost records ('Z012'), and only include U.S. records ('1000')
today = datetime.datetime.today().strftime('%Y%m%d')
a524 = (a524.filter(a524.shipment_category_zzshiptype == 'S')
	.filter(a524.sales_organization_vkorg == '1000')
	.filter(a524.valid_to_datbi >= today)
	.filter(a524.valid_from_datab <= today)
	.drop('shipment_category_zzshiptype')
	.drop('sales_organization_vkorg'))

a525 = (a525.filter(a525.condition_type_kschl != 'Z012')
	.filter(a525.sales_organization_vkorg == '1000')
	.filter(a525.valid_to_datbi >= today)
	.filter(a525.valid_from_datab <= today)
	.drop('sales_organization_vkorg'))

a526 = (a526.filter(a526.condition_type_kschl != 'Z012')
	.filter(a526.sales_organization_vkorg == '1000')
	.filter(a526.valid_to_datbi >= today)
	.filter(a526.valid_from_datab <= today)
	.drop('sales_organization_vkorg'))

a527 = (a527.filter(a527.condition_type_kschl != 'Z012')
	.filter(a527.sales_organization_vkorg == '1000')
	.filter(a527.valid_to_datbi >= today)
	.filter(a527.valid_from_datab <= today)
	.drop('sales_organization_vkorg'))

a528 = (a528.filter(a528.shipment_category_zzshiptype == 'S')
	.filter(a528.sales_organization_vkorg == '1000')
	.filter(a528.valid_to_datbi >= today)
	.filter(a528.valid_from_datab <= today)
	.drop('shipment_category_zzshiptype')
	.drop('sales_organization_vkorg'))

a529 = (a529.filter(a529.shipment_category_zzshiptype == 'S')
	.filter(a529.sales_organization_vkorg == '1000')
	.filter(a529.valid_to_datbi >= today)
	.filter(a529.valid_from_datab <= today)
	.drop('shipment_category_zzshiptype')
	.drop('sales_organization_vkorg'))

a530 = (a530.filter(a530.shipment_category_zzshiptype == 'S')
	.filter(a530.sales_organization_vkorg == '1000')
	.filter(a530.valid_to_datbi >= today)
	.filter(a530.valid_from_datab <= today)
	.drop('shipment_category_zzshiptype')
	.drop('sales_organization_vkorg'))

a586 = (a586.filter(a586.sales_organization_vkorg == '1000')
	.filter(a586.valid_to_datbi >= today)
	.filter(a586.valid_from_datab <= today)
	.drop('sales_organization_vkorg'))

a587 = (a587.filter(a587.condition_type_kschl != 'Z012')
	.filter(a587.sales_organization_vkorg == '1000')
	.filter(a587.valid_to_datbi >= today)
	.filter(a587.valid_from_datab <= today)
	.drop('sales_organization_vkorg'))

a515 = spark.sql('SELECT sales_deal_knumaag, manufacturer_mfrnr FROM saperp.a515')

# Bring in manufacturer.mfrnr from a515
a528 = a528.join(a515, 'sales_deal_knumaag')
a529 = a529.join(a515, 'sales_deal_knumaag')
a530 = a530.join(a515, 'sales_deal_knumaag')

# Create mara dataframe and split product_hierarchy_prdha into three columns
mara_columns = ['manufacturer_mfrnr', 
	'material_matnr', 
	'product_hierarchy_prdha', 
	'vendor_hierarchy_1_zzvendh1', 
	'vendor_hierarchy_2_zzvendh2']
mara = spark.sql('SELECT ' + ','.join(mara_columns) + ' FROM saperp.mara')
mara = (mara
	.withColumn('main_group_prodh1', F.substring(mara.product_hierarchy_prdha, 1, 5))
	.withColumn('group_prodh2', F.substring(mara.product_hierarchy_prdha, 6, 2))
	.withColumn('subgroup_prodh3', F.substring(mara.product_hierarchy_prdha, 11, 2))
	.drop('product_hierarchy_prdha'))

# Add material data to agreement tables
a524 = mara.join(a524, 'material_matnr').withColumn('eligibility_type',F.lit('Material'))
a525 = mara.join(a525, ['main_group_prodh1','group_prodh2','subgroup_prodh3']).withColumn('eligibility_type',F.lit('Product Hierarchy'))
a526 = mara.join(a526, ['main_group_prodh1','group_prodh2']).withColumn('eligibility_type',F.lit('Product Hierarchy Group 1/2'))
a527 = mara.join(a527, 'main_group_prodh1').withColumn('eligibility_type',F.lit('Product Hierarchy Group 1'))
a528 = mara.join(a528, ['vendor_hierarchy_1_zzvendh1','vendor_hierarchy_2_zzvendh2','manufacturer_mfrnr']).withColumn('eligibility_type',F.lit('Vendor Hierarchy 1/2'))
a529 = mara.join(a529, ['vendor_hierarchy_1_zzvendh1','vendor_hierarchy_2_zzvendh2','manufacturer_mfrnr']).withColumn('eligibility_type',F.lit('Vendor Hierarchy 1/2'))
a530 = mara.join(a530, ['vendor_hierarchy_1_zzvendh1','manufacturer_mfrnr']).withColumn('eligibility_type',F.lit('Vendor Hierarchy 1'))
a586 = mara.join(a586, 'material_matnr').withColumn('eligibility_type',F.lit('Material'))
a587 = mara.join(a587, 'material_matnr').withColumn('eligibility_type',F.lit('Material'))

cols = map(lambda x: set(x.columns),[a524,a525,a526,a527,a528,a529,a530,a586,a587])
cols = list(set().union(*cols))

print(cols)
print(len(cols))

# Prepare A-tables for .unionByName() by giving them all the same columns
def get_standard_df(df, all_cols):
	return df.select(map(lambda x: F.col(x) if x in df.columns else F.lit(None).alias(x), all_cols))

a524 = get_standard_df(a524, cols)
a525 = get_standard_df(a525, cols)
a526 = get_standard_df(a526, cols)
a527 = get_standard_df(a527, cols)
a528 = get_standard_df(a528, cols)
a529 = get_standard_df(a529, cols)
a530 = get_standard_df(a530, cols)
a586 = get_standard_df(a586, cols)
a587 = get_standard_df(a587, cols)

print(a529.rdd.getNumPartitions())

agreements_table = (a524.unionByName(a525)
	.unionByName(a526)
	.unionByName(a527)
	.unionByName(a528)
	.unionByName(a529)
	.unionByName(a530)
	.unionByName(a586)
	.unionByName(a587))

konp_columns = ['calculation_type_krech',
		'condition_record_no_knumh',
		'pricing_unit_kpein',
		'condition_type_kschl',
		'rate_kbetr']
konp = spark.sql('SELECT ' + ','.join(konp_columns) + ' FROM saperp.konp')
konp = konp.filter(konp.rate_kbetr != 0)
konp_zlis = (konp.filter(konp.condition_type_kschl == 'ZLIS')
	.drop('condition_type_kschl')
	.withColumnRenamed('rate_kbetr','price'))
konp_non_zlis = konp.filter(konp.condition_type_kschl.isin(['Z008','Z009','Z011','Z013'])).drop('condition_type_kschl')

agreements_table = agreements_table.join(konp_non_zlis, 'condition_record_no_knumh')



percent_calculations = (agreements_table
	.filter(agreements_table.calculation_type_krech == 'A')
	.withColumn('percentage', agreements_table.rate_kbetr/1000)
	.withColumn('test_date', F.when(agreements_table.conditn_pricing_date_kdatu=='00000000',F.lit(today)).otherwise(agreements_table.conditn_pricing_date_kdatu))
	.drop('condition_record_no_knumh')
	.drop('calculation_type_krech')
	.drop('pricing_unit_kpein')
	.drop('rate_kbetr'))

non_percent_calculations = agreements_table.filter(agreements_table.calculation_type_krech != 'A').withColumnRenamed('rate_kbetr','net_rate')

#pricing_date_zero = percent_calculations.filter(percent_calculations.conditn_pricing_date_kdatu == '00000000')
#pricing_date_nonzero = percent_calculations.filter(percent_calculations.conditn_pricing_date_kdatu != '00000000')

price_list_LAB = (percent_calculations.select('material_matnr','price_list_pltypd','test_date')
	.filter(percent_calculations.price_list_pltypd.isin(['L','A','B'])))
price_list_nonLAB = (percent_calculations.select('material_matnr','price_list_pltypd','test_date')
	.filter(percent_calculations.price_list_pltypd.isin(['L','A','B']) == False))

a729_cols = ['price_list_pltypd','material_matnr','valid_to_datbi AS a729_valid_to_datbi','valid_from_datab AS a729_valid_from_datab','condition_record_no_knumh',"'A729' AS r_source"]
a942_cols = ['price_list_pltypd','material_matnr','valid_to_datbi AS a942_valid_to_datbi','valid_from_datab AS a942_valid_from_datab','condition_record_no_knumh',"'A942' AS r_source"]
a944_cols = ['price_list_pltypd','material_matnr','valid_to_datbi AS a944_valid_to_datbi','valid_from_datab AS a944_valid_from_datab','condition_record_no_knumh',"'A944' AS a944_source"]

a729 = spark.sql('SELECT ' + ','.join(a729_cols) + ' FROM saperp.a729')
a942 = spark.sql('SELECT ' + ','.join(a942_cols) + ' FROM saperp.a942')
a944 = spark.sql('SELECT ' + ','.join(a944_cols) + ' FROM saperp.a944')

a729 = price_list_LAB.join(a729,['material_matnr','price_list_pltypd'])
a729 = (a729.filter(a729.test_date <= a729.a729_valid_to_datbi)
	.filter(a729.test_date >= a729.a729_valid_from_datab)
	.drop('a729_valid_from_datab')
	.drop('a729_valid_to_datbi')
	.groupby(['material_matnr','price_list_pltypd','test_date','condition_record_no_knumh','r_source'])
	.count()
	.drop('count'))


a942 = price_list_nonLAB.join(a942,['material_matnr','price_list_pltypd'])
a942 = (a942.filter(a942.test_date <= a942.a942_valid_to_datbi)
	.filter(a942.test_date >= a942.a942_valid_from_datab)
	.drop('a942_valid_from_datab')
	.drop('a942_valid_to_datbi')
	.groupby(['material_matnr','price_list_pltypd','test_date','condition_record_no_knumh','r_source'])
	.count()
	.drop('count'))

a944 = (percent_calculations.filter(percent_calculations.conditn_pricing_date_kdatu != '00000000')
	.select('material_matnr','price_list_pltypd','test_date')
	.join(a944,['material_matnr','price_list_pltypd']))
a944 = (a944.filter(a944.test_date <= a944.a944_valid_to_datbi)
	.filter(a944.test_date >= a944.a944_valid_from_datab)
	.drop('a944_valid_from_datab')
	.drop('a944_valid_to_datbi')
	.withColumnRenamed('condition_record_no_knumh','a944_condition_record_no_knumh')
	.groupby(['material_matnr','price_list_pltypd','test_date','a944_condition_record_no_knumh','a944_source'])
	.count()
	.drop('count'))

primary_tables = a729.unionByName(a942)
percentage_records = primary_tables.join(a944,['material_matnr','price_list_pltypd','test_date'],'outer')

percentage_records = (
	percentage_records.withColumn('condition_record_no_knumh',
		F.when(percentage_records.condition_record_no_knumh.isNull(),percentage_records.a944_condition_record_no_knumh).otherwise(percentage_records.condition_record_no_knumh))
	.withColumn('r_source',
		F.when(percentage_records.r_source.isNull(),percentage_records.a944_source).otherwise(percentage_records.r_source))
	.drop('a944_source')
	.drop('a944_condition_record_no_knumh'))

percent_calculations = percent_calculations.join(percentage_records,['material_matnr','price_list_pltypd','test_date'])
percent_calculations = percent_calculations.withColumn('source',F.concat(percent_calculations.source,F.lit('-'),percent_calculations.r_source)).drop('r_source').drop('test_date')


percent_calculations = percent_calculations.join(konp_zlis, 'condition_record_no_knumh')
percent_calculations = percent_calculations.withColumn('net_rate', percent_calculations.price*(1-percent_calculations.percentage))
print('percent_calculations')
percent_calculations.printSchema()

print('non_percentage_records')
non_percent_calculations.printSchema()

output = percent_calculations.unionByName(non_percent_calculations.withColumn('percentage',F.lit(None)).withColumn('price',F.lit(None)))

t685t = spark.sql('SELECT condition_type_kschl,name_vtext FROM saperp.t685t')

output = output.join(t685t,'condition_type_kschl','left_outer').withColumnRenamed('name_vtext','condition_type_desc')

kona = spark.sql('SELECT agreement_knuma AS sales_deal_knumaag,description_botext,external_description_abrex FROM saperp.kona')

output = output.join(kona,'sales_deal_knumaag','left_outer')

output.write.parquet('material_eligibility_for_gcp.parquet',mode='overwrite')
