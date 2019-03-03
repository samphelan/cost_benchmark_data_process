from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import *
import datetime

# Declare a SparkSession
conf = SparkConf()
spark = SparkSession.builder.enableHiveSupport().getOrCreate()

#Customer Info Section
kna1 = spark.sql('SELECT customer_kunnr,name_name1,account_group_ktokd,customer_classific_kukla,vertical_mkt_attrib_zzbran6,customer_market_zzmarket,customer_submarket_zzsubmarket FROM saperp.kna1')
knvv = spark.sql('SELECT customer_kunnr,delivering_plant_vwerk,sales_district_bzirk,sales_office_vkbur,sales_organization_vkorg FROM saperp.knvv')
knvv = knvv.filter(knvv['sales_organization_vkorg'] == '1000').drop('sales_organization_vkorg')
kna1 = kna1.join(knvv,'customer_kunnr')
zvlocation = spark.sql('SELECT plant_location AS delivering_plant_vwerk,zone_zzone FROM saperp.zvlocation')
kna1 = kna1.join(zvlocation,'delivering_plant_vwerk','left_outer')
zvlocation2 = spark.sql('SELECT plant_location AS r_delivering_plant_vwerk,zone_zzone AS r_zone_zzone FROM saperp.zvlocation')
zvlocation = zvlocation.join(zvlocation2,zvlocation['delivering_plant_vwerk'] == zvlocation2['r_zone_zzone'])
zvlocation = zvlocation.groupby(['delivering_plant_vwerk','zone_zzone']).count().drop('count')
zvlocation = zvlocation.filter(zvlocation['delivering_plant_vwerk'].isin(['ZAGA','ZCNC','ZCNJ','ZCOH','ZFCA','ZJIL','ZRMN','ZRVA','ZSMO','ZSTX','ZTFL','ZTMA','ZYOH']) == False)
zvlocation = zvlocation.toDF(*['r_delivering_plant_vwerk','r_zone_zzone'])
kna1 = kna1.join(zvlocation,kna1.zone_zzone==zvlocation.r_delivering_plant_vwerk, 'left_outer').drop('r_delivering_plant_vwerk')
kna1 = kna1.withColumn('zone_zzone',F.expr("""IF(r_zone_zzone IS NOT NULL, r_zone_zzone, zone_zzone)""")).drop('r_zone_zzone')
t171t = spark.sql('SELECT sales_district_bzirk,district_name_bztxt FROM saperp.t171t')
kna1 = kna1.join(t171t,'sales_district_bzirk','left_outer')
zzbran6t = spark.sql('SELECT vertical_mkt_attrib_zzbran6,description_vtext FROM saperp.zzbran6t')
kna1 = kna1.join(zzbran6t,'vertical_mkt_attrib_zzbran6','left_outer').withColumnRenamed('description_vtext','vertical_mkt_desc')
tkukt = spark.sql('SELECT customer_classific_kukla,description_vtext FROM saperp.tkukt')
kna1 = kna1.join(tkukt,'customer_classific_kukla','left_outer').withColumnRenamed('description_vtext','cot_desc')
markets_schema = StructType([StructField('customer_market_zzmarket',StringType(),True),StructField('customer_market_description',StringType(),True)])
submarkets_schema = StructType([StructField('customer_submarket_zzsubmarket',StringType(),True),StructField('customer_submarket_description',StringType(),True)])
customer_markets = spark.read.schema(markets_schema).csv('customer_market_text_table.csv')
customer_submarkets = spark.read.schema(submarkets_schema).csv('customer_submarket_text_table.csv')
kna1 = kna1.join(customer_markets, 'customer_market_zzmarket', 'left_outer')
kna1 = kna1.join(customer_submarkets, 'customer_submarket_zzsubmarket', 'left_outer')


#Eligibility Agreements
a516_cols = ['sales_deal_knumaag',
	'sales_organization_vkorg',
	'customer_kunnr',
	'customer_classific_zkukla AS customer_classific_kukla',
	'customer_hienr',
	'valid_to_datbi AS customer_valid_to',
	'valid_from_datab AS customer_valid_from',
	'shipto_party_kunwe']
a516 = spark.sql('SELECT ' + ','.join(a516_cols) + ' FROM saperp.a516')
today = datetime.date.today().strftime('%Y%m%d')
a516 = (a516.filter(a516.sales_organization_vkorg == '1000')
	.drop('sales_organization_vkorg')
	.filter(a516.shipto_party_kunwe == '')
	.drop('shipto_party_kunwe')
	.filter(a516.customer_valid_to >= today)
	.filter(a516.customer_valid_from <= today))

#Class of Trade Agreements
cot_agreements = (a516.filter(a516.customer_classific_kukla != '')
	.drop('customer_kunnr')
	.drop('customer_hienr'))
a518_cols = ['sales_deal_knumaag',
	'sales_organization_vkorg',
	'valid_from_datab',
	'valid_to_datbi',
	'sales_district_bzirk',
	'sales_office_vkbur']
a518 = spark.sql('SELECT ' + ','.join(a518_cols) + ' FROM saperp.a518')
a518 = (a518.filter(a518.sales_organization_vkorg == '1000')
	.filter(a518.valid_to_datbi >= today)
	.filter(a518.valid_from_datab <= today)
	.drop('sales_organization_vkorg')
	.drop('valid_to_datbi')
	.drop('valid_from_datab'))
cot_agreements = cot_agreements.join(a518, 'sales_deal_knumaag')
cot_district = cot_agreements.filter(cot_agreements.sales_district_bzirk != '').drop('sales_office_vkbur')
cot_sales_office = cot_agreements.filter(cot_agreements.sales_district_bzirk == '').drop('sales_district_bzirk')
	
cot_district = (kna1.join(cot_district,['customer_classific_kukla','sales_district_bzirk'])
	.withColumn('customer_eligibility_type',F.lit('COT/District'))
	.withColumn('hierarchy_num',F.lit(None))
	.withColumn('hierarchy_lvl',F.lit(None)))
cot_sales_office = (kna1.join(cot_sales_office,['customer_classific_kukla','sales_office_vkbur'])
	.withColumn('customer_eligibility_type',F.lit('COT/Sales Office'))
	.withColumn('hierarchy_num',F.lit(None))
	.withColumn('hierarchy_lvl',F.lit(None)))

#Customer Agreements
customer_agreements = (a516.filter(a516.customer_kunnr != '')
	.withColumn('customer_eligibility_type',F.lit('Customer'))
	.drop('customer_classific_kukla')
	.drop('customer_hienr'))
customer_agreements = (kna1.join(customer_agreements,'customer_kunnr')
	.withColumn('hierarchy_num',F.lit(None))
	.withColumn('hierarchy_lvl',F.lit(None)))

#Hierarchy Agreements

hierarchy_agreements = (a516.filter(a516.customer_hienr != '')
	.drop('customer_kunnr')
	.drop('customer_classific_kukla'))
customer_hierarchy = spark.read.parquet('pricing_hierarchy.parquet')

hierarchy_lvl_2 = (hierarchy_agreements.join(customer_hierarchy.select('customer_kunnr', 'level_2_customer_kunnr'),hierarchy_agreements.customer_hienr==customer_hierarchy.level_2_customer_kunnr)
	.withColumn('hierarchy_lvl',F.lit('2'))
	.withColumnRenamed('level_2_customer_kunnr','hierarchy_num')
	.drop('customer_hienr'))

hierarchy_lvl_3 = (hierarchy_agreements.join(customer_hierarchy.select('customer_kunnr', 'level_3_customer_kunnr'),hierarchy_agreements.customer_hienr==customer_hierarchy.level_3_customer_kunnr)
	.withColumn('hierarchy_lvl',F.lit('3'))
	.withColumnRenamed('level_3_customer_kunnr','hierarchy_num')
	.drop('customer_hienr'))

hierarchy_lvl_4 = (hierarchy_agreements.join(customer_hierarchy.select('customer_kunnr', 'level_4_customer_kunnr'),hierarchy_agreements.customer_hienr==customer_hierarchy.level_4_customer_kunnr)
	.withColumn('hierarchy_lvl',F.lit('4'))
	.withColumnRenamed('level_4_customer_kunnr','hierarchy_num')
	.drop('customer_hienr'))

hierarchy_agreements = (hierarchy_lvl_2.unionByName(hierarchy_lvl_3).unionByName(hierarchy_lvl_4)
	.withColumn('customer_eligibility_type',F.lit('Hierarchy')))

hierarchy_agreements = kna1.join(hierarchy_agreements,'customer_kunnr')

all_agreements = cot_district.unionByName(cot_sales_office).unionByName(customer_agreements).unionByName(hierarchy_agreements)

all_agreements.write.parquet('customer_eligibility.parquet',mode='overwrite')