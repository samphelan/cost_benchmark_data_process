from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import *
import datetime

# Declare a SparkSession
conf = SparkConf()
spark = SparkSession.builder.enableHiveSupport().getOrCreate()

today = datetime.date.today().strftime('%Y%m%d')

knvh = spark.sql('SELECT customer_kunnr,valid_from_datab,valid_to_datbi,higherlevel_customer_hkunnr,custhierarchy_type_hityp,sales_organization_vkorg FROM saperp.knvh')
knvh = (knvh.filter(knvh.custhierarchy_type_hityp == 'A')
	.filter(knvh.sales_organization_vkorg=='1000')
	.filter(knvh.valid_to_datbi >= today)
	.filter(knvh.valid_from_datab <= today)
	.drop('sales_organization_vkorg')
	.drop('custhierarchy_type_hityp')
	.drop('valid_to_datbi')
	.drop('valid_from_datab'))
knvh.write.parquet('pricing_hierarchy_for_gcp.parquet',mode='overwrite')

kna1 = spark.sql('SELECT customer_kunnr,name_name1,group_key_konzs,account_group_ktokd FROM saperp.kna1')
knvh = knvh.join(kna1,'customer_kunnr')


cols = ['level_1_customer_kunnr','level_1_name','level_1_grp_key','level_1_acct_grp']
hierarchy = knvh.filter(knvh.higherlevel_customer_hkunnr=='').drop('higherlevel_customer_hkunnr').toDF(*cols)
cols = cols + ['level_2_customer_kunnr','level_2_name','level_2_grp_key','level_2_acct_grp']
hierarchy = (hierarchy.join(knvh,hierarchy.level_1_customer_kunnr==knvh.higherlevel_customer_hkunnr,'left_outer')
	.drop('higherlevel_customer_hkunnr')
	.toDF(*cols))
cols = cols + ['level_3_customer_kunnr','level_3_name','level_3_grp_key','level_3_acct_grp']
hierarchy = (hierarchy.join(knvh,hierarchy.level_2_customer_kunnr==knvh.higherlevel_customer_hkunnr,'left_outer')
	.drop('higherlevel_customer_hkunnr')
	.toDF(*cols))
cols = cols + ['level_4_customer_kunnr','level_4_name','level_4_grp_key','level_4_acct_grp']
hierarchy = (hierarchy.join(knvh,hierarchy.level_3_customer_kunnr==knvh.higherlevel_customer_hkunnr,'left_outer')
	.drop('higherlevel_customer_hkunnr')
	.toDF(*cols))
cols = cols + ['level_5_customer_kunnr','level_5_name','level_5_grp_key','level_5_acct_grp']
hierarchy = (hierarchy.join(knvh,hierarchy.level_4_customer_kunnr==knvh.higherlevel_customer_hkunnr,'left_outer')
	.drop('higherlevel_customer_hkunnr')
	.toDF(*cols))

def pick_highest_lvl(input_1,input_2,input_3,input_4,input_5):
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
lvl_selection_UDF = F.udf(pick_highest_lvl, StringType())
hierarchy = hierarchy.withColumn('customer_kunnr',lvl_selection_UDF(hierarchy.level_1_customer_kunnr, hierarchy.level_2_customer_kunnr, hierarchy.level_3_customer_kunnr, hierarchy.level_4_customer_kunnr, hierarchy.level_5_customer_kunnr))

hierarchy.write.parquet('pricing_hierarchy.parquet',mode='overwrite')

