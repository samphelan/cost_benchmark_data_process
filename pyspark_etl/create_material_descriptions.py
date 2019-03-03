from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import *
import datetime

# Declare a SparkSession
conf = SparkConf()
spark = SparkSession.builder.enableHiveSupport().getOrCreate()

today = datetime.date.today().strftime('%Y%m%d')

mara_cols = ['material_matnr',
	'manufacturer_mfrnr',
	'manufacturer_part_no_mfrpn',
	'material_group_matkl',
	'product_hierarchy_prdha',
	'vendor_hierarchy_1_zzvendh1',
	'vendor_hierarchy_2_zzvendh2',
	'vendor_hierarchy_3_zzvendh3',
	'xplant_matl_status_mstae',
	'eanupc_ean11',
	'material_type_mtart']
mara = spark.sql('SELECT ' + ','.join(mara_cols) + ' FROM saperp.mara')
makt = spark.sql('SELECT material_matnr,language_key_spras,material_description_maktx FROM saperp.makt')
makt_z = makt.filter(makt.language_key_spras=='Z').drop('language_key_spras')
makt_e = makt.filter(makt.language_key_spras=='E').drop('language_key_spras').withColumnRenamed('material_description_maktx','mfrpn')
makt = makt_z.join(makt_e,'material_matnr')
mara = mara.join(makt,'material_matnr','left_outer')
vbrp = spark.sql('SELECT * FROM quotes_project.vbrp_top_item')
vbrp = vbrp.filter(F.substring(vbrp.created_on_erdat,0,4) >= '2012')
vbrp = vbrp.groupby('material_matnr').agg(F.max('material_group_2_mvgr2'))
mara = mara.join(vbrp, 'material_matnr','left_outer')
a729 = spark.sql('SELECT price_list_pltypd,material_matnr,valid_to_datbi,valid_from_datab,condition_record_no_knumh AS zlis_condition FROM saperp.a729')
a942 = spark.sql('SELECT price_list_pltypd,material_matnr,valid_to_datbi,valid_from_datab,condition_record_no_knumh AS zlis_condition FROM saperp.a942')
a729 = (a729.filter(a729.valid_to_datbi >= today)
	.filter(a729.valid_from_datab <= today)
	.drop('valid_to_datbi')
	.drop('valid_from_datab'))
a942 = (a942.filter(a942.valid_to_datbi >= today)
	.filter(a942.valid_from_datab <= today)
	.drop('valid_to_datbi')
	.drop('valid_from_datab'))
zlis_conditions = a729.unionByName(a942)
mara = mara.join(zlis_conditions,'material_matnr','left_outer')
a911 = spark.sql('SELECT condition_type_kschl,vendor_lifnr AS manufacturer_mfrnr,material_matnr,valid_to_datbi,valid_from_datab,condition_record_no_knumh AS zpn0_condition FROM saperp.a911')
a911 = (a911.filter(a911.valid_to_datbi >= today)
	.filter(a911.valid_from_datab <= today)
	.drop('valid_to_datbi')
	.drop('valid_from_datab'))
mara = mara.join(a911,['material_matnr','manufacturer_mfrnr'],'left_outer')
konp = spark.sql('SELECT condition_record_no_knumh,pricing_unit_kpein,rate_kbetr FROM saperp.konp')
mara = (mara.join(konp.withColumnRenamed('condition_record_no_knumh','zlis_condition'),'zlis_condition','left_outer')
	.withColumnRenamed('pricing_unit_kpein','zlis_uom')
	.withColumnRenamed('rate_kbetr','zlis_rate')
	.drop('zlis_condition'))
mara = (mara.join(konp.withColumnRenamed('condition_record_no_knumh','zpn0_condition'),'zpn0_condition','left_outer')
	.withColumnRenamed('rate_kbetr','zpn0_rate')
	.drop('pricing_unit_kpein')
	.drop('zpn0_condition'))
a662 = spark.sql('SELECT price_list_pltypd,material_matnr,valid_to_datbi,valid_from_datab,condition_record_no_knumh FROM saperp.a662')
a662 = (a662.filter(a662.price_list_pltypd=='T3')
	.filter(a662.valid_to_datbi >= today)
	.filter(a662.valid_from_datab <= today)
	.drop('price_list_pltypd')
	.drop('valid_to_datbi')
	.drop('valid_from_datab'))
mara = mara.join(a662,'material_matnr','left_outer').withColumnRenamed('condition_record_no_knumh','t3_condition')
mara = (mara.join(konp.withColumnRenamed('condition_record_no_knumh','t3_condition'),'t3_condition','left_outer')
	.withColumnRenamed('rate_kbetr','t3_rate')
	.drop('pricing_unit_kpein')
	.drop('t3_condition'))
lfa1 = spark.sql('SELECT vendor_lifnr AS manufacturer_mfrnr,name_name1,type_of_industry_j1kftind FROM saperp.lfa1')
mara = mara.join(lfa1,'manufacturer_mfrnr','left_outer')
t023t = spark.sql('SELECT material_group_matkl,material_grp_desc_2_wgbez60 FROM saperp.t023t')
mara = mara.join(t023t,'material_group_matkl','left_outer')
t134t = spark.sql('SELECT material_type_mtart,material_type_descr_mtbez FROM saperp.t134t')
mara = mara.join(t134t,'material_type_mtart','left_outer').drop('material_type_mtart')
t141t = spark.sql('SELECT plantspmatl_status_mmsta AS xplant_matl_status_mstae,description_mtstb FROM saperp.t141t')
mara = mara.join(t141t,'xplant_matl_status_mstae','left_outer').drop('xplant_matl_status_mstae')
slab_columns = (mara.filter(mara.price_list_pltypd.isNull()==False)
	.withColumn('SD', F.when(mara.price_list_pltypd=='SD',mara.zlis_rate).otherwise(None))
	.withColumn('L', F.when(mara.price_list_pltypd=='L',mara.zlis_rate).otherwise(None))
	.withColumn('A', F.when(mara.price_list_pltypd=='A',mara.zlis_rate).otherwise(None))
	.withColumn('B', F.when(mara.price_list_pltypd=='B',mara.zlis_rate).otherwise(None))
	.groupby('material_matnr')
	.agg(F.avg('SD'),F.avg('L'),F.avg('A'),F.avg('B')))
groupby_cols = ['material_matnr',
	'manufacturer_mfrnr',
	'manufacturer_part_no_mfrpn',
	'material_group_matkl',
	'product_hierarchy_prdha',
	'vendor_hierarchy_1_zzvendh1',
	'vendor_hierarchy_2_zzvendh2',
	'vendor_hierarchy_3_zzvendh3',
	'name_name1',
	'type_of_industry_j1kftind',
	'material_grp_desc_2_wgbez60',
	'material_type_descr_mtbez',
	'material_description_maktx',
	'mfrpn',
	'max(material_group_2_mvgr2)',
	'zpn0_rate',
	'eanupc_ean11',
	't3_rate',
	'description_mtstb'
	]
slab_columns = slab_columns.join(mara.groupby(groupby_cols).count().drop('count'),'material_matnr')
slab_columns = slab_columns.join(mara.filter(mara.zlis_rate.isNull()==False).groupby(['material_matnr','zlis_uom']).count().drop('count'),'material_matnr')
non_slab = (mara.filter(mara.price_list_pltypd.isNull())
	.drop('zlis_rate')
	.drop('price_list_pltypd')
	.drop('condition_type_kschl')
	.withColumn('avg(A)',F.lit(None))
	.withColumn('avg(B)',F.lit(None))
	.withColumn('avg(L)',F.lit(None))
	.withColumn('avg(SD)',F.lit(None)))

material_descriptions = slab_columns.unionByName(non_slab)
material_descriptions = material_descriptions.withColumn('material_matnr',F.when(material_descriptions.material_matnr.isNull(),F.lit('NOF/LOT')).otherwise(material_descriptions.material_matnr))

material_descriptions = (material_descriptions.withColumnRenamed('avg(A)','a_cost')
	.withColumnRenamed('avg(B)','b_cost')
	.withColumnRenamed('avg(L)','l_cost')
	.withColumnRenamed('avg(SD)','sd_cost')
	.withColumnRenamed('description_mtstb','natl_dno_code')
	.withColumnRenamed('eanupc_ean11','upc')
	.withColumnRenamed('manufacturer_mfrnr','supplier_num')
	.withColumnRenamed('manufacturer_part_no_mfrpn','mfrpn_abbreviated')
	.withColumnRenamed('material_description_maktx','material_desc')
	.withColumnRenamed('material_group_matkl','material_grp_num')
	.withColumnRenamed('material_grp_desc_2_wgbez60','material_grp_desc')
	.withColumnRenamed('material_matnr','material_matnr')
	.withColumnRenamed('material_type_descr_mtbez','material_type')
	.withColumnRenamed('mfrpn','mfrpn')
	.withColumnRenamed('name_name1','supplier_name')
	.withColumnRenamed('product_hierarchy_prdha','product_hierarchy')
	.withColumnRenamed('t3_rate','t3_cost')
	.withColumnRenamed('type_of_industry_j1kftind','supplier_status')
	.withColumnRenamed('vendor_hierarchy_1_zzvendh1','vendor_hierarchy_1')
	.withColumnRenamed('vendor_hierarchy_2_zzvendh2','vendor_hierarchy_2')
	.withColumnRenamed('vendor_hierarchy_3_zzvendh3','vendor_hierarchy_3')
	.withColumnRenamed('zlis_uom','slab_uom')
	.withColumnRenamed('zpn0_rate','natl_net_cost')
	.withColumnRenamed('max(material_group_2_mvgr2)','top_item'))

print('output')
material_descriptions.printSchema()

material_descriptions.write.parquet('material_descriptive_file.parquet',mode='overwrite')