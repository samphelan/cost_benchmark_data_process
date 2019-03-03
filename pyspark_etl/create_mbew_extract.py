from pyspark import SparkConf
from pyspark.sql import SparkSession

# Declare a SparkSession
conf = SparkConf()
spark = SparkSession.builder.enableHiveSupport().getOrCreate()

from pyspark.sql import functions as F

mbew = spark.sql('SELECT material_matnr,valuation_area_bwkey AS plant_werks,standard_price_stprs,price_unit_peinh FROM saperp.mbew')
mbew.printSchema()
mbew = mbew.filter(mbew['standard_price_stprs'] != 0)

marc = spark.sql('SELECT material_matnr,mrp_type_dismm,plant_werks FROM saperp.marc')

mbew = mbew.join(marc,['material_matnr','plant_werks'],'left_outer')
mbew = mbew.filter((mbew['mrp_type_dismm'] != 'ND') & (mbew['mrp_type_dismm'] != 'ZD'))

counts = mbew.groupby(['material_matnr','price_unit_peinh']).count().alias('counts')
result = (counts.groupby('material_matnr')
	.agg(F.max(F.struct(F.col('count'),F.col('price_unit_peinh'))).alias('max'))
	.select(F.col('material_matnr'),F.col('max.price_unit_peinh'))
	.withColumnRenamed('price_unit_peinh','common_per_stock'))


mbew = mbew.join(result,'material_matnr')

mbew = mbew.withColumn('standard_price_stprs', mbew['standard_price_stprs']/mbew['price_unit_peinh']*mbew['common_per_stock'])
mbew = mbew.drop('price_unit_peinh')

mbew.write.parquet('mbew.parquet', mode="overwrite")