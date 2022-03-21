import pandas as pd
import findspark
findspark.init()
import pyspark
from datetime import date, timedelta
from pyspark import SparkContext,SparkConf
from pyspark.sql import SQLContext,SparkSession
from pyspark.sql.types import StructType,StructField, StringType, DoubleType,IntegerType, DateType,FloatType,DecimalType
from pyspark.sql.functions import col,last_day,date_format,regexp_replace,lit
# import pyspark.sql.functions as f
conf = SparkConf().setMaster('local[9]').setAppName('KPI')\
                    .set('spark.serializer','org.apache.spark.serializer.KryoSerializer')
#sc = SparkSession.builder.appName('Consol').getOrCreate()
sc = SparkContext(conf=conf)
sqlctx = SQLContext(sc)

file_name = "KPI Jan'22"
df2 = pd.read_excel(f"/home/ajay/Downloads/{file_name}.xlsx")
df2=sqlctx.createDataFrame(df2)
df2.show()
print(df2.count())
df2=df2.distinct()
print(df2.count())
df2 = df2.select("*",col("Cust CON PROJ1").rlike("[^A-Z0-9]").alias("flag"))
df2 = df2.where("flag == false")
# df2.printSchema()
df2=df2.withColumn("revised_dpd",lit(''))
df2=df2.withColumn("cust_con_proj",df2["Cust CON PROJ1"])
df2=df2.select(col("Month").alias("month"),col("Cust CON PROJ1").alias("custconproj"),col("cust_con_proj"),col("DIVISION").alias("division"),col("ENTITY").alias("entity"),col("Revised Product").alias("revised_product"),col("Product").alias("product"),col("CUST_NAME").alias("cust_name"),col("System DPD").alias("system_dpd"),col("System Bucket-DX").alias("system_bucket"),col("revised_dpd"),col("Manual Bucket").alias("bucket"),col("Manual Backet 2").alias("bucket_2"),col("Finance NEA Dec'21").alias("fin_nea"),date_format("AGREEMENT_DATE","dd-MM-yyyy").alias("agreement_date"),date_format("MAT_DT","dd-MM-yyyy").alias("mat_dt"),col("MOB").alias("mob"),col("0+").alias("0_plus"),col("30+").alias("30_plus"),col("90+").alias("90_plus")).show()
# df2.printSchema()