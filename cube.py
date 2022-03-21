import pandas as pd
import pysftp
# import paramika
import functools
import numpy as np
# import regex
import findspark
findspark.init()
import pyspark
from datetime import date, timedelta
from pyspark import SparkContext,SparkConf
from pyspark.sql import SQLContext,SparkSession
from pyspark.sql.types import StructType,StructField, StringType, DoubleType,IntegerType, DateType,FloatType,DecimalType
from pyspark.sql.functions import col,last_day,date_format,regexp_replace,lit
# import pyspark.sql.functions as f
conf = SparkConf().setMaster('local[8]').setAppName('Cube')\
                    .set('spark.serializer','org.apache.spark.serializer.KryoSerializer')
#sc = SparkSession.builder.appName('Consol').getOrCreate()
sc = SparkContext(conf=conf)
sqlctx = SQLContext(sc)






df2 = pd.read_excel("/home/ajay/Downloads/PL_Cube_till Jan'22.xlsx")
my_schema = StructType([\
    StructField("program_type",StringType(),True),\
    StructField("Disb_Month",StringType(),True),\
    StructField("account_no",StringType(),True),\
    StructField("loan_santiones",StringType(),True),\
    StructField("sentioned_amount",IntegerType(),True),\
    StructField("focus",IntegerType(),True),\
    StructField("industry",StringType(),True),\
    StructField("Morat_count",IntegerType(),True),\
    StructField("Covid 19 Restructuring 1.0",StringType(),True),\
    StructField("Covid 19 Restructuring 2.0",StringType(),True),\
])
df=sqlctx.createDataFrame(df2,schema=my_schema)
df=df.select("*",col("account_no").rlike("[^A-Z0-9]").alias("flag"))
df= df.where("flag == false")
df = df.select("*",col("sentioned_amount").rlike("[^A-Za-z]").alias("amount_flag"))
df = df.where("amount_flag == true")
df = df.where(df.loan_santiones.isNotNull())
df=df.withColumn("sourcing_branch",lit(''))
df=df.withColumn("product",lit('PL'))
df=df.toDF(*(c.replace('.','_') for c in df.columns))
df=df.select(col("program_type"),col("Disb_Month").alias("disb_month"),col("account_no"),col("sourcing_branch"),date_format("loan_santiones","yyyy-MM-dd 00:00:00").alias("loan_santioned"),col("sentioned_amount"),col("product"),col("focus"),col("industry"),col("Morat_count").alias("count_of_morat"),col("Covid 19 Restructuring 1_0").alias("Covid 19 Restructuring 1.0"),col("Covid 19 Restructuring 2_0").alias("Covid 19 Restructuring 2.0"))
df.show()

df3 = pd.read_excel("/home/ajay/Downloads/BL_cube_Jan'22.xlsx")
my_schema1 = StructType([ \
    StructField("credit_program_name", StringType(), True), \
    StructField("account_no", StringType(), True), \
    StructField("login_date", DateType(), True), \
    StructField("sentioned_amount", IntegerType(), True), \
    StructField("focus_segment", IntegerType(), True), \
    StructField("covid restructuring 1.0", StringType(), True), \
    StructField("covid restructuring 2.0", StringType(), True), \
    StructField("Branch", StringType(), True), \
    StructField("Morat", StringType(), True), \
    StructField("industry", StringType(), True), \
 \
    ])
df1 = sqlctx.createDataFrame(df3, schema=my_schema1)
df1 = df1.select("*", col("account_no").rlike("[^A-Z0-9]").alias("flag"))
df1 = df1.where("flag == false")
# df1.show()

df1 = df1.where(df1.login_date.isNotNull())
df1 = df1.select("*", date_format("login_date", "yyyyMM").alias("disb_month"))
df1 = df1.withColumn("product", lit('BL'))
df1 = df1.toDF(*(c.replace('.', '_') for c in df1.columns))
df3 = df1.select(col("credit_program_name").alias("program_type"), col("disb_month"), col("account_no"),\
                 col("Branch").alias("sourcing_branch"),\
                 date_format("login_date", "yyyy-MM-dd 00:00:00").alias("loan_sentioned"), col("sentioned_amount"),\
                 col("product"), col("focus_segment").alias("focus"), col("industry"),\
                 col("Morat").alias("count_of_morat"),\
                 col("covid restructuring 1_0").alias("Covid 19 Restructuring 1.0"),\
                 col("covid restructuring 2_0").alias("Covid 19 Restructuring 2.0"))
df3.show()


df5 = df.union(df3)
df5.show()





