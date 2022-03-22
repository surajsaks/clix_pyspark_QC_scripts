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

sc = SparkContext(conf=conf)
sqlctx = SQLContext(sc)

file_name = "KPI Jan'22"
df2 = pd.read_excel(f"/home/ajay/Downloads/{file_name}.xlsx")
my_schema = StructType([\
    StructField("Month",DateType(),True),\
    StructField("Month",DateType(),True),\
    StructField("Report",StringType(),True),\
    StructField("Cust CON PROJ",StringType(),True),\
    StructField("Cust CON PROJ1",StringType(),True),\
    StructField("DIVISION",StringType(),True),\
    StructField("ENTITY",StringType(),True),\
    StructField("Product",StringType(),True),\
    StructField("Main Product",StringType(),True),\
    StructField("Revised Product",StringType(),True),\
    StructField("CUST_NAME",StringType(),True),\
    StructField("System DPD",StringType(),True),\
    StructField("System Bucket-DK",StringType(),True),\
    StructField("Manual Bucket",StringType(),True),\
    StructField("Manual Bucket 2",StringType(),True),\
    StructField("Dec'21",StringType(),True),\
    StructField("Finance NEA-Dec'21",StringType(),True),\
    StructField("Segment",StringType(),True),\
    StructField("Moral Flag",StringType(),True),\
    StructField("Zone",StringType(),True),\
    StructField("CLIX_Branch",StringType(),True),\
    StructField("State",StringType(),True),\
    StructField("DSA Name & Dealer Name",StringType(),True),\
    StructField("AGREEMENT_DATE",DateType(),True), \
    StructField("MAT_DT", DateType(), True),\
    StructField("MOB", StringType(), True), \
    StructField("0+", StringType(), True), \
    StructField("30+", StringType(), True), \
    StructField("90+", StringType(), True), \
    StructField("Moral Status", StringType(), True),\
    StructField("Restructuring", StringType(), True), \
    StructField("Early Write off Status", StringType(), True), \
    StructField("Restructuren2.O", StringType(), True), \
    StructField("ARC Flag", StringType(), True), \
    StructField("ECLGS Flag", StringType(), True), \
    StructField("Write off Finance Over All ", StringType(), True), \
    StructField("Write off Finance_Nov'21", StringType(), True), \
    StructField("Presentation", StringType(), True), \
    StructField("Amt Collected", StringType(), True), \

    ])
df2=sqlctx.createDataFrame(df2,schema=my_schema)

df2 = df2.drop("Month")


last_day_of_prev_month = date.today().replace(day=1) - timedelta(days=1)

print(last_day_of_prev_month)

df2=df2.withColumn("revised_dpd",lit(''))
df2=df2.withColumn("cust_con_proj",df2["Cust CON PROJ1"])
df2=df2.withColumn("month",lit(last_day_of_prev_month))

df2=df2.select(date_format("month","yyyy-MM-dd 00:00:00").alias("month"),col("Cust CON PROJ1").alias("custconproj"),col("cust_con_proj"),col("DIVISION").alias("division"),col("ENTITY").alias("entity"),col("Revised Product").alias("revised_product"),col("Product").alias("product"),col("CUST_NAME").alias("cust_name"),col("System DPD").alias("system_dpd"),col("System Bucket-DK").alias("system_bucket"),col("revised_dpd"),col("Manual Bucket").alias("bucket"),col("Manual Bucket 2").alias("bucket_2"),col("Finance NEA-Dec'21").alias("fin_nea"),date_format("AGREEMENT_DATE","dd-MM-yyyy").alias("agreement_date"),date_format("MAT_DT","dd-MM-yyyy").alias("mat_dt"),col("MOB").alias("mob"),col("0+").alias("0_plus"),col("30+").alias("30_plus"),col("90+").alias("90_plus")).show()

# pd.DataFrame(df2).to_csv('/home/ajay/sftp/assest/output/abc.csv',index=False)
