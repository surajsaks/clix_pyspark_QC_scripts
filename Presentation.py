import pandas as pd
import numpy as np
import pysftp

import findspark
findspark.init()
import pyspark
from datetime import date, timedelta
from pyspark import SparkContext,SparkConf
from pyspark.sql import SQLContext,SparkSession
from pyspark.sql.types import StructType,StructField, StringType, DoubleType,IntegerType, DateType,FloatType,DecimalType
from pyspark.sql.functions import col,last_day,date_format,regexp_replace,when,isnan
# import pyspark.sql.functions as f
conf = SparkConf().setMaster('local[8]').setAppName('Presentation')\
                    .set('spark.serializer','org.apache.spark.serializer.KryoSerializer')
#sc = SparkSession.builder.appName('Consol').getOrCreate()
sc = SparkContext(conf=conf)
sqlctx = SQLContext(sc)

data = sqlctx.read.format('csv').option('delimiter',' ').load("/home/ajay/sftp/config/cred.txt")
data1 = sqlctx.read.format('csv').option('delimiter',' ').load("/home/ajay/sftp/config/iopath.txt")

hostname = data.collect()[0][1]
username = data.collect()[1][1]
password = data.collect()[2][1]
remotepath = data1.collect()[0][1]
inputpath = data1.collect()[1][1]
outpath = data1.collect()[2][1]
with pysftp.Connection(host=hostname,username=username,password=password) as sftp:
    print("Connrction succesfully")

    remotpath = sftp.cwd(remotepath)

    directory_structure = sftp.listdir_attr()
    letest = 0
    letestfile = None
    for attr in directory_structure:
        if attr.filename.startswith('Consol_') and attr.st_mtime > letest:
            letest = attr.st_mtime
            letestfile = attr.filename
            print(letestfile)
    if letestfile is not None:
        sftp.get(letestfile,letestfile)

df2 = pd.read_excel(f'{letestfile}')
my_schema = StructType([\
    StructField("S.No.",StringType(),True),\
    StructField("LAN No",StringType(),True),\
    StructField("Customer Name",StringType(),True),\
    StructField("Region",StringType(),True),\
    StructField("Product",StringType(),True),\
    StructField("Sub Product",StringType(),True),\
    StructField("System",StringType(),True),\
    StructField("Due Date",DateType(),True),\
    StructField("EMI",StringType(),True),\
    StructField("Disbursement",DateType(),True),\
    StructField("File Receiving Status",StringType(),True),\
    StructField("Presentation status",StringType(),True),\
    StructField("Reason for non presentation",StringType(),True),\
    StructField("Raised by",StringType(),True),\
    StructField("Raised Date",StringType(),True),\
    StructField("Current Month Repayment Mode",StringType(),True),\
    StructField("NACH Reg Status",StringType(),True),\
    StructField("UMRN",StringType(),True),\
    StructField("NACH Rejection Reason",StringType(),True),\
    StructField("Instument No.(Cheque/NACH)",StringType(),True),\
    StructField("Instrument Amount(Cheque/NACH)",StringType(),True),\
    StructField("Payment Status",StringType(),True),\
    StructField("Bounce Reason",StringType(),True),\
    StructField("CLIX_ENTITY",StringType(),True),\
])
df=sqlctx.createDataFrame(df2,schema=my_schema)
df8=df.count()
print(df8)

# df.show()
df=df.distinct()
df9 = df.count()
print(df9)
df=df.select("*",col("LAN No").rlike("[^A-Z0-9]").alias("flag"))

df= df.where("flag == false")
# df = df.na.drop(subset=["Disbursement"]).show(truncate=False)
# df = df.where("Disbursement = '" | "Disbursement " )
# df = df(col("Disbursement")).dropna()
df = df.where(df.Disbursement.isNotNull())

last_day_of_prev_month = date.today().replace(day=1) - timedelta(days=1)
last_day_of_prev_month = last_day_of_prev_month.strftime('%d%m%Y')
print(last_day_of_prev_month)
# df = df[["Bo unce Reason"]].fillna('')
# df= df.fillna(value = '')




df1=df.select("LAN No",col("Customer name"),col("Product"),col("Sub Product"),col("Due Date"),col("Disbursement"),col("Payment Status"),col("Bounce Reason") )
df1 = df1.filter(col("Product")=="PL").select(['LAN No','Customer Name','Sub Product','Due Date','Disbursement','Payment Status','Bounce Reason'])
# df1=df1.select(col("LAN No"),col("Customer name"),col("Product"),col("Sub Product"),col("Due Date"),col("Disbursement"),col("Payment Status"),col("Bounce Reason"))
df1 = df1.withColumn("Month",last_day(col("Due date")).alias("Month"))
# df1 = df1.select(Month",date_format("Month","MM/dd/yyyy").alias("month"))
df1=df1.withColumn("Payment Status",regexp_replace("Payment Status",'CLR','CLEAR'))
df1=df1.withColumn("Payment Status",regexp_replace("Payment Status",'BOU','BOUNCE'))
df1=df1.select(date_format("Month","yyyy-MM-dd 00:00:00").alias("month"),col("LAN No").alias("lan_no"),col("Customer Name").alias("customer_name"),col("Sub Product").alias("product"),date_format("Due Date","dd-MM-yyyy").alias("due_date"),date_format("Disbursement","dd-MM-yyyy").alias("disbursement_date"),col("Payment Status").alias("status"),col("Bounce Reason").alias("bounce_reason")).show()
# df1.toPandas().to_csv(f'{outpath}+', index=False)


df2 = df.filter(col("Product")=="SME(BL/LAP)").select(['LAN No','Customer Name','Sub Product','Due Date','Disbursement','Payment Status','Bounce Reason'])
# df2=df2.select(col("LAN No"),col("Customer name"),col("Product"),col("Sub Product"),col("Due Date"),col("Disbursement"),col("Payment Status"),col("Bounce Reason"))
df2 = df2.withColumn("Month",last_day(col("Due date")).alias("Month"))
df2=df2.withColumn("Payment Status",regexp_replace("Payment Status",'CLR','CLEAR'))
df2=df2.withColumn("Payment Status",regexp_replace("Payment Status",'BOU','BOUNCE'))
df2=df2.select(date_format("Month","yyyy-MM-dd 00:00:00").alias("month"),col("LAN No").alias("lan_no"),col("Customer Name").alias("customer_name"),col("Sub Product").alias("product"),date_format("Due Date","dd-MM-yyyy").alias("due_date"),date_format("Disbursement","dd-MM-yyyy").alias("disbursement_date"),col("Payment Status").alias("status"),col("Bounce Reason").alias("bounce_reason")).show()
# df2.toPandas().to_csv(f'{outpath}+', index=False)


df3 = df.filter((col("Product")=="UC") | (col("Product") == "TW")).select(['LAN No','Customer Name','Sub Product','Due Date','Disbursement','Payment Status','Bounce Reason'])
# df3=df3.select(col("LAN No"),col("Customer name"),col("Product"),col("Sub Product"),col("Due Date"),col("Disbursement"),col("Payment Status"),col("Bounce Reason"))
df3 = df3.withColumn("Month",last_day(col("Due date")).alias("Month"))
df3=df3.withColumn("Payment Status",regexp_replace("Payment Status",'CLR','CLEAR'))
df3=df3.withColumn("Payment Status",regexp_replace("Payment Status",'BOU','BOUNCE'))
df3=df3.select(date_format("Month","yyyy-MM-dd 00:00:00").alias("month"),col("LAN No").alias("lan_no"),col("Customer Name").alias("customer_name"),col("Sub Product").alias("product"),date_format("Due Date","dd-MM-yyyy").alias("due_date"),date_format("Disbursement","dd-MM-yyyy").alias("disbursement_date"),col("Payment Status").alias("status"),col("Bounce Reason").alias("bounce_reason")).show()
# df3.toPandas().to_csv(f'{outpath}+', index=False)

# if df8 != df9:
#     print("Duplicate value")