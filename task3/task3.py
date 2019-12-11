from pyspark import SparkContext
from csv import reader
from pyspark.sql import SQLContext
from pyspark.sql.functions import col
from pyspark.sql import functions as F


sc = SparkContext()
sqc = SQLContext(sc)
df = sqc.read.options(header='true',inferschema='true',sep='\t', encoding='ISO-8859-1').csv('/user/hm74/NYCOpenData/erm2-nwe9.tsv.gz')
timeFmt = "MM/dd/yyyy HH:mm:ss"  
timeDiff = (F.unix_timestamp(col('Closed Date'), format=timeFmt) - F.unix_timestamp(col('Created Date'),format=timeFmt))
df = df.withColumn("ResponseTimeHrs", timeDiff/3600.0)
df.groupBy("Incident Zip").agg(F.mean('ResponseTimeHrs')).show()
df.where(col('Borough')=='BROOKLYN').where(col('Complaint Type')=='Root/Sewer/Sidewalk Condition').count() # or all boroughs 
