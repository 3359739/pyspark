from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql import functions as F
from pyspark.sql.functions import col
#这里是通过sparkcontext进行读取获取到的是一个rdd 通过rdd.toDF
if __name__ == '__main__':
   spark = SparkSession.builder.master("local[*]").appName("Python Spark SQL").getOrCreate()
   sc=spark.sparkContext
   #option("inferSchema",True)开启自动识别类型
   # .option("inferSchema", True)
   df=spark.read.format("csv").option("header","true").option("sep",",").load("file:///wenjian/lxh.data")
   #dsl
   df=df.withColumn("feng",col("feng").cast(IntegerType()))
   df.printSchema()
   dd=df.groupBy("nameid").avg('feng').withColumn("avg(feng)",F.round("avg(feng)",2))
   ww=dd.withColumn("wwww",col("avg(feng)")*2)
   ww.show()
   # 方式2
   df.groupBy("nameid").agg(F.round(F.avg("feng"),2)).show()

   #sql
   df.createTempView("vim")
   spark.sql("select nameid,round(avg(feng),2) as ww  from vim group by nameid").show()

