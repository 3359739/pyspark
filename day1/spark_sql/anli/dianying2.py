from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql import functions as F
from pyspark.sql.functions import col
from sqlalchemy import func


if __name__ == '__main__':
   spark = SparkSession.builder.master("local[*]").appName("Python Spark SQL").getOrCreate()
   sc=spark.sparkContext
   df=spark.read.format("csv").option("header","true").option("sep",",").option("inferSchema",True).load("file:///wenjian/lxh.data")
   # dsl
   df.groupBy("go_id").avg("feng").withColumn("avg(feng)",F.round(col("avg(feng)"),2)).show()
   #sql
   df.createTempView("vi")
   spark.sql("select go_id ,round(avg(feng),2) from vi group by go_id").show()