from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql import functions as F
from sqlalchemy import func
from pyspark.sql.functions import *


def dsl():
   # dsl
   # 方式一
   w = df.agg(F.round(avg("feng").alias("lxh"), 3)).first()
   df.where(df['feng'] > w[0]).select("go_id","feng").show()
   # 方式2
   df2=df.select(F.round(F.avg("feng").name("lxh"), 3)).first()[0]
   df.where(df['feng']>df2).select("go_id","feng").show()

def sql():
   df.createTempView("vi")
   spark.sql("select go_id,feng from vi where feng > (select avg(feng) from vi)").show()

if __name__ == '__main__':
   spark = SparkSession.builder.master("local[*]").appName("Python Spark SQL").getOrCreate()
   sc=spark.sparkContext
   df=spark.read.format("csv").option("header","true").option("sep",",").option("inferSchema",True).load("file:///wenjian/lxh.data")
   dsl()
   # sql()

   spark.stop()
   sc.stop()