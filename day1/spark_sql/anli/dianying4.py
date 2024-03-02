from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql import functions as F
from sqlalchemy import func
from pyspark.sql.functions import *


def dsl():
   # print(df.select(F.round(F.avg("feng"), 2)).first()[0])//平均分
   w=df.where(df['feng'] > df.select(F.round(F.avg("feng"), 2)).first()[0]).groupBy("nameid").count()
   we=w.select(F.max("count")).first()[0]
   weoxomg=w.where(w['count'] == we).first()[0]
   df.where(df["nameid"] == weoxomg).show()
   df.where(df["nameid"]==weoxomg).agg(F.round(F.avg("feng"),2)).show()
   pass

def sql():
   pass
if __name__ == '__main__':
   spark = SparkSession.builder.master("local[*]").appName("Python Spark SQL").getOrCreate()
   sc=spark.sparkContext
   df=spark.read.format("csv").option("header","true").option("sep",",").option("inferSchema",True).load("file:///wenjian/lxh.data")
   dsl()
   # sql()

   spark.stop()
   sc.stop()