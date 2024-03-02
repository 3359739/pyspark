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
   df.createTempView("vi")
   df1=spark.sql("select go_id from vi where feng>3 group by go_id ")
   df1.createTempView("vi1")
   spark.sql("SELECT  t.nameid FROM vi t JOIN vi1 t1 ON t.go_id = t1.go_id GROUP BY  t.nameid order by count(1) desc limit 1")
   spark.sql("select avg(feng) from (SELECT  * FROM vi  where nameid =(SELECT  t.nameid FROM vi t JOIN vi1 t1 ON t.go_id = t1.go_id GROUP BY  t.nameid order by count(1) desc limit 1))").show()
   pass
if __name__ == '__main__':
   spark = SparkSession.builder.master("local[*]").appName("Python Spark SQL").getOrCreate()
   sc=spark.sparkContext
   df=spark.read.format("csv").option("header","true").option("sep",",").option("inferSchema",True).load("file:///wenjian/lxh.data")
   # dsl()
   sql()

   spark.stop()
   sc.stop()