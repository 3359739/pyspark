from pyspark.sql import SparkSession
from pyspark.sql.types import *
if __name__ == '__main__':
   spark = SparkSession.builder.master("local[*]").appName("Python Spark SQL").getOrCreate()
   sc=spark.sparkContext
   schem=StructType().add("ZHUZHANG",StringType()).add("NAME",StringType()).add('XHAO',LongType())
   w=spark.read.format("csv").schema(schem).option("sep"," ").load("file:///wenjian/wenjian/lx")
   w.createTempView("use")#创建一张表
   w=spark.sql("select * from use ")#表已经在spark内直接进行spark.sql编写sql语句
   w.printSchema()#打印表结构
   w.show()#输出到控制台
   spark.stop()
   sc.stop()
