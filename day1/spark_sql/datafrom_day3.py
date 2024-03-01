from pyspark.sql import SparkSession
from pyspark.sql.types import *
if __name__ == '__main__':
   spark = SparkSession.builder.master("local[*]").appName("Python Spark SQL").getOrCreate()
   sc=spark.sparkContext
   w=spark.read.format("text").load("file:///wenjian/weixing.txt")
   w.printSchema()
   w.show()
   spark.stop()
   sc.stop()
