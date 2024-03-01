from pyspark.sql import SparkSession
from pyspark.sql.types import *
if __name__ == '__main__':
   spark = SparkSession.builder.master("local[*]").appName("Python Spark SQL").getOrCreate()
   sc=spark.sparkContext
   rdd=sc.parallelize([('zhangshang',1),('wang',2),('lisi',5)])
   schema=StructType().add("name",StringType(),True).add("age",IntegerType(),True)#自定义数据类型
   # w=rdd.toDF(schema=["name","www"])

   w=rdd.toDF(schema=schema)
   w.printSchema()
   w.show()
   spark.stop()
   sc.stop()
