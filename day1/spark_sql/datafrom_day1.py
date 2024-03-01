from pyspark.sql import SparkSession
from pyspark.sql.types import *
#创建datafrom
#shcema设置标头和数据结构类型
#createdataframe只接受结构接受结构化数据
if __name__ == '__main__':
   spark = SparkSession.builder.master("local[*]").appName("Python Spark SQL").getOrCreate()
   sc=spark.sparkContext
   rdd=sc.parallelize([('zhangshang',1),('wang',2),('lisi',5)])
   # schema=StructType().add("name",StringType(),True).add("age",IntegerType(),True)#自定义数据类型
   # w=spark.createDataFrame(data=rdd,schema=schema)
   w=spark.createDataFrame(data=rdd,schema=["name","age"])#自动识别数据类型
   w.printSchema()
   w.show()
   spark.stop()
   sc.stop()
