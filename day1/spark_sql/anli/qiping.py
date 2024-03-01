from pyspark.sql import SparkSession
from pyspark.sql.types import *
import pyspark.sql.functions as fs
#这里是通过sparkcontext进行读取获取到的是一个rdd 通过rdd.toDF
if __name__ == '__main__':
   spark = SparkSession.builder.master("local[*]").appName("Python Spark SQL").getOrCreate()
   sc=spark.sparkContext
   chi=sc.textFile("file:///wenjian/wenjian/xing.txt")
   chi1=chi.flatMap(lambda x: x.split(" ")).map(lambda x: [x])
   #dls处理
   df=chi1.toDF(["name"])
   # print(type(df))
   df.groupBy("name").count().withColumnRenamed("count",'cnt').select("name").show()
   #withColumnRenamed 修改标头名字
   #select 表示只显示某个字段
   print("---------------------------------")
   #sql处理
   df.createTempView("usename")
   spark.sql("select name,count(*) as c2 from usename group by name").show()

