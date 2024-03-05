from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
if __name__ == '__main__':
  #方式1和2不可以进行sql查询
   # 注意：一个程序创建多个udf时不能使用同一个函数
   # 创建udf方式一
   @udf
   def add(a):
        return a + 250555

   def add2(a):
    return a + 250555
   def add3(a):
    return a + 123
   spark = SparkSession.builder.master("local[*]").appName("Python Spark SQL").getOrCreate()
   sc=spark.sparkContext
   schem=StructType().add("name",StringType()).add("age",IntegerType())
   w=spark.read.format("csv").schema(schem).option("sep"," ").load("file:///wenjian/weixing.txt")
   w.select(w['name'],w['age'],add(w['age'])).show()
   #创建udf方式2
   add2=udf(add2,IntegerType())
   w.select(w['name'],w['age'],add2(w['age'])).show()
   # 方式3
   add3 = spark.udf.register("add3", f=add3, returnType=IntegerType())
   w.select(w['name'], w['age'], add3(w['age'])).show()
   print("***************sql*****************")
   w.createTempView("vi")
   spark.sql("select name,age,add3(age) ahe from vi").show()