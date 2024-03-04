from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.session import *
from pyspark.sql.types import IntegerType

if __name__ == '__main__':
    spark = SparkSession.builder.config("spark.sql.shuffle.partitions",4).master("local[*]").appName("lxh66").getOrCreate()
    sc =spark.sparkContext
    shach=StructType().add("name",StringType()).add("age",IntegerType()).add("aihao",StringType())
    df=spark.read.format("csv").option("sep"," ").schema(schema=shach).load("file:///wenjian/wenjian/qcho.txt")
    df.dropDuplicates().show()#全部字段一样进行去重保留最后一个
    df.dropDuplicates(['name','age']).show()#只要name,age两个字段字段重复进行删除 这里可以添加多个字段
