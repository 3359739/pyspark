from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.session import *
from pyspark.sql.types import IntegerType

if __name__ == '__main__':
    spark = SparkSession.builder.config("spark.sql.shuffle.partitions",4).master("local[*]").appName("lxh66").getOrCreate()
    sc =spark.sparkContext
    shach=StructType().add("name",StringType()).add("age",IntegerType()).add("aihao",StringType())

    df=spark.read.format("csv").option("sep"," ").schema(schema=shach).load("file:///wenjian/wenjian/qcho.txt")

    df.write.mode("overwrite").option("header",True).option("encoding", "utf-8").csv("file:///wenjian/wenjian/1")
