from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.session import *
from pyspark.sql.types import IntegerType


def dropna():
    df.dropna().show()  # 对空值进行处理 how=any  表示一个null清除一整行 how =all 表示全部列为null则清除
    df.dropna(thresh=2).show()  # thresh=？ 表示有多少个空值将进行清除
    df.dropna(thresh=1).show()  # thresh=？ 表示有多少个空值将进行清除
    df.dropna(thresh=2, subset=['name', 'aihao']).show()  # subset表示针对某一列为null进行清除
    # 上面这段表示的意思是name和aihao都不为name才不进行删除
    df.dropna(thresh=1, subset=['name']).show()
    # 这里表是 name 有满足一个为效值不进行删除


def fillna():
    df.fillna("name").show()  # fillna(填充的值)
    df.fillna("name", subset=['aihao']).show()  # 指定填充的列
    df.fillna({'name': "lxh", "age": 123456, "aihao": "sb"}).show()


if __name__ == '__main__':
    spark = SparkSession.builder.config("spark.sql.shuffle.partitions",4).master("local[*]").appName("lxh66").getOrCreate()
    sc =spark.sparkContext
    shach=StructType().add("name",StringType()).add("age",IntegerType()).add("aihao",StringType())
    df=spark.read.format("csv").option("sep"," ").schema(schema=shach).load("file:///wenjian/wenjian/qcho1.txt")
    # dropna()
    # fillna()

