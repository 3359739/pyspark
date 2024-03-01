import findspark
findspark.init()
from pyspark import *
conf=SparkConf().setMaster("local[*]").setAppName("123W")
sc=SparkContext(conf=conf)
lines = sc.textFile("flie///D:\daima-lxh\spark\day1\\xing.txt")
lines=lines.flatMap(lambda line: line.split(" ")).map(lambda a:(a,1)).reduceByKey(lambda a,b: a+b)
print(lines.collect())


