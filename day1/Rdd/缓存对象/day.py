import os
import time

os.environ['SPARK_HOME'] = '/anzhuan/spark'
import findspark
findspark.init()
from pyspark import SparkContext, SparkConf,StorageLevel
if __name__ == '__main__':
    conf = SparkConf().setMaster("local[*]").setAppName("name")
    sc = SparkContext(conf=conf)
    huan=sc.parallelize(["zhang",'cheng','xiao','da','wang'])
    # huan.persist(storageLevel=StorageLevel.MEMORY_AND_DISK_2).count()#设置缓存
    rdd1=huan.map(lambda a:(a,1))
    rdd2=huan.reduceByKey(lambda a,b:a+b)
    print(rdd1.collect())
    time.sleep(10000)
    sc.stop()