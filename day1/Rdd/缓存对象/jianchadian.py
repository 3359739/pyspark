import os
import time

os.environ['SPARK_HOME'] = '/anzhuan/spark'
import findspark
findspark.init()
from pyspark import SparkContext, SparkConf,StorageLevel

#持久化和缓存和共享变量
def chijhuaandgoxiangandhuanc():

    # 持久化保存

    goxiang = sc.broadcast("zhang")  # 共享变量
    sc.setCheckpointDir("/lxh/spark")  # 指定持久存储位置
    rdd_dataset = sc.parallelize(["张三 李四 王五", "李四 王五 赵六 李四", "张三 田七 赵六 赵六"])
    # 3) 处理数据
    rdd_flatMap = rdd_dataset.flatMap(lambda line: line.split(" "))
    # 如果给这个重复使用RDD 添加checkpoint
    rdd_flatMap.checkpoint()  # 不需要调用，自己触发，本身自己内部会调用collect操作
    rdd_flatMap.persist(storageLevel=StorageLevel.MEMORY_AND_DISK_2).count()  # 开启缓存
    print("=====================")
    # 3.2:统计一共有多少个元素
    print(rdd_flatMap.count())
    print("=====================")
    # 3.3:统计一共有多少个不同的名宁
    print(rdd_flatMap.distinct().count())#

def leijiaqi():
    e=sc.accumulator(0)#设置累加器初始值 | 因为在我们普通变量进行累加时是对分区内的备份进行累打印是对外面的进程进行输出
    leijia=sc.parallelize([1, 2, 3, 4, 5, 6, 7, 8, 10])
    r=leijia.foreach(lambda a:e.add(a))
    print(leijia.map(lambda a: a).collect())
    print(e.value)
if __name__ == '__main__':
    conf = SparkConf().setMaster("local[*]").setAppName("name")
    sc = SparkContext(conf=conf)
    leijiaqi()
    # chijhuaandgoxiangandhuanc()
    time.sleep(10000)
    sc.stop()