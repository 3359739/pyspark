import  findspark
findspark.init()
# rdd算子: 转化算子和动作算子
# 转换算子：对数据进行转换为rdd算子返回一个新的rdd，要执行动作算子才会生效例如：collect()
# 图解：D:\daima-lxh\spark\day1\hdfs文件\img_1.png
from pyspark import SparkContext, SparkConf

conf=SparkConf().setMaster("local[*]").setAppName("lxh66")
sc=SparkContext(conf=conf)
rdd1=sc.parallelize(range(100))


def group():
    # 分组操作
    rdd2 = rdd1.groupBy(lambda a: "大60" if (a > 60) else "小60")
    rdd2.collect()
    print(rdd2.mapValues(set).collect())
def flirelxh():
    # 过滤
    rdd2=rdd1.filter(lambda a:a>88)
    print(rdd2.collect())
def fmap():
    #整合成一个列表
    rdd1=sc.parallelize([["zhang,shang,lixi"],["weinxg,scm,lxh"]])
    rdd2=rdd1.flatMap(lambda rdd1:rdd1[0].split(",")).collect()
    print(rdd2)
def cont():
    #统计countByValue：值 countByKey：键
    rdd1=sc.parallelize([1,23,4,1,2,3,2,2,2,2,2,2,2])
    const=rdd1.countByValue()
    print(const)
    print(type(const.values()))
    print(dict(const))
def addfengqu():
    print(rdd1.getNumPartitions())#查看分区数量
    # print(rdd1.glom().collect())查看数据分区格式
    print(rdd1.glom().collect())
    rdd2=rdd1.repartition(5)#增大分区
    print(rdd2.getNumPartitions())
    print(rdd2.glom().collect())
    pass
if __name__ == '__main__':
    try:
        # group()
        # flirelxh()
        # fmap()
        # cont()
        addfengqu()
        pass
    finally:
        sc.stop()
