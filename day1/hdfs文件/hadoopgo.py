#把配置加载进去
import findspark
findspark.init()
#初始化spark
from pyspark import SparkContext,SparkConf


if __name__ == '__main__':
    conf = SparkConf().setMaster("local[*]").setAppName("123")
    sc = SparkContext(conf=conf)
    text = sc.textFile("file:///wenjian/wenjian/xing.txt",5)
    print(text.getNumPartitions())
    print(text.glom().collect())
    sc.stop()