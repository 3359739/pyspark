import findspark
import jieba
from nltk.corpus.reader import lin

findspark.init()
from pyspark import SparkContext,SparkConf


def fun(w):
    wexing = w.split()
    return (wexing[0], wexing[1], wexing[2][1:-1], wexing[3], wexing[4], wexing[5])
def day1():
    fengchi = zhuanhuan.map(lambda lin: lin[2]).flatMap(lambda line: jieba.lcut(line)).map(
        lambda line: (line, 1)).reduceByKey(lambda a, b: a + b).filter(lambda a: len(a[0]) >= 2).top(10, lambda a: a[1])
    print(fengchi)
def day2():
    global zhuanhuan
    zhuanhuan = goulu.map(lambda line: fun(line)).map(lambda lin: ((lin[1], lin[2]), 1)).reduceByKey(lambda a, b: a + b)
    paishu = zhuanhuan.top(10, lambda a: a[1])
    print(zhuanhuan.map(lambda line: line[1]).mix())
    print(zhuanhuan.map(lambda line: line[1]).max())
    print(zhuanhuan.map(lambda line: line[1]).mean())
def day3():
    ww = goulu.map(lambda line: fun(line)[0][3:5]).map(lambda line: (line, 1)).reduceByKey(lambda a, b: a + b)
    top10 = ww.top(10, lambda a: a[1])
def day4():
    wangzhang = sc.textFile("file:///wenjian/1.txt")
    # ['251.48.193.38', '-', 'user_8788', '[19/Feb/2024:16:00:00', '+0800]
    pv = wangzhang.map(lambda line: line.split()[0]).count()
    print(pv)
    uv = wangzhang.map(lambda line: line.split()[0]).distinct().count()
    print(uv)
    lujin=wangzhang.map(lambda line: line.split()[6]).map(lambda line: (line, 1)).reduceByKey(lambda a,d:a+d)
    print(lujin.top(10, lambda a: a[1]))
if __name__ == '__main__':
    conf=SparkConf().setMaster("local[*]").setAppName("lxh")
    sc=SparkContext(conf=conf)
    all_conf = sc.getConf().getAll()
    print(all_conf)
    beginning = sc.textFile("file:///wenjian/wenjian/SogouQ.sample")
    goulu = beginning.filter(lambda line: len(line.split()) == 6)
    ww=goulu.map(lambda line: fun(line))
    # day4()
    # day3()
    # day2()
    # day1()
    sc.stop()