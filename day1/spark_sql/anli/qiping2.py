from pyspark.sql import SparkSession
from pyspark.sql.types import *
import pyspark.sql.functions as fs

"""
在使用 withColumn 方法时，第二个参数代表要添加的新列的名称。在上面的代码示例中，我们将新列命名为 "word"。这意味着我们将对原始 DataFrame 添加一个名为 "word" 的新列，该列包含了对 "name" 列进行分割后得到的单词列表。
具体来说，withColumn 方法的调用形式为：withColumn(colName, col)
其中，colName 是要添加的新列的名称，而 col 则是该列的内容。在这里，我们使用 explode(split(viv["name"], " ")) 作为新列的内容，它表示对 "name" 列进行空格分割后的结果，并使用 explode 函数展开成多行。这样就能够创建一个新列 "word"，其中包含了分割后的单词列表。
希望这样解释清楚了第二个参数的含义。如果还有其他问题，欢迎随时向我提问。
"""
if __name__ == '__main__':
   spark = SparkSession.builder.master("local[*]").appName("Python Spark SQL").getOrCreate()
   sc=spark.sparkContext
   df=spark.read.text("file:///wenjian/wenjian/xing.txt").withColumnRenamed("value","name")
   # dsl
   df.withColumn("xingname",fs.explode(fs.split("name"," "))).groupBy("xingname").count().withColumnRenamed("count","con").show()
   df.createTempView("viv")
   # sql
   result=spark.sql("SELECT word, count(*) AS count FROM (SELECT explode(split(name, ' ')) AS word  FROM viv) GROUP BY word")
   result.show()
   # exploda(split(name,' '))意思是读取到的数据是一列的挤在一起 通过这个爆炸函数可以对其进行拆解

   # sql 和 dsl 结合
   we=spark.sql("select explode(split(name, ' ')) AS we from viv")
   we.groupBy("we").count().show()