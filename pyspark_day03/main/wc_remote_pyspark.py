import os
from pyspark import SparkContext, SparkConf
import re
import sys

# 程序入口
if __name__ == "__main__":
    # 修改所有集群环境变量
    os.environ['JAVA_HOME'] = '/export/server/jdk'
    os.environ['HADOOP_HOME'] = '/export/server/hadoop'
    os.environ['PYSPARK_PYTHON'] = '/export/server/anaconda3/bin/python3'
    os.environ['PYSPARK_DRIVER_PYTHON'] = '/export/server/anaconda3/bin/python3'
    os.environ['HADOOP_CONF_DIR'] = '/export/server/hadoop/etc/hadoop'
    os.environ['YARN_CONF_DIR'] = '/export/server/hadoop/etc/hadoop'
# todo:1-构建Spark程序的驱动对象：SparkCore-SparkContext、SparkSQL-SparkSession
# 构建一个SparkConf对象：用于管理当前程序所有配置：程序名称【setAppName】，运行模式【setMaster】，配置修改【set】
conf = SparkConf().setAppName("RemoteTest").setMaster("yarn")
# 构建一个SparkContext对象
sc = SparkContext(conf=conf)
print(sc)
input_rdd = sc.textFile('hdfs://node1:8020/spark/wordcount/input')
print(input_rdd.first())
print(input_rdd.count())
# step2：处理数据：调用RDD的算子对数据进行处理转换
# 1.过滤数据
r_rdd = (input_rdd
         .filter(lambda line: len(line.strip()) > 0)
         # 2扁平化处理
         .flatMap(lambda line: re.split("\\s+", line.strip()))  # 切割的东西在一个集合里
         # 3二元组构建
         .map(lambda word: (word, 1))
         .reduceByKey(lambda tmp, item: tmp + item))
r_rdd.foreach(lambda item: print(item))
r_rdd.saveAsTextFile('hdfs://node1:8020/spark/wordcount/output6')
