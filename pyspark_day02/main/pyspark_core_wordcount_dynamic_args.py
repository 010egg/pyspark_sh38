import os
from pyspark import SparkContext, SparkConf
import re
import sys
import time
# 动态传参
# 程序入口
if __name__ == "__main__":
    # 先配置程序运行需要用到的工具地址：需要用到JDK、Hadoop、Python环境变量
    # 配置JDK的路径，就是前面解压的那个路径
    os.environ['HADOOP_USER_NAME'] = 'root'
    os.environ['JAVA_HOME'] = 'D:/work/jdk1.8.0_241'
    # 配置Hadoop的路径，就是前面解压的那个路径
    os.environ['HADOOP_HOME'] = 'D:/hadoop-3.3.0'
    # 配置base环境Python解析器的路径
    os.environ['PYSPARK_PYTHON'] = 'D:/software/anaconda/python.exe'
    # 配置base环境Python解析器的路径
    os.environ['PYSPARK_DRIVER_PYTHON'] = 'D:/software/anaconda/python.exe'
# todo:1-构建Spark程序的驱动对象：SparkCore-SparkContext、SparkSQL-SparkSession
# 构建一个SparkConf对象：用于管理当前程序所有配置：程序名称【setAppName】，运行模式【setMaster】，配置修改【set】
conf = SparkConf().setAppName("test1").setMaster("local[2]")
# 构建一个SparkContext对象
sc = SparkContext(conf= conf)
print(sc)
# todo:2-实现对于数据的读取、处理、保存
# step1：读取数据：将读取到的数据都放入RDD对象
# spark和mr一样是按行读取数据
input_rdd = sc.textFile(sys.argv[1])
print(input_rdd.first())
print(input_rdd.count())
# step2：处理数据：调用RDD的算子对数据进行处理转换
# 1.过滤数据
r_rdd = (input_rdd
    .filter(lambda line: len(line.strip()) > 0)
# 2扁平化处理
    .flatMap(lambda line:line.strip()) # 切割的东西在一个集合里
# 3二元组构建
    .map(lambda word: (word, 1))
    .reduceByKey(lambda tmp,item: tmp + item))
r_rdd.foreach(lambda item: print(item))
# r_rdd.saveAsTextFile(sys.argv[2])


time.sleep(10000)
# step3：保存结果：根据需求将统计分析的结果打印输出或者保存
# todo:3-关闭驱动对象
sc.stop()
