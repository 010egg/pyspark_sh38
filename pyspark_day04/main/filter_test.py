import os
from pyspark import SparkContext, SparkConf, TaskContext
import re
# 程序入口
if __name__ == "__main__":
    # 先配置程序运行需要用到的工具地址：需要用到JDK、Hadoop、Python环境变量
    # 配置JDK的路径，就是前面解压的那个路径
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
sc = SparkContext(conf=conf)
print(sc)
input_rdd = sc.textFile('D:/work/pyspark_sh38/pyspark_day04/datas/filter.txt')
r_rdd = input_rdd.filter(lambda line: re.split("\\s+",line)[2] != '-1' and len(re.split("\\s+", line.strip()))==4)
print(r_rdd.collect())
r_rdd.foreach(lambda x: print(TaskContext().partitionId()))
sc.stop()
