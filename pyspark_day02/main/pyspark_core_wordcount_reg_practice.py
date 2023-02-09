# -*- codeing =utf-8 -*-
# @Time : 2023/2/7 9:10
# @Author : 颢强
# @File : pyspark_core_wordcount_reg_practice.py.py
# @Software : PyCharm
import re
import os
from pyspark import SparkContext,SparkConf
import time

if __name__ =="__main__":
    # java环境
    os.environ['JAVA_HOME'] = 'D:/work/jdk1.8.0_241'
    # hadoop环境
    os.environ['HADOOP_HOME'] = 'D:/hadoop-3.3.0'
    # py解释器
    os.environ['PYSPARK_PYTHON'] = 'D:/software/anaconda/python.exe'
    # 驱动解释器
    os.environ['PYSPARK_DRIVER_PYTHON'] = 'D:/software/anaconda/python.exe'
    os.environ['HADOOP_USER_NAME'] = 'root'

conf = SparkConf().setAppName('reg').setMaster('local[2]')
sc = SparkContext(conf=conf)
print(sc)
inputRDD = sc.textFile("hdfs://node1:8020//spark/wordcount/input")
resRDD = (inputRDD
          .filter(lambda line: len(line.strip()) > 0)
          .flatMap(lambda line: re.split('\\s+', line.strip()))
          .map(lambda key: (key, 1))
          .reduceByKey(lambda tmp, res: tmp+res)
            )

resRDD.foreach(lambda kv: print(kv))
time.sleep(10000)
# resRDD.saveAsTextFile("hdfs://node1:8020//spark/wordcount/output_reg_test2")