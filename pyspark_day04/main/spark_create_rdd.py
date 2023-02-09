import os
from pyspark import SparkContext, SparkConf
import re
import sys
import time

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
datas =['夜曲/发如雪/东风破/七里香', '十年/爱情转移/你的背包', '日不落/舞娘/倒带', '鼓楼/成都/吉姆餐厅/无法长大', '月亮之上/荷塘月色']
input_rdd = sc.parallelize(datas, numSlices=2)
print(input_rdd.collect())
r_rdd = input_rdd.map(lambda line: re.split("/", line.strip()))  # 切割的东西在一个集合里


# r_rdd.foreach(lambda item: print(item))
# r_rdd.saveAsTextFile('hdfs://node1:8020/spark/wordcount/output6')
#不显示？
print(r_rdd.collect())
# time.sleep(1234)
sc.stop()