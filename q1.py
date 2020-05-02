import findspark
findspark.init()
import sys
from operator import add
from pyspark import SparkContext,SparkConf
from pyspark.sql import SparkSession


def mutual_list(val):
    val = val.split('\t')
    current = val[0]
    friends = []
    friends = val[1].split(',')
    fp = []
    for friend in friends:
        if ((current) > (friend)):
            fp.append((tuple([friend, current]), friends))
        else:
            fp.append((tuple([current, friend]), friends))
    return fp


if __name__ == "__main__":
    config = SparkConf().setAppName("q1").setMaster("local[2]")

    sc = SparkContext.getOrCreate()

    fl = sc.textFile("C:/Users/psait/Desktop/bda/soc-LiveJournal1Adj.txt")
    rl = fl.flatMap(mutual_list).reduceByKey(lambda list1, list2: list((set(list1).intersection(list2)))).map(lambda x: x[0][0] + "," + x[0][1] + "\t" + str(len(x[1])) + "\n")
    rl.collect()
    rl.coalesce(1).saveAsTextFile("C:/Users/psait/Desktop/bda/q1.txt")