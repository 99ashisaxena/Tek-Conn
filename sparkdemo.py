import sys

from pyspark import SparkContext, SparkConf

if __name__ == "__main__":

    # create Spark context with Spark configuration
    conf = SparkConf().setAppName("Read Text to RDD - Python")
    sc = SparkContext(conf=conf)

    # read input text file to RDD
    lines = sc.textFile("/C:/Users/ashsaxena/PycharmProjects/Conn/a")

    # collect the RDD to a list
    llist = lines.collect()

    # print the list
    for line in llist:
        print(line)


