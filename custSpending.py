import re
from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("custspending")
sc = SparkContext(conf = conf)

def parseLine(line):
    fields = line.split(',')
    return (int(fields[0]), float(fields[2]))




input = sc.textFile("D:/IMPORTANT/UTD_4thSem/SparkCourse/customer-orders.csv")
values = input.map(parseLine)

vals = values.reduceByKey(lambda x, y: (x+y))
val1 = vals.map(lambda (x,y): (y,x)).sortByKey()
val2 = val1.map(lambda (x,y): (y,x))
results = val2.collect()

for result in results:
    print result