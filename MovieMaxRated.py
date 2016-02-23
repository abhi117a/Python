from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("Rating")
sc = SparkContext(conf=conf)

input = sc.textFile("D:/IMPORTANT/UTD_4thSem/SparkCourse/ml-100k/u.data")
mappedinput = input.map(lambda x: (int(x.split()[1]),1))
reduceVal = mappedinput.reduceByKey(lambda x,y : x+y)
flipped = reduceVal.map(lambda (x,y) : (y,x))
sortedMovies = flipped.sortByKey()

results = sortedMovies.collect()

for result in results:
    print result