from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("WordCount")
sc = SparkContext(conf = conf)

input = sc.textFile("file:///sparkcourse/book.txt")
words = input.flatMap(lambda x: x.split())
#wordCounts = words.countByValue()
wordCount = words.map(lambda x: (x,1))
wordCounts = wordCount.reduceByKey(lambda x,y : x+y)
wordCountSorted = wordCounts.map(lambda (x,y): (y,x)).sortByKey()


for word, count in wordCounts.items():
    cleanWord = word.encode('ascii', 'ignore')
    if (cleanWord):
        print cleanWord, count
