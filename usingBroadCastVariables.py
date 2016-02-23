from pyspark import SparkContext, SparkConf

def load_movies():
    movieNames={}
    with open("D:/IMPORTANT/UTD_4thSem/SparkCourse/ml-100k/u.item") as f:
        for line in f:
            fields = line.split('|')
            movieNames[int(fields[0])]=fields[1]
    return movieNames

conf = SparkConf().setMaster("local").setAppName("brdCastVar")
sc = SparkContext(conf = conf)

nameDict = sc.broadcast(load_movies())

lines = sc.textFile("D:/IMPORTANT/UTD_4thSem/SparkCourse/ml-100k/u.data")
movies = lines.map(lambda x: (int(x.split()[1]),1))
moviesCount = movies.reduceByKey(lambda x,y : x+y)

flipped  = moviesCount.map(lambda (x,y): (y,x))
sortedMovies = flipped.sortByKey()

sortedMoviesWithNames = sortedMovies.map(lambda (count,movie): (nameDict.value[movie], count))

results = sortedMoviesWithNames.collect()

for result in results:
    print result