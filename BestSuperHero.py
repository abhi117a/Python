from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("bestsuperHero")
sc = SparkContext(conf = conf)
def parseName(line):
    heros = line.split('"')
    return (int (heros[0]), heros[1].encode("utf8"))

def coOccurance(line1):
    elements = line1.split()
    return(int(elements[0]), len(elements)-1)

names = sc.textFile("D:/IMPORTANT/UTD_4thSem/SparkCourse/Superhero/Marvel-Names.txt")
namesRdd = names.map(parseName)



values = sc.textFile("D:/IMPORTANT/UTD_4thSem/SparkCourse/Superhero/Marvel-Graph.txt")

valuesRdd = values.map(coOccurance)

totalFriends = valuesRdd.reduceByKey(lambda x, y : x+y)

flipped = totalFriends.map(lambda (x, y) : (y, x))


mostpopular = flipped.max()

mostpopularName = namesRdd.lookup(mostpopular[1])[0]

print mostpopularName
print mostpopular[0]