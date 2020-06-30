"""
-author - Vijay/am.vijay@gmail.com/Vijayaraaghavan Manoharan
"""

from pyspark import SparkContext, SparkConf

# Create SparkContext
sparkConfiguraiton = SparkConf().setMaster("local[*]").setAppName("popular-hero-analysis")
sparkContext = SparkContext(conf=sparkConfiguraiton)

def readHeroFromFile(line):
    heros = {}
    data = line.split()
    for item in data:
        heros[item] = len(data) - 1
    return heros 

# Read the file and create RDD like heroId,count
heroRDD = sparkContext.textFile("../../dataset/marvel-graph.txt")\
                        .flatMap(lambda line : line.split())\
                        .map(lambda hero: (hero,1))\
                        .reduceByKey(lambda item1, item2 : (item1+item2))\
                        .sortBy(lambda data:data[1])

# Print results
for result in heroRDD.collect():
    print(result)

# Create tuple of hero id and hero name
def heroDictionary():
    heroDictionary = {}
    with open("../../dataset/marvel-names.txt") as fileContent:
        for line in fileContent:
            fields =  line.split('\"')
            heroDictionary[int(fields[0])] = fields[1]
    return heroDictionary

# broadcast variable
heroDictionaryRDD = sparkContext.broadcast(heroDictionary())

herosByNameRDD = heroRDD.map(lambda data:(heroDictionaryRDD.value[int(data[0])], data[1]))

popularheroRDD = herosByNameRDD.max(key=lambda data:data[1])


# Print the result
print(popularheroRDD)
