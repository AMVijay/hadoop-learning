"""
-author - Vijay/am.vijay/vijayaraaghavan manoharan
"""

from pyspark import SparkContext, SparkConf

# Initialize the Spark Context
configuration = SparkConf().setMaster("local").setAppName("popular-movie-analysis")
sparkContext = SparkContext(conf=configuration)

# Read the text file data and create RDD
moviedataRDD = sparkContext.textFile("../../ml-100k/u.data")

# Split each line by tab delimited, get movie id and accumlate the count by movie id and sort by count. 
movieWatchedCountRDD = moviedataRDD.map(lambda line:line.split("\t")).map(lambda data: (data[1], 1))\
                            .reduceByKey(lambda count1, count2: count1+count2)\
                            .sortBy(lambda data:data[1])
# Print the results
for result in movieWatchedCountRDD.collect():
    print(result)


# Create tuple of movie-id and movie-name from u.item file
def fetchMovieDictionary():
    movieDictionary = {}
    with open("../../ml-100k/u.item") as fileContent:
        for line in fileContent:
            fields = line.split("|")
            movieDictionary[int(fields[0])] = fields[1]
    return movieDictionary

# Broadcast MovieDictionary to all nodes
movieDictionary = sparkContext.broadcast(fetchMovieDictionary())
# Replace Movie Id with Movie Name
movieWatchedCountByNameRDD = movieWatchedCountRDD.map(lambda data: (movieDictionary.value[int(data[0])], data[1])) 

# Print the results
for result in movieWatchedCountByNameRDD.collect():
    print(result)
