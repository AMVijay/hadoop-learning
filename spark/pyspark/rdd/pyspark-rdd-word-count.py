# Author - Vijay/am.vijay@gmail.com/Vijayaraaghavan Manoharan

from pyspark.sql import SparkSession

# Create Spark Session 
sparkSession = SparkSession.builder.appName("word-count").master("local").getOrCreate()

# Create RDD by Reading Input File
bookContent = sparkSession.sparkContext.textFile("dataset/book.txt")

def parseContent(content):
    # print("print content : " + content)
    return content.split(" ")

words = bookContent.flatMap(lambda line: line.lower().split(" "))
wordCount = words.map(lambda word : (word, 1))\
    .reduceByKey(lambda countForFirstElementbyKey, countForSecondElementByKey : countForFirstElementbyKey+countForSecondElementByKey)\
        .sortBy(lambda word: word[1])

for word in wordCount.collect():
    print(word)