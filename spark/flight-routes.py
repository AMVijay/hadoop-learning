from pyspark.sql import SparkSession

sparkSession = SparkSession.builder.appName("flight-routes-analysis").master("local").getOrCreate()

dataframe = sparkSession.read.csv("dataset/routes.csv",header=True)
dataframe.show()
print("Dataframe Schema")
dataframe.printSchema()

dataframe.createOrReplaceTempView("flights") 

sqlDF = sparkSession.sql("select * from flights where ` destination apirport` = 'LAX'")
print("Print SQL dataFrame")
sqlDF.show()

