# Author - Vijay(am.vijay@gmail.com)
from pyspark.sql import SparkSession

# Create Spark Session
sparkSession = SparkSession.builder.appName("Temparature Analysis").master("local").getOrCreate()

# Create Data Frame for entire data
dataframe = sparkSession.read.csv("dataset/1800.csv")
# Print Schema to understand the column name and schema with datatype.
dataframe.printSchema()

# Creat a Temparary View for SQL Operations
dataframe.createOrReplaceTempView("temparature")

# Find minimum temparature using SQL where condition, min operation, group by.
minSqlDataframe = sparkSession.sql("select t._c0, min(t._c3*0.1*(9.0/5.0) + 32.0) as MIN_TEMPARATURE from temparature t where t._c2='TMIN' group by t._c0")
minSqlDataframe.show()
results = minSqlDataframe.collect()

for result in results:
    print(result[0] + "\t{:.2f}F".format(result[1]))

# Find maximum temparature using SQL where condition, min operation, group by.
maxSqlDataframe = sparkSession.sql("select t._c0, max(t._c3*0.1*(9.0/5.0) + 32.0) as MAX_TEMPARATURE from temparature t where t._c2='TMAX' group by t._c0")
maxSqlDataframe.show()
results = maxSqlDataframe.collect()

for result in results:
    print(result[0] + "\t{:.2f}F".format(result[1]))

## Convert the column _c3 from celcius to Farenheit and double value
dataframe = dataframe.withColumn("_c3",dataframe["_c3"] * 0.1*(9/5)+32)
## Print the schema now
dataframe.printSchema()

## Perform the max temparature find using filter, groupby, max methods available in dataframe.
dataframe.filter(dataframe["_c2"] == "TMAX").groupBy("_c0").max("_c3").show()