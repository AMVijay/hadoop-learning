from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Test").master("local").getOrCreate()

df = spark.read.json("examples/src/main/resources/people.json")

# Displays the content of the DataFrame to stdout
print("Printing the Spark SQL Dataframe content")
df.show()

# Print DataFrame Schema
print("Printing the Spark SQL Dataframe schema")
df.printSchema()

# Print Name column values 
print("Printing the Spark SQL Selective column values")
df.select("name").show()

# Register the DataFrame as a SQL temporary view
df.createOrReplaceTempView("people")

# Execute the SQL query against dataframe view
sqlDF = spark.sql("SELECT * FROM people")
print("Printing the Spark SQL Dataframe Content using SQL Query created")
sqlDF.show()