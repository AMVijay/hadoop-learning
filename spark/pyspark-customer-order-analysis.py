"""
-author - Vijay/am.vijay@gmail.com/vijayaraaghavan manoharan
"""

from pyspark.sql import SparkSession

# Create SparkSession
sparkSession = SparkSession.builder.appName("customer-analysis").master("local").getOrCreate()


# Implementation 1 - Using RDD
# Create RDD from CSV file
customerOrderDetailsRDD = sparkSession.sparkContext.textFile("dataset/customer-orders.csv")\
                        .map(lambda line: line.split(","))

# Create RDD as key value pair of customerId , amount
totalAmountByCustomerRDD = customerOrderDetailsRDD.map(lambda customerData : (customerData[0], customerData[2]))\
                            .reduceByKey(lambda amount1, amount2: (float(amount1)+float(amount2)))\
                            .sortByKey()

print("Implementation 1 - Results")
for data in totalAmountByCustomerRDD.collect():
    print("customer ID :: " + data[0] + " Total Amount Spent :: ${:.2f}".format(data[1]))


# Implementation 2 - Using SparkSQL Dataframe
customerDetailsDataFrame = sparkSession.read.csv("dataset/customer-orders.csv")

# Create Temp View
customerDetailsDataFrame.createOrReplaceTempView("customer")

customerAmountSpentSql = sparkSession.sql("select c._c0 as customerId, sum(c._c2) as totalAmountSpent from customer c group by c._c0 order by c._c0")

results = customerAmountSpentSql.collect()

print("Implementation 2 - Results")
for data in results:
    print("customer ID :: " + data[0] + " Total Amount Spent :: ${:.2f}".format(data[1]))
