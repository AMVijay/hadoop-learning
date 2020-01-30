from pyspark import SparkConf, SparkContext

conf = SparkConf().setAppName("Test Application").setMaster("spark://127.0.1.1:7077")
spark_context = SparkContext(conf=conf)

lines = spark_context.textFile("hdfs://localhost:9000/admin/input/routes.csv").map(lambda line: line.split(",")).filter(lambda column: column[4] == 'LAX').saveAsTextFile("hdfs://localhost:9000/admin/output/lax-routes")

# for line in lines:
#     print(line)

print("Ended the Execution")
