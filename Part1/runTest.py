from os import environ
from pyspark import SparkContext, SparkConf


logFile = environ['SPARK_HOME'] + "/README.md"
sc = SparkContext("local", "Simple App")
logData = sc.textFile(logFile).cache()

numAs = logData.filter(lambda s: 'a' in s).count()
numBs = logData.filter(lambda s: 'b' in s).count()

print("Lines with a: ", numAs, "Lines with b: ", numBs)
