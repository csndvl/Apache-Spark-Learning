#import pyspark
from pyspark.sql import SparkSession
#import pandas as pd


#Creates a SparkSession
spark = SparkSession.builder.appName("Session Test 1").getOrCreate()

print(spark)

#Load Data
df = spark.read.csv("Salaries.csv")

#Prints your dataframe
df.show()

#Change Column Name
df = spark.read.option('header', 'true').csv("Salaries.csv") # Turn the first column names as headers
df.show()

#Shows the first 3
print (df.head(3))

#Shows info about your dataframe
df.printSchema()