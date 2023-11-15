from pyspark.sql import SparkSession


spark = SparkSession.builder.appName("DataCleaning Practice").getOrCreate()

#Without inferSchema = True. All values are treated as strings by default
df = spark.read.option("header","true").csv("Salaries.csv")

#With inferSchama = True. Ignore by default value
df = spark.read.option("header","true").csv("Salaries.csv", inferSchema = True)

#Check data types of columns
df.printSchema()

#Better way to load data and adjust headers
df = spark.read.csv("Salaries.csv", header = True, inferSchema = True)
df.show()

#Prints all columns
print (df.columns)

#Select a specific column/s
df.select('Salary/Hr', 'Duration').show() #Without show() ,it shows the column data type

#Do a SQL Expression
df.selectExpr('Name', 'Duration * 12 as Months').show()

#Check columns data type
print (df.dtypes)

#Adding Column      Column Name             Column Values
df = df.withColumn('Duration After 2 Years',df['Duration']+2)
df.show()

#Removing Column
df = df.drop("Duration After 2 Years")
df.show()

#Rename Column
df = df.withColumnRenamed('Name', 'New Name')
df.show()

