# Datasets are types dataframes and distributed collection of jvm objects we can work with datasets similar to
# objects in any programming language as it maintains type and can call the methods as it is.

from pyspark.sql.session import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

spark = SparkSession.builder.appName("datasets").master("local").getOrCreate()
spark.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")

numbersDF = spark.read.format("csv").option("inferSchema", "true").option("header", "true").load("C:/Users/91909"
                                                                                                 "/Desktop/project"
                                                                                                 "/pyspark"
                                                                                                 "-essentials"
                                                                                                 "/resources/data"
                                                                                                 "/numbers.csv")
numbersDF.printSchema()

'''
from the above if you want to filter the numbers of your choice for boolean condition , you need to filter them
using the spark api like using column like below
numbersDF.filter(col("number") > 10)

but , what datasets provides us is to work like a objects
numbersDf.filter(number > 10)

'''
