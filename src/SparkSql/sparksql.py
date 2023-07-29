from pyspark.sql.session import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *

spark = SparkSession.builder.appName("sparksql").config("spark.sql.warehouse.dir", "src/main/resources/warehouse").master("local").getOrCreate()

'''
-> Abstraction over the dataframes for the engineers familiar with the sql
-> spark supports sql in two ways
programmatically in spark
also in the spark shell
-> it has the concept of database, table , view
-> allows accessing dataframes as tables i.e we can create tempview and can write spark.sql("")
-> Both dataframes and tables are identical in terms of storage and partitioning.But , Dataframes can be accessed program where
as tables by spark sql


'''

carsDF = spark.read.format("json") \
    .option("inferSchema", "true").load("C:/Users/91909/Desktop/project/pyspark"
                                        "-essentials/resources/data/cars.json")


# spark sql

carsDF.createOrReplaceTempView("cars")

spark.sql("""
select * from cars
""".strip()).show()

# by default below will create a database as "spark-warehouse" in the root folder which will contain all the
# databases and soon.
# like below we can run any number of sql statements . but , please be min that each statement will be executed one by one
spark.sql("create database malle")

spark.sql("use malle")

spark.sql("show databases").show()
