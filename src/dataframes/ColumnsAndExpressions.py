from pyspark.sql import SparkSession
from pyspark.sql.functions import col, Column, expr

spark = SparkSession.builder.appName("ColumnsAndExpressions").master("local").getOrCreate()

CarsDF = spark.read.format("json").option("inferSchema", "true").load(
    "C:/Users/91909/Desktop/project/pyspark-essentials/resources/data/cars.json")

# columns

# columns are special objects that will allow you to get the new dataframe by processing the exisitng dataframe

column2 = CarsDF.Name
column3 = CarsDF["Name"]
column1 = col("Name")

# selecting

# CarsDF.select(column1).show()
# CarsDF.select(column2).show()
# CarsDF.select(column3).show()

# if column name has '.' then use the below approch

# df.select(df["`name.fname`"]).show()

# reference : https://sparkbyexamples.com/pyspark/pyspark-column-functions/?expand_article=1

# below is the combined code to access columns in pyspark

CarsDF.select(CarsDF.Name,
              CarsDF["Miles_per_Gallon"],
              col("Cylinders"),
              "Displacement",
              expr("Acceleration"))

# select is a narrow transformation

# Expressions

simpleExpression = col("Acceleration")
accelaration2 = col("Acceleration") / 2

CarsDF.select(accelaration2.alias("accelaration"))

CarsDF.selectExpr("Acceleration /2 as acc2")

# DF Processing

# adding a column

carswithDP = CarsDF.withColumn("weight in lbs", col("Displacement") / 2)

# renaming a column

carsRenamed = CarsDF.withColumnRenamed("Miles_per_Gallon", "Miles per Gallon")
carsRenamed

# use escape characters when selecting columns that has spaces and other characters

carsRenamed.select("`Miles per Gallon`")

# removing columns

CarsDF.drop("Miles_per_Gallon")

# filtering

# filtering can be done by using column strings or with the expression strings.

print("Testing the filtering conditions from here")

powerfulUSACars = CarsDF.filter(col("Origin") == "USA")
powerfulEUCARS = CarsDF.filter(col("Origin") != "USA")
powerfulcars2 = CarsDF.filter("Origin != 'USA' ")
mulfilter = CarsDF.filter(col("Origin") == "USA").filter(col("Weight_in_lbs") > 3500)
mulfilter2 = CarsDF.filter((col("Origin") == "USA") & (col("Weight_in_lbs") > 3500))
# mulfilter3 = CarsDF.filter("Origin == 'USA' & Weight_in_lbs > 3500")


# more filter examples from below reference
# https://sparkbyexamples.com/pyspark/pyspark-where-filter/?expand_article=1

# unioning

moreCarsDF = spark.read.format("json"). \
    options(inferSchema='true', path='C:/Users/91909/Desktop/project/pyspark-essentials/resources/data/more_cars.json') \
    .load()

unionDF = CarsDF.union(moreCarsDF)
# to union both the cars should have the same schema

disitinctDF = CarsDF.distinct()

'''
1. Read the movies DF and select 2 columns of your choice 
2. Create another column summing up the total profit of the movies = US_Gross + Worldwide_Gross + DVD sales 
3. Select all COMEDY movies with IMDB rating above 6
Use as many versions as possible

'''

moviesDF = spark.read.format("json").options(
    inferSchema='true', path='C:/Users/91909/Desktop/project/pyspark-essentials/resources/data/movies.json'
).load()

moviesDF.select(moviesDF.Title, moviesDF.US_Gross)
moviesDF.select(col("Title"), col("US_Gross"))
moviesDF.select("Title", "US_Gross")
moviesDF.selectExpr("Title", "US_Gross")


total_profit1 = (col("US_Gross") + col("Worldwide_Gross") + col("US_DVD_Sales")).alias("total_gross")

moviesDF.select(total_profit1).show()

moviesDF.selectExpr("US_Gross + Worldwide_Gross + US_DVD_Sales").show()

moviesDF.filter((col("Major_Genre") == 'Comedy') & (col("IMDB_Rating") > 6)).show()

moviesDF.filter(col("Major_Genre") == 'Comedy').filter(col("IMDB_Rating") > 6).show()

moviesDF.filter("Major_Genre == 'Comedy' ").filter("IMDB_Rating > 6 ").show()