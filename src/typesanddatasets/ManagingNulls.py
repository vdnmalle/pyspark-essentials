"""
while defining the custom schema , please keep in mind that nullable value is true by default . if you specify the nullable to
false then spark will throw an exception if there are any null values in the data that is being created
"""
from pyspark.sql.session import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

spark = SparkSession.builder.appName("nullvalues").master("local").getOrCreate()
spark.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")

moviesDF = spark.read.format("json").option("inferSchema", "true").load("C:/Users/91909/Desktop/project/pyspark"
                                                                        "-essentials/resources/data/movies.json")

# select the first non-null value

moviesDF.select(col("Title"), col("Rotten_Tomatoes_Rating"), col("IMDB_Rating"),
                coalesce(col("Rotten_Tomatoes_Rating"), col("IMDB_Rating"))).show()

# checking for nulls

moviesDF.select(col("IMDB_Rating").isNull()).show()

# checking for not nulls

moviesDF.select(col("IMDB_Rating").isNotNull()).show()

moviesDF.select("*").where(col("IMDB_Rating").isNull()).show()

# nulls ordering

moviesDF.select(col("IMDB_Rating").desc_nulls_last())

moviesDF.select(col("IMDB_Rating").desc_nulls_first())

# dropping or replacing nulls

moviesDF.select("*").na.drop()

moviesDF.select("*").na.fill(0, ["Rotten_Tomatoes_Rating", "IMDB_Rating"]).show()

# reference
# https://sparkbyexamples.com/pyspark/pyspark-isnull/?expand_article=1

moviesDF.select("*").na.fill({
    "Rotten_Tomatoes_Rating": 0,
    "IMDB_Rating": 10
}).show()

# complex operations

# the below is not available in spark DataFrame API . only available in spark sql

moviesDF.selectExpr(
    "Title",
    "Rotten_Tomatoes_Rating",
    "IMDB_Rating",
    "ifnull(Rotten_Tomatoes_Rating,IMDB_Rating)",  # same as coalesce
    "nvl(Rotten_Tomatoes_Rating,IMDB_Rating)",  # same
    "nullif(Rotten_Tomatoes_Rating,IMDB_Rating)",  # returns null if two values are equal and if not same then it
    # returns first
    "nvl2(Rotten_Tomatoes_Rating,IMDB_Rating,0)"  # if the first is != null return second else third

).show()

# additional reference

# https://sparkbyexamples.com/pyspark/pyspark-count-of-non-null-nan-values-in-dataframe/
# https://sparkbyexamples.com/pyspark/pyspark-filter-rows-with-null-values/
