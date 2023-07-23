from pyspark.sql.session import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *

spark = SparkSession.builder.appName("commontypes").master("local").getOrCreate()

moviesDF = spark.read.format("json").option("inferSchema", "true").load("C:/Users/91909/Desktop/project/pyspark"
                                                                        "-essentials/resources/data/movies.json")

# adding a plain value to df

moviesDF.select(col("Title"), lit(47).alias("plainvalue"))

# booleans
# working with boolean values

comedyfilter = col("Major_Genre") == 'Comedy'
goodMovies = col("IMDB_Rating") > 6
goodComedy = comedyfilter & goodMovies

moviesDF.filter(goodComedy)

# we can directly select these boolean filters as columns as well

moviesDF.select("Title", goodComedy.alias("good_comedy"))

# we can negate on the column as well


# numbers

# we can work on the numbers as we wanted like all the mathematical operations until the column names are numeric


# strings

carsDf = spark.read.format("json").option("inferSchema", "true").load("C:/Users/91909/Desktop/project/pyspark"
                                                                      "-essentials/resources/data/cars.json")
carsDf.select(initcap("Name"))

# contains

carsDf.select("Name").filter(col("Name").contains("volkswagen"))

# but , above might miss the short hand notation of the brands. so, we are going to use the regex function.

regex_string = "volkswagen|vw"

vwdf = carsDf.select("Name", regexp_extract(col("Name"), regex_string, 0).alias("regex_extract")) \
    .where(col("regex_extract") != "")

vwdf.select("Name", regexp_replace("Name", regex_string, "Malle's car").alias("regex_replace")).where(
    col("regex_replace") != "")

'''
filter the carnames by a list of car names given by an API call

'''


def getCarNames() -> []:
    return ["volkswagen", "ford", "chevrolet"]


def constructregex() -> str:
    result = ""
    for i in getCarNames():
        result = i + "|"

    return result


carsDf.select("Name", regexp_extract("Name", constructregex(), 0).alias("regex_extract")).where(col("regex_extract") != "").show()
